/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.cloud.ai.autoconfigure.graph;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.alibaba.cloud.ai.graph.CompileConfig;
import com.alibaba.cloud.ai.graph.CompiledGraph;
import com.alibaba.cloud.ai.graph.KeyStrategy;
import com.alibaba.cloud.ai.graph.StateGraph;
import com.alibaba.cloud.ai.graph.action.AsyncNodeActionWithConfig;
import com.alibaba.cloud.ai.graph.observation.GraphObservationLifecycleListener;
import com.alibaba.cloud.ai.graph.state.strategy.AppendStrategy;
import io.micrometer.context.ContextRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.alibaba.cloud.ai.graph.StateGraph.END;
import static com.alibaba.cloud.ai.graph.StateGraph.START;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for observation scope propagation in the graph observation listener.
 */
class GraphObservationScopeRegressionTest {

	private static final String ACCESSOR_LOGGER = "io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor";

	private ObservationRegistry observationRegistry;

	private ObservationThreadLocalAccessor accessor;

	private ExecutorService nodeExecutor;

	private ListAppender<ILoggingEvent> logAppender;

	private Logger accessorLogger;

	@BeforeEach
	void setUp() {
		this.observationRegistry = ObservationRegistry.create();
		this.accessor = new ObservationThreadLocalAccessor(this.observationRegistry);
		ContextRegistry.getInstance().registerThreadLocalAccessor(this.accessor);
		Hooks.enableAutomaticContextPropagation();

		this.nodeExecutor = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "graph-node-worker"));

		this.accessorLogger = (Logger) LoggerFactory.getLogger(ACCESSOR_LOGGER);
		this.logAppender = new ListAppender<>();
		this.logAppender.start();
		this.accessorLogger.addAppender(this.logAppender);
	}

	@AfterEach
	void tearDown() {
		this.accessorLogger.detachAppender(this.logAppender);
		this.nodeExecutor.shutdownNow();
		if (this.observationRegistry.getCurrentObservationScope() != null) {
			this.observationRegistry.getCurrentObservationScope().close();
		}
	}

	@Test
	void shouldNotLeakObservationScopeWhenAsyncNodeCompletesOnAnotherThread() throws Exception {
		StateGraph graph = new StateGraph(this::keyStrategies)
			.addNode("asyncNode", (AsyncNodeActionWithConfig) (state, config) -> CompletableFuture
				.supplyAsync(() -> Map.of("messages", "async-result"), this.nodeExecutor))
			.addEdge(START, "asyncNode")
			.addEdge("asyncNode", END);

		CompileConfig compileConfig = CompileConfig.builder()
			.observationRegistry(this.observationRegistry)
			.withLifecycleListener(new GraphObservationLifecycleListener(this.observationRegistry))
			.build();

		CompiledGraph compiledGraph = graph.compile(compileConfig);

		compiledGraph.stream(Map.of("input", "value")).blockLast(Duration.ofSeconds(10));

		List<ILoggingEvent> warnings = collectWarnings();

		assertThat(this.observationRegistry.getCurrentObservation())
			.as("node observation scope must not leak into the calling thread")
			.isNull();
		assertNoAccessorWarnings(warnings);
	}

	@Test
	void shouldPropagateNodeObservationToDeferredChildObservation() throws Exception {
		TestObservationHandler handler = new TestObservationHandler();
		this.observationRegistry.observationConfig().observationHandler(handler);

		AtomicReference<Observation.Context> childContext = new AtomicReference<>();
		AsyncNodeActionWithConfig streamingNode = (state, config) -> CompletableFuture.completedFuture(Map.of(
				"messages", Flux.deferContextual(contextView -> {
					Observation parent = contextView.getOrDefault(ObservationThreadLocalAccessor.KEY, null);
					Observation child = Observation
						.createNotStarted("test.model.call", Observation.Context::new, this.observationRegistry)
						.parentObservation(parent)
						.start();
					childContext.set(child.getContext());
					child.stop();
					return Flux.just("chunk");
				})));

		StateGraph graph = new StateGraph(this::keyStrategies)
			.addNode("streamingNode", streamingNode)
			.addEdge(START, "streamingNode")
			.addEdge("streamingNode", END);

		CompileConfig compileConfig = CompileConfig.builder()
			.observationRegistry(this.observationRegistry)
			.withLifecycleListener(new GraphObservationLifecycleListener(this.observationRegistry))
			.build();

		graph.compile(compileConfig).stream(Map.of("input", "value")).blockLast(Duration.ofSeconds(10));

		Observation.Context nodeContext = handler.find("spring.ai.alibaba.graph.node.streamingNode");
		assertThat(nodeContext).isNotNull();
		assertThat(childContext.get()).isNotNull();
		assertThat(childContext.get().getParentObservation()).isNotNull();
		assertThat(childContext.get().getParentObservation().getContextView())
			.as("deferred model call must be a child of the node observation")
			.isSameAs(nodeContext);
		assertThat(this.observationRegistry.getCurrentObservation()).isNull();
		assertNoAccessorWarnings(collectWarnings());
	}

	@Test
	void shouldParentNodeObservationsUnderGraphObservation() throws Exception {
		TestObservationHandler handler = new TestObservationHandler();
		this.observationRegistry.observationConfig().observationHandler(handler);

		StateGraph graph = new StateGraph(this::keyStrategies)
			.addNode("firstNode", (AsyncNodeActionWithConfig) (state, config) -> CompletableFuture
				.supplyAsync(() -> Map.of("messages", "first"), this.nodeExecutor))
			.addNode("secondNode", (AsyncNodeActionWithConfig) (state, config) -> CompletableFuture
				.supplyAsync(() -> Map.of("messages", "second"), this.nodeExecutor))
			.addEdge(START, "firstNode")
			.addEdge("firstNode", "secondNode")
			.addEdge("secondNode", END);

		CompileConfig compileConfig = CompileConfig.builder()
			.observationRegistry(this.observationRegistry)
			.withLifecycleListener(new GraphObservationLifecycleListener(this.observationRegistry))
			.build();

		graph.compile(compileConfig).stream(Map.of("input", "value")).blockLast(Duration.ofSeconds(10));

		Observation.Context graphContext = handler.find("spring.ai.alibaba.graph.graph-execution");
		Observation.Context firstNodeContext = handler.find("spring.ai.alibaba.graph.node.firstNode");
		Observation.Context secondNodeContext = handler.find("spring.ai.alibaba.graph.node.secondNode");

		assertThat(graphContext).isNotNull();
		assertThat(firstNodeContext).isNotNull();
		assertThat(secondNodeContext).isNotNull();
		assertThat(firstNodeContext.getParentObservation()).isNotNull();
		assertThat(firstNodeContext.getParentObservation().getContextView()).isSameAs(graphContext);
		assertThat(secondNodeContext.getParentObservation()).isNotNull();
		assertThat(secondNodeContext.getParentObservation().getContextView()).isSameAs(graphContext);
		assertThat(this.observationRegistry.getCurrentObservation()).isNull();
		assertNoAccessorWarnings(collectWarnings());
	}

	private List<ILoggingEvent> collectWarnings() {
		return this.logAppender.list.stream()
			.filter(event -> event.getLevel().isGreaterOrEqual(Level.WARN))
			.collect(Collectors.toList());
	}

	private void assertNoAccessorWarnings(List<ILoggingEvent> warnings) {
		assertThat(warnings).as("ObservationThreadLocalAccessor must not report scope corruption").isEmpty();
	}

	private Map<String, KeyStrategy> keyStrategies() {
		return Map.of("messages", new AppendStrategy());
	}

	private static class TestObservationHandler implements ObservationHandler<Observation.Context> {

		private final Map<String, Observation.Context> contexts = new ConcurrentHashMap<>();

		@Override
		public void onStart(Observation.Context context) {
			this.contexts.put(context.getName(), context);
		}

		Observation.Context find(String name) {
			return this.contexts.get(name);
		}

		@Override
		public boolean supportsContext(Observation.Context context) {
			return true;
		}

	}

}
