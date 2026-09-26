/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.async;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncResultsService;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.async.StoredAsyncResponse;
import org.elasticsearch.xpack.core.async.StoredAsyncTask;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.ql.async.AsyncTaskManagementService.addCompletionListener;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AsyncTaskManagementServiceTests extends ESSingleNodeTestCase {
    private ClusterService clusterService;
    private TransportService transportService;
    private AsyncResultsService<TestTask, StoredAsyncResponse<TestResponse>> results;

    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    public static class TestRequest extends UntypedActionRequest {
        private final String string;

        public TestRequest(String string) {
            this.string = string;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class TestResponse extends ActionResponse {
        private final String string;
        private final String id;

        public TestResponse(String string, String id) {
            this.string = string;
            this.id = id;
        }

        public TestResponse(StreamInput input) throws IOException {
            this.string = input.readOptionalString();
            this.id = input.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(string);
            out.writeOptionalString(id);
        }
    }

    public static class TestTask extends StoredAsyncTask<TestResponse> {
        public volatile AtomicReference<TestResponse> finalResponse = new AtomicReference<>();

        public TestTask(
            long id,
            String type,
            String action,
            String description,
            TaskId parentTaskId,
            Map<String, String> headers,
            Map<String, String> originHeaders,
            AsyncExecutionId asyncExecutionId,
            TimeValue keepAlive
        ) {
            super(id, type, action, description, parentTaskId, headers, originHeaders, asyncExecutionId, keepAlive);
        }

        @Override
        public TestResponse getCurrentResult() {
            return Objects.requireNonNullElseGet(finalResponse.get(), () -> new TestResponse(null, getExecutionId().getEncoded()));
        }
    }

    public static class TestOperation implements AsyncTaskManagementService.AsyncOperation<TestRequest, TestResponse, TestTask> {

        @Override
        public TestTask createTask(
            TestRequest request,
            long id,
            String type,
            String action,
            TaskId parentTaskId,
            Map<String, String> headers,
            Map<String, String> originHeaders,
            AsyncExecutionId asyncExecutionId
        ) {
            return new TestTask(
                id,
                type,
                action,
                request.getDescription(),
                parentTaskId,
                headers,
                originHeaders,
                asyncExecutionId,
                TimeValue.timeValueDays(5)
            );
        }

        @Override
        public void execute(TestRequest request, TestTask task, ActionListener<TestResponse> listener) {
            if (request.string.equals("die")) {
                listener.onFailure(new IllegalArgumentException("test exception"));
            } else {
                listener.onResponse(new TestResponse("response for [" + request.string + "]", task.getExecutionId().getEncoded()));
            }
        }

        @Override
        public TestResponse initialResponse(TestTask task) {
            return new TestResponse(null, task.getExecutionId().getEncoded());
        }

        @Override
        public TestResponse readResponse(StreamInput inputStream) throws IOException {
            return new TestResponse(inputStream);
        }
    }

    public String index = "test-index";

    @Before
    public void setup() {
        clusterService = getInstanceFromNode(ClusterService.class);
        transportService = getInstanceFromNode(TransportService.class);
        BigArrays bigArrays = getInstanceFromNode(BigArrays.class);
        AsyncTaskIndexService<StoredAsyncResponse<TestResponse>> store = new AsyncTaskIndexService<>(
            index,
            clusterService,
            transportService.getThreadPool().getThreadContext(),
            client(),
            "test",
            in -> new StoredAsyncResponse<>(TestResponse::new, in),
            writableRegistry(),
            bigArrays
        );
        results = new AsyncResultsService<>(
            store,
            true,
            TestTask.class,
            (task, listener, timeout, returnIntermediateResults) -> addCompletionListener(
                transportService.getThreadPool(),
                task,
                listener,
                timeout,
                returnIntermediateResults
            ),
            transportService.getTaskManager(),
            clusterService
        );
    }

    /**
     * Shutdown the executor so we don't leak threads into other test runs.
     */
    @After
    public void shutdownExec() {
        executorService.shutdown();
    }

    private AsyncTaskManagementService<TestRequest, TestResponse, TestTask> createManagementService(
        AsyncTaskManagementService.AsyncOperation<TestRequest, TestResponse, TestTask> operation
    ) {
        return createManagementService(operation, transportService.getTaskManager());
    }

    private AsyncTaskManagementService<TestRequest, TestResponse, TestTask> createManagementService(
        AsyncTaskManagementService.AsyncOperation<TestRequest, TestResponse, TestTask> operation,
        TaskManager taskManager
    ) {
        BigArrays bigArrays = getInstanceFromNode(BigArrays.class);
        return new AsyncTaskManagementService<>(
            index,
            client(),
            "test_origin",
            writableRegistry(),
            taskManager,
            "test_action",
            operation,
            TestTask.class,
            clusterService,
            transportService.getThreadPool(),
            bigArrays
        );
    }

    public void testConcurrentBackgroundTasksActivateAndRestoreContext() throws Exception {
        var threadPool = transportService.getThreadPool();
        var threadContext = threadPool.getThreadContext();
        var exporter = InMemorySpanExporter.create();
        try (
            var sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build())
                .build()
        ) {
            var taskManager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), Tracer.NOOP);
            taskManager.setOpenTelemetry(sdk);
            List<Runnable> completions = new ArrayList<>();
            List<TestTask> tasks = new ArrayList<>();
            List<Span> parents = new ArrayList<>();
            var service = createManagementService(new TestOperation() {
                @Override
                public void execute(TestRequest request, TestTask task, ActionListener<TestResponse> listener) {
                    var taskSpan = Span.fromContext(task.getTraceContext());
                    assertEquals(taskSpan.getSpanContext(), Span.current().getSpanContext());
                    tasks.add(task);
                    completions.add(threadContext.preserveContext(() -> {
                        assertEquals(taskSpan.getSpanContext(), Span.current().getSpanContext());
                        var dependency = sdk.getTracer("test").spanBuilder("dependency").startSpan();
                        dependency.end();
                        super.execute(request, task, listener);
                    }));
                }
            }, taskManager);
            try {
                for (String name : List.of("first", "second")) {
                    var parent = sdk.getTracer("test").spanBuilder(name).setNoParent().startSpan();
                    parents.add(parent);
                    try (var scope = parent.makeCurrent()) {
                        service.asyncExecute(
                            new TestRequest(name),
                            TimeValue.timeValueMinutes(1),
                            TimeValue.timeValueMinutes(10),
                            false,
                            ActionTestUtils.assertNoFailureListener(
                                response -> assertEquals("response for [" + name + "]", response.string)
                            )
                        );
                        assertEquals(parent.getSpanContext(), Span.current().getSpanContext());
                        assertNull(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER));
                    }
                }
                assertTrue(exporter.getFinishedSpanItems().isEmpty());
                for (int index = completions.size() - 1; index >= 0; index--) {
                    var completion = completions.get(index);
                    executorService.submit(() -> {
                        completion.run();
                        assertFalse(Span.current().getSpanContext().isValid());
                        assertTrue(threadContext.isDefaultContext());
                    }).get(10, TimeUnit.SECONDS);
                }
                assertTrue(taskManager.getTasks().isEmpty());
                assertEquals(4, exporter.getFinishedSpanItems().size());
                for (int index = 0; index < tasks.size(); index++) {
                    var taskContext = Span.fromContext(tasks.get(index).getTraceContext()).getSpanContext();
                    var parentContext = parents.get(index).getSpanContext();
                    var taskSpan = exporter.getFinishedSpanItems()
                        .stream()
                        .filter(span -> span.getSpanId().equals(taskContext.getSpanId()))
                        .findFirst()
                        .orElseThrow();
                    assertEquals(parentContext.getSpanId(), taskSpan.getParentSpanId());
                    assertEquals(parentContext.getTraceId(), taskSpan.getTraceId());
                    var dependency = exporter.getFinishedSpanItems()
                        .stream()
                        .filter(span -> span.getName().equals("dependency") && span.getTraceId().equals(parentContext.getTraceId()))
                        .findFirst()
                        .orElseThrow();
                    assertEquals(taskSpan.getSpanId(), dependency.getParentSpanId());
                }
            } finally {
                tasks.forEach(taskManager::unregister);
                parents.forEach(Span::end);
            }
        }
    }

    public void testReturnBeforeTimeout() throws Exception {
        AsyncTaskManagementService<TestRequest, TestResponse, TestTask> service = createManagementService(new TestOperation());
        boolean success = randomBoolean();
        boolean keepOnCompletion = randomBoolean();
        CountDownLatch latch = new CountDownLatch(1);
        TestRequest request = new TestRequest(success ? randomAlphaOfLength(10) : "die");
        service.asyncExecute(
            request,
            TimeValue.timeValueMinutes(1),
            TimeValue.timeValueMinutes(10),
            keepOnCompletion,
            ActionListener.wrap(r -> {
                assertThat(success, equalTo(true));
                assertThat(r.string, equalTo("response for [" + request.string + "]"));
                assertThat(r.id, notNullValue());
                latch.countDown();
            }, e -> {
                assertThat(success, equalTo(false));
                assertThat(e.getMessage(), equalTo("test exception"));
                latch.countDown();
            })
        );
        assertThat(latch.await(10, TimeUnit.SECONDS), equalTo(true));
    }

    public void testReturnAfterTimeout() throws Exception {
        CountDownLatch executionLatch = new CountDownLatch(1);
        AsyncTaskManagementService<TestRequest, TestResponse, TestTask> service = createManagementService(new TestOperation() {
            @Override
            public void execute(TestRequest request, TestTask task, ActionListener<TestResponse> listener) {
                executorService.submit(() -> {
                    try {
                        assertThat(executionLatch.await(10, TimeUnit.SECONDS), equalTo(true));
                    } catch (InterruptedException ex) {
                        fail("Shouldn't be here");
                    }
                    super.execute(request, task, listener);
                });
            }
        });
        boolean success = randomBoolean();
        boolean keepOnCompletion = randomBoolean();
        boolean timeoutOnFirstAttempt = randomBoolean();
        boolean waitForCompletion = randomBoolean();
        CountDownLatch latch = new CountDownLatch(1);
        TestRequest request = new TestRequest(success ? randomAlphaOfLength(10) : "die");
        AtomicReference<TestResponse> responseHolder = new AtomicReference<>();
        service.asyncExecute(
            request,
            TimeValue.timeValueMillis(1),
            TimeValue.timeValueMinutes(10),
            keepOnCompletion,
            ActionTestUtils.assertNoFailureListener(r -> {
                assertThat(r.string, nullValue());
                assertThat(r.id, notNullValue());
                assertThat(responseHolder.getAndSet(r), nullValue());
                latch.countDown();
            })
        );
        assertThat(latch.await(20, TimeUnit.SECONDS), equalTo(true));

        if (timeoutOnFirstAttempt) {
            logger.trace("Getting an in-flight response");
            // try getting results, but fail with timeout because it is not ready yet
            StoredAsyncResponse<TestResponse> response = getResponse(responseHolder.get().id, TimeValue.timeValueMillis(2));
            assertThat(response.getException(), nullValue());
            assertThat(response.getResponse(), notNullValue());
            assertThat(response.getResponse().id, equalTo(responseHolder.get().id));
            assertThat(response.getResponse().string, nullValue());
        }

        if (waitForCompletion) {
            // now we are waiting for the task to finish
            logger.trace("Waiting for response to complete");
            AtomicReference<StoredAsyncResponse<TestResponse>> responseRef = new AtomicReference<>();
            // Unblock execute before wait-for-completion so the listener timeout measures task completion, not time blocked on
            // executionLatch.
            executionLatch.countDown();
            CountDownLatch getResponseCountDown = getResponse(
                responseHolder.get().id,
                TimeValue.timeValueSeconds(5),
                ActionTestUtils.assertNoFailureListener(responseRef::set)
            );
            assertThat(getResponseCountDown.await(10, TimeUnit.SECONDS), equalTo(true));

            StoredAsyncResponse<TestResponse> response = responseRef.get();
            if (success) {
                assertThat(response.getException(), nullValue());
                assertThat(response.getResponse(), notNullValue());
                assertThat(response.getResponse().id, equalTo(responseHolder.get().id));
                assertThat(response.getResponse().string, equalTo("response for [" + request.string + "]"));
            } else {
                assertThat(response.getException(), notNullValue());
                assertThat(response.getResponse(), nullValue());
                assertThat(response.getException().getMessage(), equalTo("test exception"));
            }
        } else {
            executionLatch.countDown();
        }

        // finally wait until the task disappears and get the response from the index
        logger.trace("Wait for task to disappear ");
        assertBusy(() -> {
            Task task = transportService.getTaskManager().getTask(AsyncExecutionId.decode(responseHolder.get().id).getTaskId().getId());
            assertThat(task, nullValue());
        });

        logger.trace("Getting the final response from the index");
        StoredAsyncResponse<TestResponse> response = getResponse(responseHolder.get().id, TimeValue.ZERO);
        if (success) {
            assertThat(response.getException(), nullValue());
            assertThat(response.getResponse(), notNullValue());
            assertThat(response.getResponse().string, equalTo("response for [" + request.string + "]"));
        } else {
            assertThat(response.getException(), notNullValue());
            assertThat(response.getResponse(), nullValue());
            assertThat(response.getException().getMessage(), equalTo("test exception"));
        }
    }

    private StoredAsyncResponse<TestResponse> getResponse(String id, TimeValue timeout) throws InterruptedException {
        AtomicReference<StoredAsyncResponse<TestResponse>> response = new AtomicReference<>();
        assertThat(
            getResponse(id, timeout, ActionTestUtils.assertNoFailureListener(response::set)).await(10, TimeUnit.SECONDS),
            equalTo(true)
        );
        return response.get();
    }

    private CountDownLatch getResponse(String id, TimeValue timeout, ActionListener<StoredAsyncResponse<TestResponse>> listener) {
        CountDownLatch responseLatch = new CountDownLatch(1);
        GetAsyncResultRequest getResultsRequest = new GetAsyncResultRequest(id).setWaitForCompletionTimeout(timeout);
        results.retrieveResult(getResultsRequest, ActionListener.wrap(r -> {
            listener.onResponse(r);
            responseLatch.countDown();
        }, e -> {
            listener.onFailure(e);
            responseLatch.countDown();
        }));
        return responseLatch;
    }

}
