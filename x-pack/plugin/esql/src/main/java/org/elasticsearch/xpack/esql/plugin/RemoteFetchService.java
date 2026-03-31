/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Internal transport service that fetches field values for coordinator-selected rows from the owning data node.
 * <p>
 * This is the transport half of the remote late-materialization prototype. It intentionally works on a narrow v1
 * contract: a batch of {@link RemoteFetchHandle}s plus a list of plain field specifications to load.
 */
public final class RemoteFetchService {
    static final String ACTION_NAME = EsqlQueryAction.NAME + "/remote_fetch";
    static final String RELEASE_ACTION_NAME = ACTION_NAME + "/release";

    private static final Logger logger = LogManager.getLogger(RemoteFetchService.class);

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final BigArrays bigArrays;
    private final BlockFactory blockFactory;
    private final PlannerSettings.Holder plannerSettings;
    private final LocalCircuitBreaker.SizeSettings localBreakerSettings;
    private final RetainedSearchContextsRegistry retainedSearchContexts = new RetainedSearchContextsRegistry();

    RemoteFetchService(TransportActionServices transportActionServices, BigArrays bigArrays, BlockFactory blockFactory) {
        this.clusterService = transportActionServices.clusterService();
        this.transportService = transportActionServices.transportService();
        this.bigArrays = bigArrays;
        this.blockFactory = blockFactory;
        this.plannerSettings = transportActionServices.plannerSettings();
        this.localBreakerSettings = new LocalCircuitBreaker.SizeSettings(clusterService.getSettings());
        transportService.registerRequestHandler(
            ACTION_NAME,
            transportService.getThreadPool().executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME),
            in -> new Request(in),
            new TransportHandler()
        );
        transportService.registerRequestHandler(
            RELEASE_ACTION_NAME,
            transportService.getThreadPool().executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME),
            ReleaseRequest::new,
            new ReleaseTransportHandler()
        );
    }

    RetainedSearchContextsRegistry.Registration retainSearchContexts(String sessionId, AcquiredSearchContexts searchContexts) {
        return retainedSearchContexts.register(sessionId, searchContexts);
    }

    void fetchAsync(CancellableTask parentTask, DiscoveryNode targetNode, Request request, ActionListener<Response> listener) {
        transportService.sendChildRequest(
            targetNode,
            ACTION_NAME,
            request,
            parentTask,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(
                listener,
                in -> new Response(in, blockFactory),
                transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
            )
        );
    }

    public void fetchAsync(CancellableTask parentTask, String targetNodeId, Request request, ActionListener<List<Page>> listener) {
        DiscoveryNode targetNode = clusterService.state().nodes().get(targetNodeId);
        if (targetNode == null) {
            listener.onFailure(new IllegalStateException("remote fetch target node [" + targetNodeId + "] not found"));
            return;
        }
        fetchAsync(
            parentTask,
            targetNode,
            request,
            ActionListener.wrap(response -> listener.onResponse(response.takePages()), listener::onFailure)
        );
    }

    void releaseAsync(DiscoveryNode targetNode, String sessionId, ActionListener<Void> listener) {
        transportService.sendRequest(
            targetNode,
            RELEASE_ACTION_NAME,
            new ReleaseRequest(sessionId),
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(
                listener.map(ignored -> null),
                in -> ActionResponse.Empty.INSTANCE,
                transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
            )
        );
    }

    public ThreadContext threadContext() {
        return transportService.getThreadPool().getThreadContext();
    }

    TrackedSessions trackedSessions() {
        return new TrackedSessions(this);
    }

    int retainedSessions() {
        return retainedSearchContexts.retainedSessions();
    }

    boolean isRegistered(String sessionId) {
        return retainedSearchContexts.isRegistered(sessionId);
    }

    private void releaseSession(String sessionId) {
        retainedSearchContexts.closeRegistration(sessionId);
    }

    private void releaseBestEffort(DiscoveryNode targetNode, String sessionId) {
        releaseAsync(targetNode, sessionId, ActionListener.wrap(ignored -> {}, e -> {
            logger.debug("failed to release retained remote fetch session [{}] on node [{}]", sessionId, targetNode.getId(), e);
        }));
    }

    private void doFetch(Request request, ActionListener<Response> listener) {
        request.validateForNode(clusterService.localNode().getId());
        if (request.handles().isEmpty()) {
            ActionListener.respondAndRelease(listener, new Response(List.of(), blockFactory));
            return;
        }
        try (RetainedSearchContextsRegistry.Lease lease = retainedSearchContexts.acquire(request.sessionId())) {
            PlannerSettings settings = plannerSettings.get();
            IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts = lease.searchContexts()
                .map(ComputeSearchContext::shardContext);
            List<ValuesSourceReaderOperator.FieldInfo> fieldInfos = buildFieldInfos(request.fields(), shardContexts, settings);
            IndexedByShardId<ValuesSourceReaderOperator.ShardContext> readerContexts = shardContexts.map(
                c -> new ValuesSourceReaderOperator.ShardContext(
                    c.searcher().getIndexReader(),
                    c::newSourceLoader,
                    c.storedFieldsSequentialProportion()
                )
            );
            List<Page> pages = executeFetch(
                request.handles(),
                fieldInfos,
                readerContexts,
                bigArrays,
                blockFactory,
                localBreakerSettings,
                settings
            );
            ActionListener.respondAndRelease(listener, new Response(pages, blockFactory));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    static List<ValuesSourceReaderOperator.FieldInfo> buildFieldInfos(
        List<FetchField> fields,
        IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts,
        PlannerSettings plannerSettings
    ) {
        List<ValuesSourceReaderOperator.FieldInfo> fieldInfos = new ArrayList<>(fields.size());
        for (FetchField field : fields) {
            fieldInfos.add(
                new ValuesSourceReaderOperator.FieldInfo(
                    field.fieldName(),
                    PlannerUtils.toElementType(field.dataType()),
                    false,
                    (warningsMode, shardIdx) -> {
                        BlockLoader loader = shardContexts.get(shardIdx)
                            .blockLoader(
                                field.fieldName(),
                                field.dataType() == DataType.UNSUPPORTED,
                                MappedFieldType.FieldExtractPreference.NONE,
                                null,
                                null,
                                plannerSettings.blockLoaderSizeOrdinals(),
                                plannerSettings.blockLoaderSizeScript()
                            );
                        return ValuesSourceReaderOperator.load(loader);
                    }
                )
            );
        }
        return fieldInfos;
    }

    static List<Page> executeFetch(
        List<RemoteFetchHandle> handles,
        List<ValuesSourceReaderOperator.FieldInfo> fieldInfos,
        IndexedByShardId<ValuesSourceReaderOperator.ShardContext> shardContexts,
        BigArrays bigArrays,
        BlockFactory blockFactory,
        LocalCircuitBreaker.SizeSettings localBreakerSettings,
        PlannerSettings plannerSettings
    ) {
        if (handles.isEmpty()) {
            return List.of();
        }
        if (fieldInfos.isEmpty()) {
            throw new IllegalArgumentException("remote fetch requires at least one field");
        }

        final LocalCircuitBreaker localBreaker = new LocalCircuitBreaker(
            blockFactory.breaker(),
            localBreakerSettings.overReservedBytes(),
            localBreakerSettings.maxOverReservedBytes()
        );
        final DriverContext driverContext = new DriverContext(
            bigArrays,
            blockFactory.newChildFactory(localBreaker),
            localBreakerSettings,
            "remote_fetch"
        );
        final Operator operator = new ValuesSourceReaderOperator.Factory(
            plannerSettings.valuesLoadingJumboSize(),
            fieldInfos,
            shardContexts,
            fieldInfos.size() <= plannerSettings.reuseColumnLoadersThreshold(),
            0,
            plannerSettings.sourceReservationFactor(),
            plannerSettings.docSequenceBytesRefFieldThreshold()
        ).get(driverContext);
        final int[] projection = new int[fieldInfos.size()];
        for (int i = 0; i < projection.length; i++) {
            projection[i] = i + 1;
        }
        Page inputPage = inputPage(driverContext.blockFactory(), handles);
        boolean releaseInputPage = true;
        boolean success = false;
        List<Page> outputPages = new ArrayList<>();
        try {
            operator.addInput(inputPage);
            releaseInputPage = false;
            operator.finish();
            while (operator.isFinished() == false) {
                Page page = operator.getOutput();
                if (page == null) {
                    throw new IllegalStateException("remote fetch operator stalled without producing output");
                }
                Page projected = page.projectBlocks(projection);
                page.releaseBlocks();
                projected.allowPassingToDifferentDriver();
                outputPages.add(projected);
            }
            success = true;
            return outputPages;
        } finally {
            Releasables.closeExpectNoException(operator, localBreaker);
            if (releaseInputPage) {
                inputPage.releaseBlocks();
            }
            if (success == false) {
                Releasables.closeExpectNoException(Releasables.wrap(Iterators.map(outputPages.iterator(), page -> page::releaseBlocks)));
            }
        }
    }

    static Page inputPage(BlockFactory blockFactory, List<RemoteFetchHandle> handles) {
        try (DocVector.FixedBuilder builder = DocVector.newFixedBuilder(blockFactory, handles.size())) {
            for (RemoteFetchHandle handle : handles) {
                builder.append(handle.shard(), handle.segment(), handle.doc());
            }
            return new Page(builder.build(DocVector.config()).asBlock());
        }
    }

    public static final class FetchField implements Writeable {
        private final String fieldName;
        private final DataType dataType;

        public FetchField(String fieldName, DataType dataType) {
            this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
            this.dataType = Objects.requireNonNull(dataType, "dataType");
        }

        FetchField(StreamInput in) throws IOException {
            this(in.readString(), DataType.fromTypeName(in.readString()));
        }

        public String fieldName() {
            return fieldName;
        }

        public DataType dataType() {
            return dataType;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(fieldName);
            out.writeString(dataType.typeName());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof FetchField == false) {
                return false;
            }
            FetchField other = (FetchField) obj;
            return fieldName.equals(other.fieldName) && dataType == other.dataType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, dataType);
        }

        @Override
        public String toString() {
            return fieldName + ":" + dataType.typeName();
        }
    }

    public static final class Request extends AbstractTransportRequest {
        private final String sessionId;
        private final List<FetchField> fields;
        private final List<RemoteFetchHandle> handles;

        public Request(String sessionId, List<FetchField> fields, List<RemoteFetchHandle> handles) {
            this.sessionId = Objects.requireNonNull(sessionId, "sessionId");
            this.fields = List.copyOf(fields);
            this.handles = List.copyOf(handles);
        }

        Request(StreamInput in) throws IOException {
            super(in);
            this.sessionId = in.readString();
            this.fields = in.readCollectionAsList(FetchField::new);
            this.handles = in.readCollectionAsList(RemoteFetchHandle.READER);
        }

        String sessionId() {
            return sessionId;
        }

        List<FetchField> fields() {
            return fields;
        }

        List<RemoteFetchHandle> handles() {
            return handles;
        }

        void validateForNode(String localNodeId) {
            if (fields.isEmpty()) {
                throw new IllegalArgumentException("remote fetch requires at least one field");
            }
            for (RemoteFetchHandle handle : handles) {
                if (sessionId.equals(handle.sessionId()) == false) {
                    throw new IllegalArgumentException(
                        "remote fetch request session [" + sessionId + "] does not match handle session [" + handle.sessionId() + "]"
                    );
                }
                if (localNodeId.equals(handle.nodeId()) == false) {
                    throw new IllegalArgumentException(
                        "remote fetch handle node [" + handle.nodeId() + "] does not match local node [" + localNodeId + "]"
                    );
                }
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sessionId);
            out.writeCollection(fields);
            out.writeCollection(handles);
        }

        @Override
        public String toString() {
            return "RemoteFetchRequest[session=" + sessionId + ", fields=" + fields + ", handles=" + handles.size() + "]";
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof Request == false) {
                return false;
            }
            Request other = (Request) obj;
            return sessionId.equals(other.sessionId) && fields.equals(other.fields) && handles.equals(other.handles);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sessionId, fields, handles);
        }
    }

    static final class ReleaseRequest extends AbstractTransportRequest {
        private final String sessionId;

        ReleaseRequest(String sessionId) {
            this.sessionId = Objects.requireNonNull(sessionId, "sessionId");
        }

        ReleaseRequest(StreamInput in) throws IOException {
            super(in);
            this.sessionId = in.readString();
        }

        String sessionId() {
            return sessionId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sessionId);
        }
    }

    static final class Response extends TransportResponse implements RefCounted {
        private final RefCounted refs = AbstractRefCounted.of(this::release);
        private final BlockFactory blockFactory;
        private List<Page> pages;
        private long reservedBytes;

        Response(List<Page> pages, BlockFactory blockFactory) {
            this.pages = List.copyOf(pages);
            this.blockFactory = blockFactory;
        }

        Response(StreamInput in, BlockFactory blockFactory) throws IOException {
            this.blockFactory = blockFactory;
            try (BlockStreamInput blockStreamInput = new BlockStreamInput(in, blockFactory)) {
                this.pages = blockStreamInput.readCollectionAsList(Page::new);
            }
        }

        List<Page> pages() {
            return pages;
        }

        List<Page> takePages() {
            List<Page> taken = pages;
            pages = null;
            return taken;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            long bytes = pages.stream().mapToLong(Page::ramBytesUsedByBlocks).sum();
            blockFactory.breaker().addEstimateBytesAndMaybeBreak(bytes, "serialize remote fetch response");
            reservedBytes += bytes;
            out.writeCollection(pages);
        }

        private void release() {
            blockFactory.breaker().addWithoutBreaking(-reservedBytes);
            if (pages != null) {
                Releasables.closeExpectNoException(Releasables.wrap(Iterators.map(pages.iterator(), page -> page::releaseBlocks)));
                pages = null;
            }
        }

        @Override
        public void incRef() {
            refs.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return refs.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return refs.decRef();
        }

        @Override
        public boolean hasReferences() {
            return refs.hasReferences();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof Response == false) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(pages, other.pages);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(pages);
        }
    }

    private class TransportHandler implements TransportRequestHandler<Request> {
        @Override
        public void messageReceived(Request request, TransportChannel channel, Task task) {
            doFetch(request, new ChannelActionListener<>(channel));
        }
    }

    private class ReleaseTransportHandler implements TransportRequestHandler<ReleaseRequest> {
        @Override
        public void messageReceived(ReleaseRequest request, TransportChannel channel, Task task) throws Exception {
            releaseSession(request.sessionId());
            channel.sendResponse(ActionResponse.Empty.INSTANCE);
        }
    }

    static final class TrackedSessions implements Releasable {
        private final RemoteFetchService remoteFetchService;
        private final List<TrackedSession> sessions = new ArrayList<>();
        private boolean closed;

        private TrackedSessions(RemoteFetchService remoteFetchService) {
            this.remoteFetchService = remoteFetchService;
        }

        synchronized void track(DiscoveryNode targetNode, String sessionId) {
            if (closed) {
                remoteFetchService.releaseBestEffort(targetNode, sessionId);
                return;
            }
            sessions.add(new TrackedSession(targetNode, sessionId));
        }

        @Override
        public void close() {
            List<TrackedSession> tracked;
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                tracked = List.copyOf(sessions);
                sessions.clear();
            }
            for (TrackedSession session : tracked) {
                remoteFetchService.releaseBestEffort(session.targetNode(), session.sessionId());
            }
        }
    }

    private record TrackedSession(DiscoveryNode targetNode, String sessionId) {}
}
