/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.RemoteException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.compute.operator.topn.TopNOperator.InputOrdering;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.datasources.FilterPushdownRegistry;
import org.elasticsearch.xpack.esql.datasources.OperatorFactoryRegistry;
import org.elasticsearch.xpack.esql.datasources.SplitCoalescer;
import org.elasticsearch.xpack.esql.datasources.SplitDiscoveryPhase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupService;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexService;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalVerifier;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;
import org.elasticsearch.xpack.esql.plan.logical.TsInfo;
import org.elasticsearch.xpack.esql.plan.materialize.MaterializeTarget;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.FuseScoreEvalExec;
import org.elasticsearch.xpack.esql.plan.physical.MaterializeExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlCCSUtils;
import org.elasticsearch.xpack.esql.session.Result;
import org.elasticsearch.xpack.esql.stats.SearchContextStats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.action.EsqlExecutionInfo.IncludeExecutionMetadata.ALWAYS;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME;

/**
 * Once query is parsed and validated it is scheduled for execution by {@code org.elasticsearch.xpack.esql.plugin.ComputeService#execute}
 * This method is responsible for splitting physical plan into coordinator and data node plans.
 * <p>
 * Coordinator plan is immediately executed locally (using {@code org.elasticsearch.xpack.esql.plugin.ComputeService#runCompute})
 * and is prepared to collect and merge pages from data nodes into the final query result.
 * <p>
 * Data node plan is passed to {@code org.elasticsearch.xpack.esql.plugin.DataNodeComputeHandler#startComputeOnDataNodes}
 * that is responsible for
 * <ul>
 * <li>
 *     Determining list of nodes that contain shards referenced by the query with
 *     {@code org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender#searchShards}
 * </li>
 * <li>
 *     Each node in the list processed in
 *     {@code org.elasticsearch.xpack.esql.plugin.DataNodeComputeHandler#startComputeOnDataNodes}
 *     in order to
 *     <ul>
 *     <li>
 *         Open ExchangeSink on the target data node and link it with local ExchangeSource for the query
 *         using `internal:data/read/esql/open_exchange` transport request.
 *         {@see org.elasticsearch.compute.operator.exchange.ExchangeService#openExchange}
 *     </li>
 *     <li>
 *         Start data node plan execution on the target data node
 *         using `indices:data/read/esql/data` transport request
 *         {@see org.elasticsearch.xpack.esql.plugin.DataNodeComputeHandler#messageReceived}
 *         {@see org.elasticsearch.xpack.esql.plugin.DataNodeComputeHandler#runComputeOnDataNode}
 *     </li>
 *     <li>
 *         While coordinator plan executor is running it will read data from ExchangeSource that will poll pages
 *         from linked ExchangeSink on target data nodes or notify them that data set is already completed
 *         (for example when running FROM * | LIMIT 10 type of query) or query is canceled
 *         using `internal:data/read/esql/exchange` transport requests.
 *         {@see org.elasticsearch.compute.operator.exchange.ExchangeService.ExchangeTransportAction#messageReceived}
 *     </li>
 *     </ul>
 * </li>
 * </ul>
 */
public class ComputeService {
    public static final String DATA_DESCRIPTION = "data";
    public static final String REDUCE_DESCRIPTION = "node_reduce";
    public static final String DATA_ACTION_NAME = EsqlQueryAction.NAME + "/data";
    public static final String CLUSTER_ACTION_NAME = EsqlQueryAction.NAME + "/cluster";
    private static final String LOCAL_CLUSTER = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;

    private static final Logger LOGGER = LogManager.getLogger(ComputeService.class);
    private final SearchService searchService;
    private final BigArrays bigArrays;
    private final BlockFactory blockFactory;

    private final TransportService transportService;
    private final DriverTaskRunner driverRunner;
    private final EnrichLookupService enrichLookupService;
    private final LookupFromIndexService lookupFromIndexService;
    private final InferenceService inferenceService;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final AtomicLong childSessionIdGenerator = new AtomicLong();
    private final DataNodeComputeHandler dataNodeComputeHandler;
    private final ClusterComputeHandler clusterComputeHandler;
    private final ExchangeService exchangeService;
    private final PlannerSettings.Holder plannerSettings;
    private final OperatorFactoryRegistry operatorFactoryRegistry;
    private final FilterPushdownRegistry filterPushdownRegistry;

    @SuppressWarnings("this-escape")
    public ComputeService(
        TransportActionServices transportActionServices,
        EnrichLookupService enrichLookupService,
        LookupFromIndexService lookupFromIndexService,
        ThreadPool threadPool,
        BigArrays bigArrays,
        BlockFactory blockFactory,
        OperatorFactoryRegistry operatorFactoryRegistry,
        FilterPushdownRegistry filterPushdownRegistry
    ) {
        this.searchService = transportActionServices.searchService();
        this.transportService = transportActionServices.transportService();
        this.exchangeService = transportActionServices.exchangeService();
        this.bigArrays = bigArrays.withCircuitBreaking();
        this.blockFactory = blockFactory;
        var esqlExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
        this.driverRunner = new DriverTaskRunner(transportService, esqlExecutor);
        this.enrichLookupService = enrichLookupService;
        this.lookupFromIndexService = lookupFromIndexService;
        this.inferenceService = transportActionServices.inferenceService();
        this.clusterService = transportActionServices.clusterService();
        this.projectResolver = transportActionServices.projectResolver();
        this.dataNodeComputeHandler = new DataNodeComputeHandler(
            this,
            clusterService,
            projectResolver,
            searchService,
            transportService,
            exchangeService,
            esqlExecutor
        );
        this.clusterComputeHandler = new ClusterComputeHandler(
            this,
            exchangeService,
            transportService,
            esqlExecutor,
            dataNodeComputeHandler
        );
        this.plannerSettings = transportActionServices.plannerSettings();
        this.operatorFactoryRegistry = operatorFactoryRegistry;
        this.filterPushdownRegistry = filterPushdownRegistry != null ? filterPushdownRegistry : FilterPushdownRegistry.empty();
    }

    PlannerSettings.Holder plannerSettings() {
        return plannerSettings;
    }

    FilterPushdownRegistry filterPushdownRegistry() {
        return filterPushdownRegistry;
    }

    PhysicalPlan discoverSplits(PhysicalPlan plan) {
        if (operatorFactoryRegistry == null) {
            return plan;
        }
        try {
            PhysicalPlan discovered = SplitDiscoveryPhase.resolveExternalSplits(plan, operatorFactoryRegistry.sourceFactories());
            return coalesceSplits(discovered);
        } catch (Exception e) {
            LOGGER.warn("split discovery failed for external source", e);
            throw e;
        }
    }

    static PhysicalPlan coalesceSplits(PhysicalPlan plan) {
        return plan.transformUp(ExternalSourceExec.class, exec -> {
            List<ExternalSplit> splits = exec.splits();
            if (splits.size() <= SplitCoalescer.COALESCING_THRESHOLD) {
                return exec;
            }
            List<ExternalSplit> coalesced = SplitCoalescer.coalesce(splits);
            if (coalesced == splits) {
                return exec;
            }
            return exec.withSplits(coalesced);
        });
    }

    static ExternalDistributionStrategy resolveExternalDistributionStrategy(QueryPragmas pragmas) {
        String value = pragmas.externalDistribution();
        return switch (value) {
            case "", "adaptive" -> new AdaptiveStrategy();
            case "coordinator_only" -> CoordinatorOnlyStrategy.INSTANCE;
            case "round_robin" -> new RoundRobinStrategy();
            case "weighted_round_robin" -> new WeightedRoundRobinStrategy();
            default -> {
                LOGGER.warn("unknown external_distribution pragma value [{}]; falling back to adaptive", value);
                yield new AdaptiveStrategy();
            }
        };
    }

    ExternalDistributionResult applyExternalDistributionStrategy(PhysicalPlan plan, Configuration configuration) {
        List<ExternalSplit> externalSplits = collectExternalSplits(plan);
        if (externalSplits.isEmpty()) {
            return new ExternalDistributionResult(collapseExternalSourceExchanges(plan), null, List.of());
        }

        ExternalDistributionStrategy strategy = resolveExternalDistributionStrategy(configuration.pragmas());
        ExternalDistributionContext context = new ExternalDistributionContext(
            plan,
            externalSplits,
            clusterService.state().nodes(),
            configuration.pragmas()
        );

        ExternalDistributionPlan distributionPlan = strategy.planDistribution(context);

        if (distributionPlan.distributed()) {
            LOGGER.debug(
                "external distribution: distributing {} splits across {} nodes",
                externalSplits.size(),
                distributionPlan.nodeAssignments().size()
            );
            return new ExternalDistributionResult(plan, distributionPlan, List.of());
        }

        return new ExternalDistributionResult(collapseExternalSourceExchanges(plan), null, externalSplits);
    }

    record ExternalDistributionResult(PhysicalPlan plan, ExternalDistributionPlan distributionPlan, List<ExternalSplit> coordinatorSplits) {
        boolean isDistributed() {
            return distributionPlan != null && distributionPlan.distributed();
        }
    }

    private List<ExternalSplit> collectExternalSplits(PhysicalPlan plan) {
        List<ExternalSplit> splits = new ArrayList<>();
        plan.forEachDown(ExternalSourceExec.class, exec -> splits.addAll(exec.splits()));
        if (splits.isEmpty()) {
            discoverSplitsFromFragments(plan, splits);
            if (splits.size() > SplitCoalescer.COALESCING_THRESHOLD) {
                List<ExternalSplit> coalesced = SplitCoalescer.coalesce(splits);
                if (coalesced != splits) {
                    splits.clear();
                    splits.addAll(coalesced);
                }
            }
        }
        return splits;
    }

    private void discoverSplitsFromFragments(PhysicalPlan plan, List<ExternalSplit> splits) {
        if (operatorFactoryRegistry == null) {
            return;
        }
        plan.forEachDown(FragmentExec.class, fragment -> {
            fragment.fragment().forEachDown(ExternalRelation.class, external -> {
                ExternalSourceExec tempExec = external.toPhysicalExec();
                PhysicalPlan discovered = SplitDiscoveryPhase.resolveExternalSplits(tempExec, operatorFactoryRegistry.sourceFactories());
                if (discovered instanceof ExternalSourceExec withSplits) {
                    splits.addAll(withSplits.splits());
                }
            });
        });
    }

    static PhysicalPlan collapseExternalSourceExchanges(PhysicalPlan plan) {
        PhysicalPlan collapsed = plan.transformUp(ExchangeExec.class, exchange -> {
            if (exchange.child() instanceof ExternalSourceExec) {
                return exchange.child();
            }
            if (exchange.child() instanceof FragmentExec fragment && fragment.fragment().anyMatch(ExternalRelation.class::isInstance)) {
                return exchange.child();
            }
            return exchange;
        });
        return collapsed.transformUp(TopNExec.class, topN -> {
            if (topN.inputOrdering() != InputOrdering.NOT_SORTED && topN.child() instanceof FragmentExec) {
                return topN.withNonSortedInput();
            }
            return topN;
        });
    }

    public void execute(
        String sessionId,
        CancellableTask rootTask,
        EsqlFlags flags,
        PhysicalPlan physicalPlan,
        Configuration configuration,
        FoldContext foldContext,
        EsqlExecutionInfo execInfo,
        PlanTimeProfile planTimeProfile,
        ActionListener<Result> listener
    ) {
        assert ThreadPool.assertCurrentThreadPool(
            ESQL_WORKER_THREAD_POOL_NAME,
            ThreadPool.Names.SYSTEM_READ,
            ThreadPool.Names.SEARCH,
            ThreadPool.Names.SEARCH_COORDINATION
        );
        Tuple<List<PhysicalPlan>, PhysicalPlan> subplansAndMainPlan = PlannerUtils.breakPlanIntoSubPlansAndMainPlan(physicalPlan);

        List<PhysicalPlan> subplans = subplansAndMainPlan.v1();

        // take a snapshot of the initial cluster statuses, this is the status after index resolutions,
        // and it will be checked before executing data node plan on remote clusters
        Map<String, EsqlExecutionInfo.Cluster.Status> initialClusterStatuses = new HashMap<>(execInfo.clusterInfo.size());
        for (Map.Entry<String, EsqlExecutionInfo.Cluster> entry : execInfo.clusterInfo.entrySet()) {
            initialClusterStatuses.put(entry.getKey(), entry.getValue().getStatus());
        }

        // we have no sub plans, so we can just execute the given plan
        if (subplans == null || subplans.isEmpty()) {
            executePlan(
                sessionId,
                rootTask,
                flags,
                physicalPlan,
                configuration,
                foldContext,
                execInfo,
                null,
                listener,
                null,
                initialClusterStatuses,
                planTimeProfile
            );
            return;
        }

        ForkLateMaterializationPlan forkLateMaterializationPlan = maybePlanForkLateMaterialization(
            flags,
            configuration,
            foldContext,
            subplans,
            subplansAndMainPlan.v2(),
            planTimeProfile
        );
        if (forkLateMaterializationPlan != null) {
            executeForkLateMaterialization(
                sessionId,
                rootTask,
                flags,
                configuration,
                foldContext,
                execInfo,
                listener,
                forkLateMaterializationPlan
            );
            return;
        }

        final List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());
        PhysicalPlan mainPlan = new OutputExec(subplansAndMainPlan.v2(), collectedPages::add);

        listener = listener.delegateResponse((l, e) -> {
            collectedPages.forEach(p -> Releasables.closeExpectNoException(p::releaseBlocks));
            l.onFailure(e);
        });

        var mainSessionId = newChildSession(sessionId);
        QueryPragmas queryPragmas = configuration.pragmas();

        ExchangeSourceHandler mainExchangeSource = new ExchangeSourceHandler(
            queryPragmas.exchangeBufferSize(),
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
        );

        exchangeService.addExchangeSourceHandler(mainSessionId, mainExchangeSource);
        try (var ignored = mainExchangeSource.addEmptySink()) {
            var finalListener = ActionListener.runBefore(listener, () -> exchangeService.removeExchangeSourceHandler(sessionId));
            var computeContext = new ComputeContext(
                mainSessionId,
                "main.final",
                LOCAL_CLUSTER,
                flags,
                EmptyIndexedByShardId.instance(),
                configuration,
                foldContext,
                mainExchangeSource::createExchangeSource,
                null
            );

            Runnable cancelQueryOnFailure = cancelQueryOnFailure(rootTask);

            try (
                ComputeListener localListener = new ComputeListener(
                    transportService.getThreadPool(),
                    cancelQueryOnFailure,
                    finalListener.map(profiles -> {
                        execInfo.markEndQuery();
                        return new Result(mainPlan.output(), collectedPages, configuration, profiles, execInfo);
                    })
                )
            ) {
                runCompute(
                    rootTask,
                    computeContext,
                    mainPlan,
                    plannerSettings.get(),
                    LocalPhysicalOptimization.ENABLED,
                    planTimeProfile,
                    localListener.acquireCompute()
                );

                for (int i = 0; i < subplans.size(); i++) {
                    var subplan = subplans.get(i);
                    var childSessionId = newChildSession(sessionId);
                    ExchangeSinkHandler exchangeSink = exchangeService.createSinkHandler(childSessionId, queryPragmas.exchangeBufferSize());
                    // funnel sub plan pages into the main plan exchange source
                    mainExchangeSource.addRemoteSink(exchangeSink::fetchPageAsync, true, () -> {}, 1, ActionListener.noop());
                    var subPlanListener = localListener.acquireCompute();

                    executePlan(
                        childSessionId,
                        rootTask,
                        flags,
                        subplan,
                        configuration,
                        foldContext,
                        execInfo,
                        "subplan-" + i,
                        ActionListener.wrap(result -> {
                            exchangeSink.addCompletionListener(
                                ActionListener.running(() -> { exchangeService.finishSinkHandler(childSessionId, null); })
                            );
                            subPlanListener.onResponse(result.completionInfo());
                        }, e -> {
                            exchangeService.finishSinkHandler(childSessionId, e);
                            subPlanListener.onFailure(e);
                        }),
                        () -> exchangeSink.createExchangeSink(() -> {}),
                        initialClusterStatuses,
                        configuration.profile() ? new PlanTimeProfile() : null
                    );
                }
            }
        }
    }

    public void executePlan(
        String sessionId,
        CancellableTask rootTask,
        EsqlFlags flags,
        PhysicalPlan physicalPlan,
        Configuration configuration,
        FoldContext foldContext,
        EsqlExecutionInfo execInfo,
        String profileQualifier,
        ActionListener<Result> listener,
        Supplier<ExchangeSink> exchangeSinkSupplier,
        Map<String, EsqlExecutionInfo.Cluster.Status> initialClusterStatuses,
        PlanTimeProfile planTimeProfile
    ) {
        final PhysicalPlan splitPlan = discoverSplits(physicalPlan);
        final ExternalDistributionResult distributionResult = applyExternalDistributionStrategy(splitPlan, configuration);
        final PhysicalPlan resolvedPlan = distributionResult.plan();
        Tuple<PhysicalPlan, PhysicalPlan> coordinatorAndDataNodePlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(
            resolvedPlan,
            configuration
        );
        final List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());
        listener = listener.delegateResponse((l, e) -> {
            collectedPages.forEach(p -> Releasables.closeExpectNoException(p::releaseBlocks));
            l.onFailure(e);
        });
        PhysicalPlan coordinatorPlan = coordinatorAndDataNodePlan.v1();

        if (exchangeSinkSupplier == null) {
            coordinatorPlan = new OutputExec(coordinatorAndDataNodePlan.v1(), collectedPages::add);
        }

        PhysicalPlan dataNodePlan = coordinatorAndDataNodePlan.v2();
        if (dataNodePlan != null && dataNodePlan instanceof ExchangeSinkExec == false) {
            assert false : "expected data node plan starts with an ExchangeSink; got " + dataNodePlan;
            listener.onFailure(new IllegalStateException("expected data node plan starts with an ExchangeSink; got " + dataNodePlan));
            return;
        }
        Map<String, OriginalIndices> clusterToConcreteIndices = getIndices(resolvedPlan, EsRelation::concreteIndices);
        Runnable cancelQueryOnFailure = cancelQueryOnFailure(rootTask);
        boolean hasConcreteIndices = false;
        for (OriginalIndices v : clusterToConcreteIndices.values()) {
            if (v.indices().length > 0) {
                hasConcreteIndices = true;
                break;
            }
        }
        if (dataNodePlan == null) {
            if (hasConcreteIndices) {
                String error = "expected no concrete indices without data node plan; got " + clusterToConcreteIndices;
                assert false : error;
                listener.onFailure(new IllegalStateException(error));
                return;
            }
            var computeContext = new ComputeContext(
                newChildSession(sessionId),
                profileDescription(profileQualifier, "single"),
                LOCAL_CLUSTER,
                flags,
                EmptyIndexedByShardId.instance(),
                configuration,
                foldContext,
                null,
                exchangeSinkSupplier
            );
            updateShardCountForCoordinatorOnlyQuery(execInfo);
            try (
                var computeListener = new ComputeListener(
                    transportService.getThreadPool(),
                    cancelQueryOnFailure,
                    listener.map(completionInfo -> {
                        updateExecutionInfoAfterCoordinatorOnlyQuery(execInfo);
                        return new Result(resolvedPlan.output(), collectedPages, configuration, completionInfo, execInfo);
                    })
                )
            ) {
                runCompute(
                    rootTask,
                    computeContext,
                    coordinatorPlan,
                    plannerSettings.get(),
                    LocalPhysicalOptimization.ENABLED,
                    distributionResult.coordinatorSplits(),
                    planTimeProfile,
                    computeListener.acquireCompute()
                );
                return;
            }
        }
        // External source distribution: data node plan exists but no ES indices
        if (distributionResult.isDistributed() && hasConcreteIndices == false) {
            executeExternalDistribution(
                sessionId,
                rootTask,
                flags,
                configuration,
                foldContext,
                (ExchangeSinkExec) dataNodePlan,
                coordinatorPlan,
                resolvedPlan,
                distributionResult.distributionPlan(),
                collectedPages,
                execInfo,
                profileQualifier,
                cancelQueryOnFailure,
                exchangeSinkSupplier,
                planTimeProfile,
                listener
            );
            return;
        }
        if (hasConcreteIndices == false) {
            var error = "expected concrete indices with data node plan but got empty; data node plan " + dataNodePlan;
            assert false : error;
            listener.onFailure(new IllegalStateException(error));
            return;
        }
        Map<String, OriginalIndices> clusterToOriginalIndices = getIndices(resolvedPlan, EsRelation::originalIndices);
        var localOriginalIndices = clusterToOriginalIndices.remove(LOCAL_CLUSTER);
        var localConcreteIndices = clusterToConcreteIndices.remove(LOCAL_CLUSTER);
        /*
         * Grab the output attributes here, so we can pass them to
         * the listener without holding on to a reference to the
         * entire plan.
         */
        List<Attribute> outputAttributes = resolvedPlan.output();
        /*
         * In the narrow single-node case we can move the late-materialization subtree one step further, from the
         * logical node-reduce driver into the coordinator's final driver. The data-side fragment still runs through
         * a local exchange, but _doc-bearing pages never cross transport and final can reuse the same shard contexts.
         */
        ReductionPlan localReductionPlan = maybePlanLocalSingleNodeLateMaterialization(
            flags,
            configuration,
            foldContext,
            (ExchangeSinkExec) dataNodePlan,
            localConcreteIndices,
            localOriginalIndices,
            clusterToConcreteIndices,
            clusterToOriginalIndices,
            planTimeProfile
        );
        if (localReductionPlan != null) {
            executeLocalSingleNodeLateMaterialization(
                sessionId,
                rootTask,
                flags,
                configuration,
                foldContext,
                coordinatorPlan,
                (ExchangeSinkExec) dataNodePlan,
                localReductionPlan,
                localOriginalIndices,
                localConcreteIndices,
                collectedPages,
                outputAttributes,
                execInfo,
                profileQualifier,
                cancelQueryOnFailure,
                exchangeSinkSupplier,
                planTimeProfile,
                listener,
                null,
                MaterializeTarget.CURRENT_FINAL,
                Map.of()
            );
            return;
        }
        var exchangeSource = new ExchangeSourceHandler(
            configuration.pragmas().exchangeBufferSize(),
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
        );
        listener = ActionListener.runBefore(listener, () -> exchangeService.removeExchangeSourceHandler(sessionId));
        exchangeService.addExchangeSourceHandler(sessionId, exchangeSource);
        try (
            var computeListener = new ComputeListener(
                transportService.getThreadPool(),
                cancelQueryOnFailure,
                listener.delegateFailureAndWrap((l, completionInfo) -> {
                    failIfAllShardsFailed(execInfo, collectedPages);
                    execInfo.markEndQuery();
                    l.onResponse(new Result(outputAttributes, collectedPages, configuration, completionInfo, execInfo));
                })
            )
        ) {
            try (Releasable ignored = exchangeSource.addEmptySink()) {
                // run compute on the coordinator
                final AtomicBoolean localClusterWasInterrupted = new AtomicBoolean();
                try (
                    var localListener = new ComputeListener(
                        transportService.getThreadPool(),
                        cancelQueryOnFailure,
                        computeListener.acquireCompute().delegateFailure((l, completionInfo) -> {
                            if (execInfo.clusterInfo.containsKey(LOCAL_CLUSTER)) {
                                execInfo.swapCluster(LOCAL_CLUSTER, (k, v) -> {
                                    var tookTime = execInfo.queryProfile().total().timeSinceStarted();
                                    var builder = new EsqlExecutionInfo.Cluster.Builder(v).setTook(tookTime);
                                    if (execInfo.isMainPlan() && v.getStatus() == EsqlExecutionInfo.Cluster.Status.RUNNING) {
                                        final Integer failedShards = execInfo.getCluster(LOCAL_CLUSTER).getFailedShards();
                                        // Set the local cluster status (including the final driver) to partial if the query was stopped
                                        // or encountered resolution or execution failures.
                                        var status = localClusterWasInterrupted.get()
                                            || (failedShards != null && failedShards > 0)
                                            || v.getFailures().isEmpty() == false
                                                ? EsqlExecutionInfo.Cluster.Status.PARTIAL
                                                : EsqlExecutionInfo.Cluster.Status.SUCCESSFUL;
                                        builder.setStatus(status);
                                    }
                                    return builder.build();
                                });
                            }
                            l.onResponse(completionInfo);
                        })
                    )
                ) {
                    runCompute(
                        rootTask,
                        new ComputeContext(
                            sessionId,
                            profileDescription(profileQualifier, "final"),
                            LOCAL_CLUSTER,
                            flags,
                            EmptyIndexedByShardId.instance(),
                            configuration,
                            foldContext,
                            exchangeSource::createExchangeSource,
                            exchangeSinkSupplier
                        ),
                        coordinatorPlan,
                        plannerSettings.get(),
                        LocalPhysicalOptimization.ENABLED,
                        planTimeProfile,
                        localListener.acquireCompute()
                    );
                    // starts computes on data nodes on the main cluster
                    if (localConcreteIndices != null && localConcreteIndices.indices().length > 0) {
                        final var dataNodesListener = localListener.acquireCompute();
                        dataNodeComputeHandler.startComputeOnDataNodes(
                            sessionId,
                            LOCAL_CLUSTER,
                            rootTask,
                            flags,
                            configuration,
                            dataNodePlan,
                            Set.of(localConcreteIndices.indices()),
                            localOriginalIndices,
                            exchangeSource,
                            cancelQueryOnFailure,
                            ActionListener.wrap(r -> {
                                localClusterWasInterrupted.set(execInfo.isStopped());
                                execInfo.swapCluster(
                                    LOCAL_CLUSTER,
                                    (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setTotalShards(r.getTotalShards())
                                        .setSuccessfulShards(r.getSuccessfulShards())
                                        .setSkippedShards(r.getSkippedShards())
                                        .setFailedShards(r.getFailedShards())
                                        .addFailures(r.failures)
                                        .build()
                                );
                                dataNodesListener.onResponse(r.getCompletionInfo());
                            }, e -> {
                                if (configuration.allowPartialResults() && EsqlCCSUtils.canAllowPartial(e)) {
                                    execInfo.swapCluster(
                                        LOCAL_CLUSTER,
                                        (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setStatus(
                                            EsqlExecutionInfo.Cluster.Status.PARTIAL
                                        ).addFailures(List.of(new ShardSearchFailure(e))).build()
                                    );
                                    dataNodesListener.onResponse(DriverCompletionInfo.EMPTY);
                                } else {
                                    dataNodesListener.onFailure(e);
                                }
                            })
                        );
                    }
                }
                // starts computes on remote clusters
                final var remoteClusters = clusterComputeHandler.getRemoteClusters(clusterToConcreteIndices, clusterToOriginalIndices);
                for (ClusterComputeHandler.RemoteCluster cluster : remoteClusters) {
                    String clusterAlias = cluster.clusterAlias();
                    // Check the initial cluster status set by planning phase before executing the data node plan on remote clusters,
                    // only if this is a fork or subquery branch (exchangeSinkSupplier is not null).
                    EsqlExecutionInfo.Cluster.Status clusterStatus = exchangeSinkSupplier != null
                        ? initialClusterStatuses.get(clusterAlias)
                        : execInfo.getCluster(clusterAlias).getStatus();
                    if (clusterStatus != EsqlExecutionInfo.Cluster.Status.RUNNING) {
                        // if the cluster is already in the terminal state from the planning stage, no need to call it
                        // the initial cluster status is collected before the query is executed
                        LOGGER.trace(
                            "skipping execution on remote cluster [{}] since its initial status is [{}]",
                            clusterAlias,
                            clusterStatus
                        );
                        continue;
                    }
                    clusterComputeHandler.startComputeOnRemoteCluster(
                        sessionId,
                        rootTask,
                        configuration,
                        dataNodePlan,
                        exchangeSource,
                        cluster,
                        cancelQueryOnFailure,
                        execInfo,
                        computeListener.acquireCompute().delegateResponse((l, ex) -> {
                            /*
                             * At various points, when collecting failures before sending a response, we manually check
                             * if an ex is a transport error and if it is, we unwrap it. Because we're wrapping an ex
                             * in RemoteException, the checks fail and unwrapping does not happen. We offload the
                             * unwrapping to here.
                             *
                             * Note: The other error we explicitly check for is TaskCancelledException which is never
                             * wrapped.
                             */
                            if (ex instanceof TransportException te) {
                                l.onFailure(new RemoteException(cluster.clusterAlias(), FailureCollector.unwrapTransportException(te)));
                            } else {
                                l.onFailure(new RemoteException(cluster.clusterAlias(), ex));
                            }
                        })
                    );
                }
            }
        }
    }

    private void executeExternalDistribution(
        String sessionId,
        CancellableTask rootTask,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldContext,
        ExchangeSinkExec dataNodePlan,
        PhysicalPlan coordinatorPlan,
        PhysicalPlan resolvedPlan,
        ExternalDistributionPlan distributionPlan,
        List<Page> collectedPages,
        EsqlExecutionInfo execInfo,
        String profileQualifier,
        Runnable cancelQueryOnFailure,
        Supplier<ExchangeSink> exchangeSinkSupplier,
        PlanTimeProfile planTimeProfile,
        ActionListener<Result> listener
    ) {
        List<Attribute> outputAttributes = resolvedPlan.output();
        var exchangeSource = new ExchangeSourceHandler(
            configuration.pragmas().exchangeBufferSize(),
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
        );
        listener = ActionListener.runBefore(listener, () -> exchangeService.removeExchangeSourceHandler(sessionId));
        exchangeService.addExchangeSourceHandler(sessionId, exchangeSource);
        try (
            var computeListener = new ComputeListener(
                transportService.getThreadPool(),
                cancelQueryOnFailure,
                listener.delegateFailureAndWrap((l, completionInfo) -> {
                    execInfo.markEndQuery();
                    l.onResponse(new Result(outputAttributes, collectedPages, configuration, completionInfo, execInfo));
                })
            )
        ) {
            try (Releasable ignored = exchangeSource.addEmptySink()) {
                // Run the coordinator plan
                runCompute(
                    rootTask,
                    new ComputeContext(
                        sessionId,
                        profileDescription(profileQualifier, "final"),
                        LOCAL_CLUSTER,
                        flags,
                        EmptyIndexedByShardId.instance(),
                        configuration,
                        foldContext,
                        exchangeSource::createExchangeSource,
                        exchangeSinkSupplier
                    ),
                    coordinatorPlan,
                    plannerSettings.get(),
                    LocalPhysicalOptimization.ENABLED,
                    planTimeProfile,
                    computeListener.acquireCompute()
                );
                // Dispatch to each data node with its assigned splits
                dataNodeComputeHandler.startExternalComputeOnDataNodes(
                    sessionId,
                    rootTask,
                    flags,
                    configuration,
                    dataNodePlan,
                    distributionPlan,
                    exchangeSource,
                    cancelQueryOnFailure,
                    computeListener
                );
            }
        }
    }

    private ForkLateMaterializationPlan maybePlanForkLateMaterialization(
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldContext,
        List<PhysicalPlan> subplans,
        PhysicalPlan mainPlan,
        PlanTimeProfile planTimeProfile
    ) {
        if (subplans.isEmpty()
            || configuration.pragmas().nodeLevelReduction() == false
            || EsqlCapabilities.Cap.ENABLE_REDUCE_NODE_LATE_MATERIALIZATION.isEnabled() == false
            || clusterService.state().nodes().getDataNodes().size() != 1
            || mainPlan.anyMatch(FuseScoreEvalExec.class::isInstance) == false
            || mainPlan.anyMatch(TopNExec.class::isInstance) == false) {
            return null;
        }

        List<ForkSubplanLateMaterializationPlan> plannedSubplans = new ArrayList<>(subplans.size());
        Map<String, FieldAttribute> lateFieldAttributes = new HashMap<>();
        Attribute docAttribute = null;
        for (PhysicalPlan subplan : subplans) {
            Tuple<PhysicalPlan, PhysicalPlan> coordinatorAndDataNodePlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(
                subplan,
                configuration
            );
            PhysicalPlan dataNodePhysicalPlan = coordinatorAndDataNodePlan.v2();
            if (dataNodePhysicalPlan instanceof ExchangeSinkExec == false) {
                return null;
            }
            ExchangeSinkExec dataNodePlan = (ExchangeSinkExec) dataNodePhysicalPlan;
            Map<String, OriginalIndices> clusterToOriginalIndices = getIndices(subplan, EsRelation::originalIndices);
            Map<String, OriginalIndices> clusterToConcreteIndices = getIndices(subplan, EsRelation::concreteIndices);
            OriginalIndices localOriginalIndices = clusterToOriginalIndices.remove(LOCAL_CLUSTER);
            OriginalIndices localConcreteIndices = clusterToConcreteIndices.remove(LOCAL_CLUSTER);
            ReductionPlan reductionPlan = maybePlanLocalSingleNodeLateMaterialization(
                flags,
                configuration,
                foldContext,
                dataNodePlan,
                localConcreteIndices,
                localOriginalIndices,
                clusterToConcreteIndices,
                clusterToOriginalIndices,
                planTimeProfile
            );
            if (reductionPlan == null) {
                return null;
            }
            Map<String, FieldAttribute> subplanLateFieldAttributes = parentFinalLateFieldAttributes(
                mainPlan,
                subplan,
                dataNodePlan,
                reductionPlan
            );
            reductionPlan = reductionPlanForParentFinal(dataNodePlan, reductionPlan, subplanLateFieldAttributes);
            if (docAttribute == null) {
                docAttribute = firstDocAttribute(reductionPlan.dataNodePlan().output());
            }
            lateFieldAttributes.putAll(subplanLateFieldAttributes);
            plannedSubplans.add(
                new ForkSubplanLateMaterializationPlan(
                    coordinatorAndDataNodePlan.v1(),
                    dataNodePlan,
                    reductionPlan,
                    localOriginalIndices,
                    localConcreteIndices,
                    subplan.output(),
                    subplanLateFieldAttributes
                )
            );
        }
        if (docAttribute == null || lateFieldAttributes.isEmpty()) {
            return null;
        }
        lateFieldAttributes.putAll(lateFieldAttributes(mainPlan));
        return new ForkLateMaterializationPlan(
            plannedSubplans,
            mainPlanWithLateMaterialization(mainPlan, docAttribute, lateFieldAttributes)
        );
    }

    private void executeForkLateMaterialization(
        String sessionId,
        CancellableTask rootTask,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldContext,
        EsqlExecutionInfo execInfo,
        ActionListener<Result> listener,
        ForkLateMaterializationPlan forkLateMaterializationPlan
    ) {
        final List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());
        PhysicalPlan mainPlan = new OutputExec(forkLateMaterializationPlan.mainPlan(), collectedPages::add);
        listener = listener.delegateResponse((l, e) -> {
            collectedPages.forEach(p -> Releasables.closeExpectNoException(p::releaseBlocks));
            l.onFailure(e);
        });

        AcquiredSearchContexts sharedSearchContexts = new AcquiredSearchContexts(0);
        listener = ActionListener.releaseAfter(listener, sharedSearchContexts);

        var mainSessionId = newChildSession(sessionId);
        QueryPragmas queryPragmas = configuration.pragmas();
        ExchangeSourceHandler mainExchangeSource = new ExchangeSourceHandler(
            queryPragmas.exchangeBufferSize(),
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
        );
        exchangeService.addExchangeSourceHandler(mainSessionId, mainExchangeSource);
        try (var ignored = mainExchangeSource.addEmptySink()) {
            var finalListener = ActionListener.runBefore(listener, () -> exchangeService.removeExchangeSourceHandler(mainSessionId));
            Runnable cancelQueryOnFailure = cancelQueryOnFailure(rootTask);
            try (
                ComputeListener localListener = new ComputeListener(
                    transportService.getThreadPool(),
                    cancelQueryOnFailure,
                    finalListener.map(profiles -> {
                        execInfo.markEndQuery();
                        return new Result(mainPlan.output(), collectedPages, configuration, profiles, execInfo);
                    })
                )
            ) {
                runCompute(
                    rootTask,
                    new ComputeContext(
                        mainSessionId,
                        "main.final",
                        LOCAL_CLUSTER,
                        flags,
                        sharedSearchContexts.globalView(),
                        configuration,
                        foldContext,
                        mainExchangeSource::createExchangeSource,
                        null
                    ),
                    mainPlan,
                    plannerSettings.get(),
                    LocalPhysicalOptimization.COORDINATOR_ONLY,
                    null,
                    localListener.acquireCompute()
                );
                for (int i = 0; i < forkLateMaterializationPlan.subplans().size(); i++) {
                    ForkSubplanLateMaterializationPlan subplan = forkLateMaterializationPlan.subplans().get(i);
                    var childSessionId = newChildSession(sessionId);
                    ExchangeSinkHandler exchangeSink = exchangeService.createSinkHandler(childSessionId, queryPragmas.exchangeBufferSize());
                    mainExchangeSource.addRemoteSink(exchangeSink::fetchPageAsync, true, () -> {}, 1, ActionListener.noop());
                    var subPlanListener = localListener.acquireCompute();
                    executeLocalSingleNodeLateMaterialization(
                        childSessionId,
                        rootTask,
                        flags,
                        configuration,
                        foldContext,
                        subplan.coordinatorPlan(),
                        subplan.dataNodePlan(),
                        subplan.reductionPlan(),
                        subplan.localOriginalIndices(),
                        subplan.localConcreteIndices(),
                        Collections.synchronizedList(new ArrayList<>()),
                        subplan.outputAttributes(),
                        execInfo,
                        "subplan-" + i,
                        cancelQueryOnFailure,
                        () -> exchangeSink.createExchangeSink(() -> {}),
                        configuration.profile() ? new PlanTimeProfile() : null,
                        ActionListener.wrap(result -> {
                            exchangeSink.addCompletionListener(
                                ActionListener.running(() -> { exchangeService.finishSinkHandler(childSessionId, null); })
                            );
                            subPlanListener.onResponse(result.completionInfo());
                        }, e -> {
                            exchangeService.finishSinkHandler(childSessionId, e);
                            subPlanListener.onFailure(e);
                        }),
                        sharedSearchContexts,
                        MaterializeTarget.PARENT_FINAL,
                        subplan.lateFieldAttributes()
                    );
                }
            }
        }
    }

    /**
     * Detect the MVP case where late materialization can move all the way into the coordinator's final driver because
     * the coordinator and data-side work stay on the same node and there is no transport boundary to cross.
     */
    private ReductionPlan maybePlanLocalSingleNodeLateMaterialization(
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldContext,
        ExchangeSinkExec dataNodePlan,
        OriginalIndices localConcreteIndices,
        OriginalIndices localOriginalIndices,
        Map<String, OriginalIndices> remoteConcreteIndices,
        Map<String, OriginalIndices> remoteOriginalIndices,
        PlanTimeProfile planTimeProfile
    ) {
        if (configuration.pragmas().nodeLevelReduction() == false
            || EsqlCapabilities.Cap.ENABLE_REDUCE_NODE_LATE_MATERIALIZATION.isEnabled() == false
            || localConcreteIndices == null
            || localConcreteIndices.indices().length == 0
            || localOriginalIndices == null
            || remoteConcreteIndices.isEmpty() == false
            || remoteOriginalIndices.isEmpty() == false
            || clusterService.state().nodes().getDataNodes().size() != 1) {
            return null;
        }
        long startTime = planTimeProfile == null ? 0L : System.nanoTime();
        ReductionPlan reductionPlan = reductionPlan(
            plannerSettings.get(),
            flags,
            configuration,
            foldContext,
            dataNodePlan,
            true,
            true,
            null
        );
        // The late-materialization planner preplans the reduce subtree and marks it as DISABLED so local planning
        // will preserve it exactly as-is. Any other shape falls back to the regular coordinator/data-node flow.
        if (reductionPlan.localPhysicalOptimization() != LocalPhysicalOptimization.DISABLED) {
            return null;
        }
        if (planTimeProfile != null) {
            planTimeProfile.addReductionPlanNanos(System.nanoTime() - startTime);
        }
        return reductionPlan;
    }

    /**
     * Run the single-node late-materialization flow:
     * - splice the old node-reduce TopN/field-loading subtree into final
     * - execute the data-side fragment locally
     * - share the acquired shard contexts between both sides
     */
    private void executeLocalSingleNodeLateMaterialization(
        String sessionId,
        CancellableTask rootTask,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldContext,
        PhysicalPlan coordinatorPlan,
        ExchangeSinkExec dataNodePlan,
        ReductionPlan reductionPlan,
        OriginalIndices localOriginalIndices,
        OriginalIndices localConcreteIndices,
        List<Page> collectedPages,
        List<Attribute> outputAttributes,
        EsqlExecutionInfo execInfo,
        String profileQualifier,
        Runnable cancelQueryOnFailure,
        Supplier<ExchangeSink> exchangeSinkSupplier,
        PlanTimeProfile planTimeProfile,
        ActionListener<Result> listener,
        AcquiredSearchContexts sharedSearchContexts,
        MaterializeTarget lateMaterializationTarget,
        Map<String, FieldAttribute> lateFieldAttributes
    ) {
        final LocalLateMaterializationPlan lateMaterializationPlan = coordinatorPlanWithLateMaterialization(
            coordinatorPlan,
            reductionPlan,
            lateMaterializationTarget,
            lateFieldAttributes
        );
        final QueryBuilder requestFilter = PlannerUtils.canMatchFilter(
            flags,
            configuration,
            clusterService.state().getMinTransportVersion(),
            dataNodePlan
        );
        searchLocalTargetShards(
            rootTask,
            localOriginalIndices,
            requestFilter,
            Set.of(localConcreteIndices.indices()),
            configuration,
            ActionListener.wrap(
                targetShards -> executeLocalSingleNodeLateMaterializationForTargetShards(
                    sessionId,
                    rootTask,
                    flags,
                    configuration,
                    foldContext,
                    lateMaterializationPlan.plan(),
                    lateMaterializationPlan.localPhysicalOptimization(),
                    reductionPlan,
                    localOriginalIndices,
                    collectedPages,
                    outputAttributes,
                    execInfo,
                    profileQualifier,
                    cancelQueryOnFailure,
                    exchangeSinkSupplier,
                    planTimeProfile,
                    listener,
                    targetShards,
                    sharedSearchContexts
                ),
                listener::onFailure
            )
        );
    }

    private void executeLocalSingleNodeLateMaterializationForTargetShards(
        String sessionId,
        CancellableTask rootTask,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldContext,
        PhysicalPlan finalCoordinatorPlan,
        LocalPhysicalOptimization finalLocalPhysicalOptimization,
        ReductionPlan reductionPlan,
        OriginalIndices localOriginalIndices,
        List<Page> collectedPages,
        List<Attribute> outputAttributes,
        EsqlExecutionInfo execInfo,
        String profileQualifier,
        Runnable cancelQueryOnFailure,
        Supplier<ExchangeSink> exchangeSinkSupplier,
        PlanTimeProfile planTimeProfile,
        ActionListener<Result> listener,
        DataNodeRequestSender.TargetShards targetShards,
        AcquiredSearchContexts sharedSearchContexts
    ) {
        // Subplans can already use `sessionId` for their outer sink into main.final. Give the local-only
        // data->final exchange its own child session so the in-process producer does not collide with that sink.
        final String localExchangeSessionId = exchangeSinkSupplier == null ? sessionId : newChildSession(sessionId);
        // final borrows the same search contexts that the local data-side execution will populate. This is
        // the key difference from the normal coordinator path, which gives final an EmptyIndexedByShardId.
        final AcquiredSearchContexts searchContexts = sharedSearchContexts == null
            ? new AcquiredSearchContexts(targetShards.shards().size())
            : sharedSearchContexts;
        ActionListener<Result> resultListener = sharedSearchContexts == null
            ? ActionListener.releaseAfter(
                ActionListener.runBefore(listener, () -> exchangeService.removeExchangeSourceHandler(localExchangeSessionId)),
                searchContexts
            )
            : ActionListener.runBefore(listener, () -> exchangeService.removeExchangeSourceHandler(localExchangeSessionId));
        final ExchangeSourceHandler exchangeSource = new ExchangeSourceHandler(
            configuration.pragmas().exchangeBufferSize(),
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
        );
        exchangeService.addExchangeSourceHandler(localExchangeSessionId, exchangeSource);
        try (
            // Owns the whole local-only branch and turns the collected pages into the final Result once all
            // local-cluster work has reported completion.
            var computeListener = new ComputeListener(
                transportService.getThreadPool(),
                cancelQueryOnFailure,
                resultListener.delegateFailureAndWrap((l, completionInfo) -> {
                    failIfAllShardsFailed(execInfo, collectedPages);
                    execInfo.markEndQuery();
                    l.onResponse(new Result(outputAttributes, collectedPages, configuration, completionInfo, execInfo));
                })
            )
        ) {
            // Keep final's exchange source open until we've either attached the in-process producer or
            // determined there are no shard-backed pages to send.
            try (Releasable ignored = exchangeSource.addEmptySink()) {
                // Filled in by the data-side callback and consulted when final decides the terminal cluster
                // status for the local cluster.
                final AtomicBoolean localClusterWasInterrupted = new AtomicBoolean();
                try (
                    // Tracks the local-cluster subcomputations specifically: final plus, when present, the
                    // in-process data-side fragment that feeds it.
                    var localListener = new ComputeListener(
                        transportService.getThreadPool(),
                        cancelQueryOnFailure,
                        computeListener.acquireCompute()
                            .delegateFailure(
                                (l, completionInfo) -> finalizeLocalSingleNodeClusterExecution(
                                    execInfo,
                                    localClusterWasInterrupted,
                                    completionInfo,
                                    l
                                )
                            )
                    )
                ) {
                    attachLocalSingleNodeProducerOrRecordNoShards(
                        localExchangeSessionId,
                        configuration,
                        targetShards,
                        exchangeSource,
                        execInfo
                    );
                    runLocalSingleNodeFinal(
                        localExchangeSessionId,
                        rootTask,
                        flags,
                        configuration,
                        foldContext,
                        finalCoordinatorPlan,
                        finalLocalPhysicalOptimization,
                        profileQualifier,
                        exchangeSource,
                        exchangeSinkSupplier,
                        planTimeProfile,
                        searchContexts,
                        localListener.acquireCompute()
                    );
                    if (targetShards.shards().isEmpty() == false) {
                        startLocalSingleNodeDataSide(
                            localExchangeSessionId,
                            rootTask,
                            flags,
                            configuration,
                            reductionPlan,
                            localOriginalIndices,
                            targetShards,
                            searchContexts,
                            execInfo,
                            localClusterWasInterrupted,
                            localListener.acquireCompute()
                        );
                    }
                }
            }
        }
    }

    private void finalizeLocalSingleNodeClusterExecution(
        EsqlExecutionInfo execInfo,
        AtomicBoolean localClusterWasInterrupted,
        DriverCompletionInfo completionInfo,
        ActionListener<DriverCompletionInfo> listener
    ) {
        if (execInfo.clusterInfo.containsKey(LOCAL_CLUSTER)) {
            execInfo.swapCluster(LOCAL_CLUSTER, (k, v) -> {
                var tookTime = execInfo.queryProfile().total().timeSinceStarted();
                var builder = new EsqlExecutionInfo.Cluster.Builder(v).setTook(tookTime);
                if (execInfo.isMainPlan() && v.getStatus() == EsqlExecutionInfo.Cluster.Status.RUNNING) {
                    final Integer failedShards = execInfo.getCluster(LOCAL_CLUSTER).getFailedShards();
                    var status = localClusterWasInterrupted.get()
                        || (failedShards != null && failedShards > 0)
                        || v.getFailures().isEmpty() == false
                            ? EsqlExecutionInfo.Cluster.Status.PARTIAL
                            : EsqlExecutionInfo.Cluster.Status.SUCCESSFUL;
                    builder.setStatus(status);
                }
                return builder.build();
            });
        }
        listener.onResponse(completionInfo);
    }

    private void attachLocalSingleNodeProducerOrRecordNoShards(
        String sessionId,
        Configuration configuration,
        DataNodeRequestSender.TargetShards targetShards,
        ExchangeSourceHandler exchangeSource,
        EsqlExecutionInfo execInfo
    ) {
        if (targetShards.shards().isEmpty() == false) {
            ExchangeSinkHandler localSinkHandler = exchangeService.createSinkHandler(
                sessionId,
                configuration.pragmas().exchangeBufferSize()
            );
            // Wire the local producer into final's exchange using the same sink/source API that
            // remote execution uses, but keep everything in-process.
            exchangeSource.addRemoteSink(localSinkHandler::fetchPageAsync, true, () -> {}, 1, ActionListener.noop());
            return;
        }
        // No shard-backed producers will run, so record the shard accounting up front.
        execInfo.swapCluster(
            LOCAL_CLUSTER,
            (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setTotalShards(targetShards.totalShards())
                .setSuccessfulShards(targetShards.totalShards() - targetShards.skippedShards())
                .setSkippedShards(targetShards.skippedShards())
                .setFailedShards(0)
                .build()
        );
    }

    private void runLocalSingleNodeFinal(
        String sessionId,
        CancellableTask rootTask,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldContext,
        PhysicalPlan finalCoordinatorPlan,
        LocalPhysicalOptimization finalLocalPhysicalOptimization,
        String profileQualifier,
        ExchangeSourceHandler exchangeSource,
        Supplier<ExchangeSink> exchangeSinkSupplier,
        PlanTimeProfile planTimeProfile,
        AcquiredSearchContexts searchContexts,
        ActionListener<DriverCompletionInfo> listener
    ) {
        // Start final first so it can block on the exchange and immediately consume pages once
        // the in-process data-side fragment starts producing them.
        runCompute(
            rootTask,
            new ComputeContext(
                sessionId,
                profileDescription(profileQualifier, "final"),
                LOCAL_CLUSTER,
                flags,
                searchContexts.globalView(),
                configuration,
                foldContext,
                exchangeSource::createExchangeSource,
                exchangeSinkSupplier
            ),
            finalCoordinatorPlan,
            plannerSettings.get(),
            finalLocalPhysicalOptimization,
            planTimeProfile,
            listener
        );
    }

    private void startLocalSingleNodeDataSide(
        String sessionId,
        CancellableTask rootTask,
        EsqlFlags flags,
        Configuration configuration,
        ReductionPlan reductionPlan,
        OriginalIndices localOriginalIndices,
        DataNodeRequestSender.TargetShards targetShards,
        AcquiredSearchContexts searchContexts,
        EsqlExecutionInfo execInfo,
        AtomicBoolean localClusterWasInterrupted,
        ActionListener<DriverCompletionInfo> listener
    ) {
        // The data-side fragment still runs separately, but entirely in-process. It emits the
        // _doc/sort-key pages that final now consumes to perform the global TopN and only then
        // load deferred fields.
        // This listener mirrors the normal data-node response accounting into the local
        // cluster bookkeeping before letting the shared local listener complete.
        dataNodeComputeHandler.startComputeOnLocalNode(
            sessionId,
            LOCAL_CLUSTER,
            rootTask,
            flags,
            configuration,
            reductionPlan.dataNodePlan(),
            localShards(targetShards),
            localAliasFilters(targetShards),
            localOriginalIndices,
            searchContexts,
            plannerSettings.get(),
            configuration.profile() ? new PlanTimeProfile() : null,
            ActionListener.wrap(r -> {
                ComputeResponse response = localComputeResponse(targetShards, r);
                localClusterWasInterrupted.set(execInfo.isStopped());
                execInfo.swapCluster(
                    LOCAL_CLUSTER,
                    (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setTotalShards(response.getTotalShards())
                        .setSuccessfulShards(response.getSuccessfulShards())
                        .setSkippedShards(response.getSkippedShards())
                        .setFailedShards(response.getFailedShards())
                        .addFailures(response.failures)
                        .build()
                );
                listener.onResponse(response.getCompletionInfo());
            }, e -> {
                if (configuration.allowPartialResults() && EsqlCCSUtils.canAllowPartial(e)) {
                    execInfo.swapCluster(
                        LOCAL_CLUSTER,
                        (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setStatus(EsqlExecutionInfo.Cluster.Status.PARTIAL)
                            .addFailures(List.of(new ShardSearchFailure(e)))
                            .build()
                    );
                    listener.onResponse(DriverCompletionInfo.EMPTY);
                } else {
                    listener.onFailure(e);
                }
            })
        );
    }

    private void searchLocalTargetShards(
        CancellableTask rootTask,
        OriginalIndices originalIndices,
        QueryBuilder requestFilter,
        Set<String> concreteIndices,
        Configuration configuration,
        ActionListener<DataNodeRequestSender.TargetShards> listener
    ) {
        new DataNodeRequestSender(
            clusterService,
            projectResolver,
            transportService,
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH),
            rootTask,
            originalIndices,
            requestFilter,
            LOCAL_CLUSTER,
            configuration.allowPartialResults(),
            -1,
            configuration.pragmas().unavailableShardResolutionAttempts()
        ) {
            @Override
            void sendRequest(
                DiscoveryNode node,
                List<DataNodeRequest.Shard> shards,
                Map<Index, AliasFilter> aliasFilters,
                NodeListener nodeListener
            ) {
                throw new UnsupportedOperationException("local late materialization planning should not send data node requests");
            }
        }.searchShards(concreteIndices, listener);
    }

    private static LocalLateMaterializationPlan coordinatorPlanWithLateMaterialization(
        PhysicalPlan coordinatorPlan,
        ReductionPlan reductionPlan,
        MaterializeTarget lateMaterializationTarget,
        Map<String, FieldAttribute> lateFieldAttributes
    ) {
        if (lateMaterializationTarget == MaterializeTarget.PARENT_FINAL) {
            return coordinatorPlanForParentFinal(coordinatorPlan, reductionPlan, lateFieldAttributes);
        }
        final boolean keepOriginalCoordinatorPipeline = coordinatorPlan.collect(TopNExec.class::isInstance).size() > 1;
        Attribute docAttribute = firstDocAttribute(reductionPlan.dataNodePlan().output());
        final AtomicBoolean replaced = new AtomicBoolean();
        PhysicalPlan updatedPlan = coordinatorPlan.transformUp(ExchangeSourceExec.class, exchangeSource -> {
            if (replaced.compareAndSet(false, true)) {
                if (keepOriginalCoordinatorPipeline == false) {
                    // The original single-TopN path already materializes at the latest useful point. Keep splicing
                    // the preplanned reduce subtree into final for that established behavior.
                    return reductionPlan.nodeReducePlan().child();
                }
                // Keep the original coordinator pipeline, but feed it the reduced data-side shape (_doc plus
                // ordering/ranking fields) instead of the eagerly materialized exchange rows. final's local
                // optimizer can then insert field extraction at the latest use site while _doc is still available.
                ExchangeSourceExec passthroughSource = new ExchangeSourceExec(
                    exchangeSource.source(),
                    reductionPlan.dataNodePlan().output(),
                    exchangeSource.isIntermediateAgg()
                );
                return docAttribute == null
                    ? passthroughSource
                    : withMaterializeBoundary(passthroughSource, docAttribute, lateFieldAttributes, MaterializeTarget.CURRENT_FINAL);
            }
            return exchangeSource;
        });
        if (replaced.get() == false) {
            throw new IllegalStateException("expected coordinator plan to contain an exchange source");
        }
        return new LocalLateMaterializationPlan(
            updatedPlan,
            keepOriginalCoordinatorPipeline ? LocalPhysicalOptimization.COORDINATOR_ONLY : LocalPhysicalOptimization.ENABLED
        );
    }

    private static LocalLateMaterializationPlan coordinatorPlanForParentFinal(
        PhysicalPlan coordinatorPlan,
        ReductionPlan reductionPlan,
        Map<String, FieldAttribute> lateFieldAttributes
    ) {
        Attribute docAttribute = firstDocAttribute(reductionPlan.dataNodePlan().output());
        final AtomicBoolean replaced = new AtomicBoolean();
        PhysicalPlan updatedPlan = coordinatorPlan.transformUp(ExchangeSourceExec.class, exchangeSource -> {
            if (replaced.compareAndSet(false, true)) {
                List<Attribute> passthroughOutput = exchangeSource.output()
                    .stream()
                    .filter(attr -> lateFieldAttributes.containsKey(attr.name()) == false)
                    .collect(Collectors.toCollection(ArrayList::new));
                if (docAttribute != null) {
                    passthroughOutput.add(0, docAttribute);
                }
                ExchangeSourceExec passthroughSource = new ExchangeSourceExec(
                    exchangeSource.source(),
                    passthroughOutput,
                    exchangeSource.isIntermediateAgg()
                );
                return docAttribute == null
                    ? passthroughSource
                    : withMaterializeBoundary(passthroughSource, docAttribute, lateFieldAttributes, MaterializeTarget.PARENT_FINAL);
            }
            return exchangeSource;
        });
        if (replaced.get() == false) {
            throw new IllegalStateException("expected coordinator plan to contain an exchange source");
        }
        if (updatedPlan instanceof ExchangeSinkExec sink) {
            PhysicalPlan sinkChild = sink.child();
            if (sinkChild instanceof ProjectExec project) {
                sinkChild = project.child();
            }
            updatedPlan = sink.replaceChildAndUpdateOutput(sinkChild);
        }
        return new LocalLateMaterializationPlan(updatedPlan, LocalPhysicalOptimization.ENABLED);
    }

    private static ReductionPlan reductionPlanForParentFinal(
        ExchangeSinkExec originalDataNodePlan,
        ReductionPlan reductionPlan,
        Map<String, FieldAttribute> lateFieldAttributes
    ) {
        Attribute docAttribute = firstDocAttribute(reductionPlan.dataNodePlan().output());
        if (docAttribute == null) {
            return reductionPlan;
        }
        PhysicalPlan dataNodeChild = originalDataNodePlan.child();
        if (dataNodeChild instanceof FragmentExec == false) {
            return reductionPlan;
        }
        FragmentExec fragmentExec = (FragmentExec) dataNodeChild;
        LogicalPlan fragment = fragmentExec.fragment();
        if (fragment instanceof org.elasticsearch.xpack.esql.plan.logical.Project == false) {
            return reductionPlan;
        }
        org.elasticsearch.xpack.esql.plan.logical.Project project = (org.elasticsearch.xpack.esql.plan.logical.Project) fragment;

        List<NamedExpression> passthroughProjections = project.projections()
            .stream()
            .filter(projection -> lateFieldAttributes.containsKey(projection.name()) == false)
            .map(NamedExpression.class::cast)
            .collect(Collectors.toCollection(ArrayList::new));
        passthroughProjections.add(0, docAttribute);

        LogicalPlan withDocToRelation = project.child().transformUp(EsRelation.class, relation -> {
            if (relation.indexMode() == IndexMode.LOOKUP) {
                return relation;
            }
            return relation.withAttributes(CollectionUtils.prependToCopy(docAttribute, relation.output()));
        });
        FragmentExec updatedFragment = fragmentExec.withFragment(
            new org.elasticsearch.xpack.esql.plan.logical.Project(project.source(), withDocToRelation, passthroughProjections)
        );
        ExchangeSinkExec updatedDataNodePlan = originalDataNodePlan.replaceChildAndUpdateOutput(updatedFragment);
        return new ReductionPlan(reductionPlan.nodeReducePlan(), updatedDataNodePlan, reductionPlan.localPhysicalOptimization());
    }

    private record LocalLateMaterializationPlan(PhysicalPlan plan, LocalPhysicalOptimization localPhysicalOptimization) {}

    private record ForkLateMaterializationPlan(List<ForkSubplanLateMaterializationPlan> subplans, PhysicalPlan mainPlan) {}

    private record ForkSubplanLateMaterializationPlan(
        PhysicalPlan coordinatorPlan,
        ExchangeSinkExec dataNodePlan,
        ReductionPlan reductionPlan,
        OriginalIndices localOriginalIndices,
        OriginalIndices localConcreteIndices,
        List<Attribute> outputAttributes,
        Map<String, FieldAttribute> lateFieldAttributes
    ) {}

    private static PhysicalPlan mainPlanWithLateMaterialization(
        PhysicalPlan mainPlan,
        Attribute docAttribute,
        Map<String, FieldAttribute> lateFieldAttributes
    ) {
        return mainPlan.transformExpressionsDown(Expression.class, expression -> {
            if (expression instanceof Attribute attribute) {
                FieldAttribute lateField = lateFieldAttributes.get(attribute.name());
                if (lateField != null) {
                    return lateField.withId(attribute.id());
                }
            }
            return expression;
        }).transformUp(ExchangeSourceExec.class, exchangeSource -> {
            List<Attribute> passthroughOutput = exchangeSource.output()
                .stream()
                .filter(attr -> lateFieldAttributes.containsKey(attr.name()) == false)
                .collect(Collectors.toCollection(ArrayList::new));
            passthroughOutput.add(0, docAttribute);
            ExchangeSourceExec passthroughSource = new ExchangeSourceExec(
                exchangeSource.source(),
                passthroughOutput,
                exchangeSource.isIntermediateAgg()
            );
            return withMaterializeBoundary(passthroughSource, docAttribute, lateFieldAttributes, MaterializeTarget.CURRENT_FINAL);
        });
    }

    private static Map<String, FieldAttribute> lateFieldAttributes(ReductionPlan reductionPlan) {
        return lateFieldAttributes(reductionPlan.nodeReducePlan());
    }

    private static Map<String, FieldAttribute> lateFieldAttributes(PhysicalPlan plan) {
        return plan.collect(node -> node instanceof org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec)
            .stream()
            .map(org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec.class::cast)
            .flatMap(fieldExtract -> fieldExtract.attributesToExtract().stream())
            .filter(FieldAttribute.class::isInstance)
            .map(FieldAttribute.class::cast)
            .collect(Collectors.toMap(FieldAttribute::name, field -> field, (left, right) -> left));
    }

    private static PhysicalPlan withMaterializeBoundary(
        ExchangeSourceExec exchangeSource,
        Attribute docAttribute,
        Map<String, FieldAttribute> lateFieldAttributes,
        MaterializeTarget target
    ) {
        if (lateFieldAttributes.isEmpty()) {
            return exchangeSource;
        }
        List<Attribute> deferredAttributes = lateFieldAttributes.values()
            .stream()
            .sorted(java.util.Comparator.comparing(Attribute::name))
            .map(Attribute.class::cast)
            .toList();
        return MaterializeExec.local(exchangeSource.source(), exchangeSource, docAttribute, deferredAttributes, target);
    }

    private static Map<String, FieldAttribute> parentFinalLateFieldAttributes(
        PhysicalPlan mainPlan,
        PhysicalPlan subplan,
        ExchangeSinkExec originalDataNodePlan,
        ReductionPlan reductionPlan
    ) {
        Map<String, FieldAttribute> lateFieldAttributes = new HashMap<>(lateFieldAttributes(subplan));
        lateFieldAttributes.putAll(lateFieldAttributes(originalDataNodePlan, reductionPlan));
        lateFieldAttributes.putAll(lateFieldAttributes(originalDataNodePlan, mainPlan));
        lateFieldAttributes.putAll(lateFieldAttributes(mainPlan));
        return lateFieldAttributes;
    }

    private static Map<String, FieldAttribute> lateFieldAttributes(ExchangeSinkExec originalDataNodePlan, PhysicalPlan parentPlan) {
        Set<String> parentAttributeNames = new HashSet<>();
        parentPlan.forEachExpressionDown(Attribute.class, attribute -> parentAttributeNames.add(attribute.name()));
        Map<String, FieldAttribute> lateFieldAttributes = new HashMap<>();
        PhysicalPlan dataNodeChild = originalDataNodePlan.child();
        if (dataNodeChild instanceof FragmentExec fragmentExec
            && fragmentExec.fragment() instanceof org.elasticsearch.xpack.esql.plan.logical.Project project) {
            Map<String, FieldAttribute> fragmentFieldAttributes = new HashMap<>();
            fragmentExec.fragment()
                .forEachExpressionDown(FieldAttribute.class, field -> fragmentFieldAttributes.putIfAbsent(field.name(), field));
            fragmentExec.fragment()
                .collect(EsRelation.class)
                .forEach(
                    relation -> relation.output()
                        .stream()
                        .filter(FieldAttribute.class::isInstance)
                        .map(FieldAttribute.class::cast)
                        .forEach(field -> fragmentFieldAttributes.putIfAbsent(field.name(), field))
                );
            for (NamedExpression projection : project.projections()) {
                if (parentAttributeNames.contains(projection.name()) == false) {
                    continue;
                }
                FieldAttribute fieldAttribute = null;
                if (projection instanceof FieldAttribute field) {
                    fieldAttribute = field;
                } else if (projection instanceof Alias alias && alias.child() instanceof FieldAttribute field) {
                    fieldAttribute = (FieldAttribute) field.withName(alias.name()).withId(alias.id());
                }
                if (fieldAttribute == null) {
                    fieldAttribute = fragmentFieldAttributes.get(projection.name());
                }
                if (fieldAttribute != null) {
                    lateFieldAttributes.putIfAbsent(projection.name(), fieldAttribute);
                }
            }
        }
        return lateFieldAttributes;
    }

    private static Map<String, FieldAttribute> lateFieldAttributes(ExchangeSinkExec originalDataNodePlan, ReductionPlan reductionPlan) {
        Map<String, FieldAttribute> lateFieldAttributes = new HashMap<>(lateFieldAttributes(reductionPlan));
        Set<String> passthroughNames = reductionPlan.dataNodePlan().output().stream().map(Attribute::name).collect(Collectors.toSet());
        PhysicalPlan dataNodeChild = originalDataNodePlan.child();
        if (dataNodeChild instanceof FragmentExec fragmentExec
            && fragmentExec.fragment() instanceof org.elasticsearch.xpack.esql.plan.logical.Project project) {
            Map<String, FieldAttribute> fragmentFieldAttributes = new HashMap<>();
            fragmentExec.fragment()
                .forEachExpressionDown(FieldAttribute.class, field -> fragmentFieldAttributes.putIfAbsent(field.name(), field));
            fragmentExec.fragment()
                .collect(EsRelation.class)
                .forEach(
                    relation -> relation.output()
                        .stream()
                        .filter(FieldAttribute.class::isInstance)
                        .map(FieldAttribute.class::cast)
                        .forEach(field -> fragmentFieldAttributes.putIfAbsent(field.name(), field))
                );
            for (NamedExpression projection : project.projections()) {
                if (passthroughNames.contains(projection.name())) {
                    continue;
                }
                FieldAttribute fieldAttribute = null;
                if (projection instanceof FieldAttribute field) {
                    fieldAttribute = field;
                } else if (projection instanceof Alias alias && alias.child() instanceof FieldAttribute field) {
                    fieldAttribute = (FieldAttribute) field.withName(alias.name()).withId(alias.id());
                }
                if (fieldAttribute == null) {
                    fieldAttribute = fragmentFieldAttributes.get(projection.name());
                }
                if (fieldAttribute != null) {
                    lateFieldAttributes.putIfAbsent(projection.name(), fieldAttribute);
                }
            }
        }
        return lateFieldAttributes;
    }

    private static Attribute firstDocAttribute(List<Attribute> output) {
        return output.stream().filter(org.elasticsearch.xpack.esql.plan.physical.EsQueryExec::isDocAttribute).findFirst().orElse(null);
    }

    private static List<DataNodeRequest.Shard> localShards(DataNodeRequestSender.TargetShards targetShards) {
        return targetShards.shards()
            .values()
            .stream()
            .map(shard -> new DataNodeRequest.Shard(shard.shardId(), shard.reshardSplitShardCountSummary()))
            .toList();
    }

    private static Map<Index, AliasFilter> localAliasFilters(DataNodeRequestSender.TargetShards targetShards) {
        Map<Index, AliasFilter> aliasFilters = new HashMap<>();
        for (DataNodeRequestSender.TargetShard shard : targetShards.shards().values()) {
            if (shard.aliasFilter() != null) {
                aliasFilters.put(shard.shardId().getIndex(), shard.aliasFilter());
            }
        }
        return aliasFilters;
    }

    private static ComputeResponse localComputeResponse(DataNodeRequestSender.TargetShards targetShards, DataNodeComputeResponse response) {
        int failedShards = response.shardLevelFailures().size();
        int skippedShards = targetShards.skippedShards();
        int totalShards = targetShards.totalShards();
        return new ComputeResponse(
            response.completionInfo(),
            null,
            totalShards,
            totalShards - skippedShards - failedShards,
            skippedShards,
            failedShards,
            localShardFailures(response.shardLevelFailures())
        );
    }

    private static List<ShardSearchFailure> localShardFailures(Map<org.elasticsearch.index.shard.ShardId, Exception> shardLevelFailures) {
        List<ShardSearchFailure> failures = new ArrayList<>();
        for (var entry : shardLevelFailures.entrySet()) {
            if (ExceptionsHelper.unwrap(entry.getValue(), TaskCancelledException.class) != null) {
                continue;
            }
            failures.add(new ShardSearchFailure(entry.getValue(), new SearchShardTarget(null, entry.getKey(), LOCAL_CLUSTER)));
            if (failures.size() == 5) {
                break;
            }
        }
        if (failures.isEmpty() && shardLevelFailures.isEmpty() == false) {
            var entry = shardLevelFailures.entrySet().iterator().next();
            failures.add(new ShardSearchFailure(entry.getValue(), new SearchShardTarget(null, entry.getKey(), LOCAL_CLUSTER)));
        }
        return failures;
    }

    // For queries like: FROM logs* | LIMIT 0 (including cross-cluster LIMIT 0 queries)
    private static void updateShardCountForCoordinatorOnlyQuery(EsqlExecutionInfo execInfo) {
        if (execInfo.isCrossClusterSearch() || execInfo.includeExecutionMetadata() == ALWAYS) {
            for (String clusterAlias : execInfo.clusterAliases()) {
                execInfo.swapCluster(
                    clusterAlias,
                    (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setTotalShards(0)
                        .setSuccessfulShards(0)
                        .setSkippedShards(0)
                        .setFailedShards(0)
                        .build()
                );
            }
        }
    }

    // For queries like: FROM logs* | LIMIT 0 (including cross-cluster LIMIT 0 queries)
    private static void updateExecutionInfoAfterCoordinatorOnlyQuery(EsqlExecutionInfo execInfo) {
        execInfo.markEndQuery();
        if ((execInfo.isCrossClusterSearch() || execInfo.includeExecutionMetadata() == ALWAYS) && execInfo.isMainPlan()) {
            assert execInfo.queryProfile().planning().timeTook() != null
                : "Planning took time should be set on EsqlExecutionInfo but is null";
            for (String clusterAlias : execInfo.clusterAliases()) {
                execInfo.swapCluster(clusterAlias, (k, v) -> {
                    var builder = new EsqlExecutionInfo.Cluster.Builder(v).setTook(execInfo.overallTook());
                    if (v.getStatus() == EsqlExecutionInfo.Cluster.Status.RUNNING) {
                        builder.setStatus(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
                    }
                    return builder.build();
                });
            }
        }
    }

    /**
     * If all of target shards excluding the skipped shards failed from the local or remote clusters, then we should fail the entire query
     * regardless of the partial_results configuration or skip_unavailable setting. This behavior doesn't fully align with the search API,
     * which doesn't consider the failures from the remote clusters when skip_unavailable is true.
     */
    static void failIfAllShardsFailed(EsqlExecutionInfo execInfo, List<Page> finalResults) {
        // do not fail if any final result has results
        for (Page p : finalResults) {
            if (p.getPositionCount() > 0) {
                return;
            }
        }
        int totalFailedShards = 0;
        for (EsqlExecutionInfo.Cluster cluster : execInfo.clusterInfo.values()) {
            final Integer successfulShards = cluster.getSuccessfulShards();
            if (successfulShards != null && successfulShards > 0) {
                return;
            }
            if (cluster.getFailedShards() != null) {
                totalFailedShards += cluster.getFailedShards();
            }
        }
        if (totalFailedShards == 0) {
            return;
        }
        final var failureCollector = new FailureCollector();
        for (EsqlExecutionInfo.Cluster cluster : execInfo.clusterInfo.values()) {
            var failedShards = cluster.getFailedShards();
            if (failedShards != null && failedShards > 0) {
                assert cluster.getFailures().isEmpty() == false : "expected failures for cluster [" + cluster.getClusterAlias() + "]";
                for (ShardSearchFailure failure : cluster.getFailures()) {
                    if (failure.getCause() instanceof Exception e) {
                        failureCollector.unwrapAndCollect(e);
                    } else {
                        assert false : "unexpected failure: " + new AssertionError(failure.getCause());
                        failureCollector.unwrapAndCollect(failure);
                    }
                }
            }
        }
        ExceptionsHelper.reThrowIfNotNull(failureCollector.getFailure());
    }

    void runCompute(
        CancellableTask task,
        ComputeContext context,
        PhysicalPlan plan,
        PlannerSettings plannerSettings,
        LocalPhysicalOptimization localPhysicalOptimization,
        PlanTimeProfile planTimeProfile,
        ActionListener<DriverCompletionInfo> listener
    ) {
        runCompute(task, context, plan, plannerSettings, localPhysicalOptimization, List.of(), planTimeProfile, listener);
    }

    void runCompute(
        CancellableTask task,
        ComputeContext context,
        PhysicalPlan plan,
        PlannerSettings plannerSettings,
        LocalPhysicalOptimization localPhysicalOptimization,
        List<ExternalSplit> coordinatorExternalSplits,
        PlanTimeProfile planTimeProfile,
        ActionListener<DriverCompletionInfo> listener
    ) {
        var shardContexts = context.searchContexts().map(ComputeSearchContext::shardContext);
        EsPhysicalOperationProviders physicalOperationProviders = new EsPhysicalOperationProviders(
            context.foldCtx(),
            shardContexts,
            searchService.getIndicesService().getAnalysis(),
            plannerSettings
        );

        try {
            LocalExecutionPlanner planner = new LocalExecutionPlanner(
                context.sessionId(),
                context.clusterAlias(),
                task,
                bigArrays,
                blockFactory,
                clusterService.getSettings(),
                context.configuration(),
                context.exchangeSourceSupplier(),
                context.exchangeSinkSupplier(),
                enrichLookupService,
                lookupFromIndexService,
                inferenceService,
                physicalOperationProviders,
                operatorFactoryRegistry
            );

            LOGGER.debug("Received physical plan for {}:\n{}", context.description(), plan);

            List<SearchExecutionContext> localContexts = new ArrayList<>();
            context.searchExecutionContexts().iterable().forEach(localContexts::add);
            boolean hasExternalSource = plan.anyMatch(
                p -> p instanceof ExternalSourceExec
                    || (p instanceof FragmentExec f && f.fragment().anyMatch(ExternalRelation.class::isInstance))
            );
            var localPlan = switch (localPhysicalOptimization) {
                case ENABLED -> hasExternalSource
                    ? PlannerUtils.localPlan(
                        plannerSettings,
                        context.flags(),
                        context.configuration(),
                        context.foldCtx(),
                        plan,
                        SearchContextStats.from(localContexts),
                        filterPushdownRegistry,
                        planTimeProfile
                    )
                    : PlannerUtils.localPlan(
                        plannerSettings,
                        context.flags(),
                        localContexts,
                        context.configuration(),
                        context.foldCtx(),
                        plan,
                        planTimeProfile
                    );
                case COORDINATOR_ONLY -> PlannerUtils.localCoordinatorPlan(
                    plannerSettings,
                    context.flags(),
                    localContexts,
                    context.configuration(),
                    context.foldCtx(),
                    plan,
                    planTimeProfile
                );
                case DISABLED -> plan;
            };
            if (coordinatorExternalSplits.isEmpty() == false) {
                localPlan = localPlan.transformUp(
                    ExternalSourceExec.class,
                    exec -> exec.splits().isEmpty() ? exec.withSplits(coordinatorExternalSplits) : exec
                );
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Local plan for {}:\n{}", context.description(), localPlan);
            }
            // the planner will also set the driver parallelism in LocalExecutionPlanner.LocalExecutionPlan (used down below)
            // it's doing this in the planning of EsQueryExec (the source of the data)
            // see also EsPhysicalOperationProviders.sourcePhysicalOperation
            var localExecutionPlan = planner.plan(context.description(), context.foldCtx(), plannerSettings, localPlan, shardContexts);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Local execution plan for {}:\n{}", context.description(), localExecutionPlan.describe());
            }
            String driverSessionId = new TaskId(clusterService.localNode().getId(), task.getId()).toString();
            var drivers = localExecutionPlan.createDrivers(driverSessionId);
            // Note that the drivers themselves do not hold a reference to the search contexts, but rather, these are held (and therefore
            // incremented) by the source operators, and the DocVectors. Since The contexts are pre-created with a count of 1, and then
            // incremented by the relevant source operators, after creating the *data* drivers (and therefore, the source operators), we can
            // safely decrement the reference count so only the source operators and doc vectors control when these will be released.
            // Note that only the data drivers will increment the reference count when created, hence the if below.
            if (context.description().equals(DATA_DESCRIPTION)) {
                shardContexts.iterable().forEach(RefCounted::decRef);
            }
            if (drivers.isEmpty()) {
                throw new IllegalStateException("no drivers created");
            }
            LOGGER.debug("using {} drivers", drivers.size());
            ActionListener<Void> driverListener = addCompletionInfo(listener, drivers, context, localPlan, planTimeProfile);
            driverRunner.executeDrivers(
                task,
                drivers,
                transportService.getThreadPool().executor(ESQL_WORKER_THREAD_POOL_NAME),
                ActionListener.releaseAfter(driverListener, () -> Releasables.close(drivers))
            );
        } catch (Exception e) {
            Releasables.close(context.searchContexts().iterable());
            LOGGER.debug("Error in ComputeService.runCompute for : " + context.description());
            listener.onFailure(e);
        }
    }

    ActionListener<Void> addCompletionInfo(
        ActionListener<DriverCompletionInfo> listener,
        List<Driver> drivers,
        ComputeContext context,
        PhysicalPlan localPlan,
        PlanTimeProfile planTimeProfile
    ) {
        /*
         * We *really* don't want to close over the localPlan because it can
         * be quite large, and it isn't tracked.
         */
        boolean needPlanString = LOGGER.isDebugEnabled() || context.configuration().profile();
        String planString = needPlanString ? localPlan.toString() : null;
        return listener.map(ignored -> {
            if (LOGGER.isDebugEnabled() || context.configuration().profile()) {
                DriverCompletionInfo driverCompletionInfo = DriverCompletionInfo.includingProfiles(
                    drivers,
                    context.description(),
                    clusterService.getClusterName().value(),
                    transportService.getLocalNode().getName(),
                    planString,
                    planTimeProfile
                );
                LOGGER.debug("finished {}", driverCompletionInfo);
                if (context.configuration().profile()) {
                    /*
                     * planString *might* be null if we *just* set DEBUG to *after*
                     * we built the listener but before we got here. That's something
                     * we can live with.
                     */
                    return driverCompletionInfo;
                }
            }

            return DriverCompletionInfo.excludingProfiles(drivers);
        });
    }

    // public for testing
    public static ReductionPlan reductionPlan(
        PlannerSettings plannerSettings,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldCtx,
        ExchangeSinkExec originalPlan,
        boolean runNodeLevelReduction,
        boolean reduceNodeLateMaterialization,
        PlanTimeProfile planTimeProfile
    ) {
        long startTime = planTimeProfile == null ? 0 : System.nanoTime();
        PhysicalPlan source = new ExchangeSourceExec(originalPlan.source(), originalPlan.output(), originalPlan.isIntermediateAgg());
        // Just send out everything through a single exchange as a fallback
        ReductionPlan passThroughReduction = new ReductionPlan(
            originalPlan.replaceChild(source),
            originalPlan,
            LocalPhysicalOptimization.ENABLED
        );
        if (reduceNodeLateMaterialization == false && runNodeLevelReduction == false) {
            return passThroughReduction;
        }

        Function<PhysicalPlan, ReductionPlan> placePlanBetweenExchanges = p -> new ReductionPlan(
            originalPlan.replaceChild(p.replaceChildren(List.of(source))),
            originalPlan,
            LocalPhysicalOptimization.ENABLED
        );

        // The default plan is just the exchange source piped directly into the exchange sink.
        ReductionPlan reductionPlan = switch (PlannerUtils.reductionPlan(originalPlan)) {
            case PlannerUtils.TopNReduction topN when reduceNodeLateMaterialization ->
                // In the case of TopN, the source output type is replaced since we're pulling the FieldExtractExec to the reduction node,
                // so essentially we are splitting the TopNExec into two parts, similar to other aggregations, but unlike other
                // aggregations, we also need the original plan, since we add the project in the reduction node.
                LateMaterializationPlanner.planReduceDriverTopN(
                    stats -> new LocalPhysicalOptimizerContext(plannerSettings, flags, configuration, foldCtx, stats),
                    originalPlan
                )
                    // Fallback to the behavior listed below, i.e., a regular top n reduction without loading new fields.
                    .orElseGet(() -> runNodeLevelReduction ? placePlanBetweenExchanges.apply(topN.plan()) : passThroughReduction);
            case PlannerUtils.TopNReduction topN when runNodeLevelReduction -> placePlanBetweenExchanges.apply(topN.plan());
            // Not a TopN - must be an agg or a limit
            case PlannerUtils.ReducedPlan rp when runNodeLevelReduction -> placePlanBetweenExchanges.apply(rp.plan());
            default -> passThroughReduction;
        };
        if (planTimeProfile != null) {
            planTimeProfile.addReductionPlanNanos(System.nanoTime() - startTime);
        }

        // TODO: How we generate intermediate attributes prevents us from cleanly checking dependencies here. We should always be
        // able to perform this check.
        if (Assertions.ENABLED == false
            || (reductionPlan.dataNodePlan().child() instanceof FragmentExec fragment
                && skipConsistencyCheckAfterReductionPlanning(fragment.fragment()))) {
            return reductionPlan;
        }

        PhysicalVerifier.LOCAL_INSTANCE.verify(reductionPlan.nodeReducePlan(), originalPlan.output());
        ExchangeSourceExec reductionSource = (ExchangeSourceExec) reductionPlan.nodeReducePlan().collectLeaves().getFirst();
        // The data driver's output is sent to the reduction driver, so the outputs must match up.
        PhysicalVerifier.LOCAL_INSTANCE.verify(reductionPlan.dataNodePlan(), reductionSource.output());

        return reductionPlan;
    }

    private static boolean skipConsistencyCheckAfterReductionPlanning(LogicalPlan fragment) {
        // FragmentExec.output() doesn't take into account intermediate attributes of aggs, and time series aggs
        // have some peculiarities due to implicit dimensions. We should clean this up and add a proper check here.
        return fragment instanceof Aggregate
            // MetricsInfo/TsInfo do not serialize their output attributes (they are generated automatically and do not depend on the
            // input). After de-serializing the data node plan, the output attributes have different NameIds than the ExchangeSink of
            // the data node plan.
            || fragment instanceof MetricsInfo
            || fragment instanceof TsInfo;
    }

    String newChildSession(String session) {
        return session + "/" + childSessionIdGenerator.incrementAndGet();
    }

    String profileDescription(String qualifier, String label) {
        return qualifier == null ? label : qualifier + "." + label;
    }

    Runnable cancelQueryOnFailure(CancellableTask task) {
        return new RunOnce(() -> {
            LOGGER.debug("cancelling ESQL task {} on failure", task);
            transportService.getTaskManager().cancelTaskAndDescendants(task, "cancelled on failure", false, ActionListener.noop());
        });
    }

    CancellableTask createGroupTask(Task parentTask, Supplier<String> description) throws TaskCancelledException {
        final TaskManager taskManager = transportService.getTaskManager();
        try (var ignored = transportService.getThreadPool().getThreadContext().newTraceContext()) {
            return (CancellableTask) taskManager.register(
                "transport",
                "esql_compute_group",
                new ComputeGroupTaskRequest(parentTask.taskInfo(transportService.getLocalNode().getId(), false).taskId(), description)
            );
        }
    }

    public EsqlFlags createFlags() {
        return new EsqlFlags(clusterService.getClusterSettings());
    }

    private static class ComputeGroupTaskRequest extends AbstractTransportRequest {
        private final Supplier<String> parentDescription;

        ComputeGroupTaskRequest(TaskId parentTask, Supplier<String> description) {
            this.parentDescription = description;
            setParentTask(parentTask);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            assert parentTaskId.isSet();
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public String getDescription() {
            return "group [" + parentDescription.get() + "]";
        }
    }

    private static Map<String, OriginalIndices> getIndices(PhysicalPlan plan, Function<EsRelation, Map<String, List<String>>> getter) {
        var holder = new Holder<Map<String, OriginalIndices>>();
        PlannerUtils.forEachRelation(plan, esRelation -> {
            holder.set(Maps.transformValues(getter.apply(esRelation), v -> {
                return new OriginalIndices(v.toArray(String[]::new), SearchRequest.DEFAULT_INDICES_OPTIONS);
            }));
        });
        return holder.getOrDefault(Map::of);
    }
}
