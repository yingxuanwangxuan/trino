/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.log.Logger;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.exchange.ExchangeInput;
import io.trino.exchange.SpoolingExchangeInput;
import io.trino.execution.BasicStageStats;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.QueryState;
import io.trino.execution.QueryStateMachine;
import io.trino.execution.RemoteTaskFactory;
import io.trino.execution.SqlStage;
import io.trino.execution.StageId;
import io.trino.execution.StageInfo;
import io.trino.execution.TaskId;
import io.trino.failuredetector.FailureDetector;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Metadata;
import io.trino.operator.RetryPolicy;
import io.trino.server.DynamicFilterService;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.plan.PlanFragmentId;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Ticker.systemTicker;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.reverse;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionPartitionCount;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.execution.QueryState.FINISHING;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class FaultTolerantQueryScheduler
        implements QueryScheduler
{
    private static final Logger log = Logger.get(FaultTolerantQueryScheduler.class);

    private final QueryStateMachine queryStateMachine;
    private final ExecutorService queryExecutor;
    private final SplitSchedulerStats schedulerStats;
    private final FailureDetector failureDetector;
    private final TaskSourceFactory taskSourceFactory;
    private final TaskDescriptorStorage taskDescriptorStorage;
    private final ExchangeManager exchangeManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final int taskRetryAttemptsOverall;
    private final int taskRetryAttemptsPerTask;
    private final int maxTasksWaitingForNodePerStage;
    private final ScheduledExecutorService scheduledExecutorService;
    private final NodeAllocatorService nodeAllocatorService;
    private final PartitionMemoryEstimatorFactory partitionMemoryEstimatorFactory;
    private final TaskExecutionStats taskExecutionStats;
    private final DynamicFilterService dynamicFilterService;

    private final StageManager stageManager;

    @GuardedBy("this")
    private boolean started;
    @GuardedBy("this")
    private Scheduler scheduler;

    public FaultTolerantQueryScheduler(
            QueryStateMachine queryStateMachine,
            ExecutorService queryExecutor,
            SplitSchedulerStats schedulerStats,
            FailureDetector failureDetector,
            TaskSourceFactory taskSourceFactory,
            TaskDescriptorStorage taskDescriptorStorage,
            ExchangeManager exchangeManager,
            NodePartitioningManager nodePartitioningManager,
            int taskRetryAttemptsOverall,
            int taskRetryAttemptsPerTask,
            int maxTasksWaitingForNodePerStage,
            ScheduledExecutorService scheduledExecutorService,
            NodeAllocatorService nodeAllocatorService,
            PartitionMemoryEstimatorFactory partitionMemoryEstimatorFactory,
            TaskExecutionStats taskExecutionStats,
            DynamicFilterService dynamicFilterService,
            Metadata metadata,
            RemoteTaskFactory taskFactory,
            NodeTaskMap nodeTaskMap,
            SubPlan planTree,
            boolean summarizeTaskInfo)
    {
        this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        RetryPolicy retryPolicy = getRetryPolicy(queryStateMachine.getSession());
        verify(retryPolicy == TASK, "unexpected retry policy: %s", retryPolicy);
        this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
        this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.taskSourceFactory = requireNonNull(taskSourceFactory, "taskSourceFactory is null");
        this.taskDescriptorStorage = requireNonNull(taskDescriptorStorage, "taskDescriptorStorage is null");
        this.exchangeManager = requireNonNull(exchangeManager, "exchangeManager is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.taskRetryAttemptsOverall = taskRetryAttemptsOverall;
        this.taskRetryAttemptsPerTask = taskRetryAttemptsPerTask;
        this.maxTasksWaitingForNodePerStage = maxTasksWaitingForNodePerStage;
        this.scheduledExecutorService = requireNonNull(scheduledExecutorService, "scheduledExecutorService is null");
        this.nodeAllocatorService = requireNonNull(nodeAllocatorService, "nodeAllocatorService is null");
        this.partitionMemoryEstimatorFactory = requireNonNull(partitionMemoryEstimatorFactory, "partitionMemoryEstimatorFactory is null");
        this.taskExecutionStats = requireNonNull(taskExecutionStats, "taskExecutionStats is null");
        this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");

        stageManager = StageManager.create(
                queryStateMachine,
                metadata,
                taskFactory,
                nodeTaskMap,
                queryExecutor,
                schedulerStats,
                planTree,
                summarizeTaskInfo);
    }

    @Override
    public synchronized void start()
    {
        if (started) {
            return;
        }
        started = true;

        if (queryStateMachine.isDone()) {
            return;
        }

        // when query is done or any time a stage completes, attempt to transition query to "final query info ready"
        queryStateMachine.addStateChangeListener(state -> {
            if (!state.isDone()) {
                return;
            }
            Scheduler scheduler;
            synchronized (this) {
                scheduler = this.scheduler;
                this.scheduler = null;
            }
            if (state == QueryState.FINISHED) {
                if (scheduler != null) {
                    scheduler.cancel();
                }
                stageManager.finish();
            }
            else if (state == QueryState.FAILED) {
                if (scheduler != null) {
                    scheduler.abort();
                }
                stageManager.abort();
            }
            queryStateMachine.updateQueryInfo(Optional.ofNullable(getStageInfo()));
        });

        scheduler = createScheduler();
        queryExecutor.submit(scheduler::schedule);
    }

    private Scheduler createScheduler()
    {
        taskDescriptorStorage.initialize(queryStateMachine.getQueryId());
        queryStateMachine.addStateChangeListener(state -> {
            if (state.isDone()) {
                taskDescriptorStorage.destroy(queryStateMachine.getQueryId());
            }
        });

        Session session = queryStateMachine.getSession();
        int partitionCount = getFaultTolerantExecutionPartitionCount(session);
        Function<PartitioningHandle, BucketToPartition> bucketToPartitionCache = createBucketToPartitionCache(nodePartitioningManager, session, partitionCount);

        ImmutableList.Builder<FaultTolerantStageScheduler> schedulers = ImmutableList.builder();
        Map<PlanFragmentId, Exchange> exchanges = new HashMap<>();
        NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(session);

        try {
            // root to children order
            List<SqlStage> stagesInTopologicalOrder = stageManager.getStagesInTopologicalOrder();
            // children to root order
            List<SqlStage> stagesInReverseTopologicalOrder = reverse(stagesInTopologicalOrder);

            checkArgument(taskRetryAttemptsOverall >= 0, "taskRetryAttemptsOverall must be greater than or equal to 0: %s", taskRetryAttemptsOverall);
            AtomicInteger remainingTaskRetryAttemptsOverall = new AtomicInteger(taskRetryAttemptsOverall);
            List<Exchange> outputStages = new ArrayList<>();
            for (SqlStage stage : stagesInReverseTopologicalOrder) {
                PlanFragment fragment = stage.getFragment();

                boolean outputStage = stageManager.getOutputStage().getStageId().equals(stage.getStageId());
                ExchangeContext exchangeContext = new ExchangeContext(session.getQueryId(), new ExchangeId("external-exchange-" + stage.getStageId().getId()));
                Exchange exchange = exchangeManager.createExchange(
                        exchangeContext,
                        partitionCount,
                        // order of output records for coordinator consumed stages must be preserved as the stage
                        // may produce sorted dataset (for example an output of a global OrderByOperator)
                        outputStage);
                exchanges.put(fragment.getId(), exchange);

                if (outputStage) {
                    // output will be consumed by coordinator
                    outputStages.add(exchange);
                }

                ImmutableMap.Builder<PlanFragmentId, Exchange> sourceExchanges = ImmutableMap.builder();
                for (SqlStage childStage : stageManager.getChildren(fragment.getId())) {
                    PlanFragmentId childFragmentId = childStage.getFragment().getId();
                    Exchange sourceExchange = exchanges.get(childFragmentId);
                    verify(sourceExchange != null, "exchange not found for fragment: %s", childFragmentId);
                    sourceExchanges.put(childFragmentId, sourceExchange);
                }

                BucketToPartition inputBucketToPartition = bucketToPartitionCache.apply(fragment.getPartitioning());
                FaultTolerantStageScheduler scheduler = new FaultTolerantStageScheduler(
                        session,
                        stage,
                        failureDetector,
                        taskSourceFactory,
                        nodeAllocator,
                        taskDescriptorStorage,
                        partitionMemoryEstimatorFactory.createPartitionMemoryEstimator(),
                        taskExecutionStats,
                        (future, delay) -> scheduledExecutorService.schedule(() -> future.set(null), delay.toMillis(), MILLISECONDS),
                        systemTicker(),
                        exchange,
                        bucketToPartitionCache.apply(fragment.getPartitioningScheme().getPartitioning().getHandle()).getBucketToPartitionMap(),
                        sourceExchanges.buildOrThrow(),
                        inputBucketToPartition.getBucketToPartitionMap(),
                        inputBucketToPartition.getBucketNodeMap(),
                        remainingTaskRetryAttemptsOverall,
                        taskRetryAttemptsPerTask,
                        maxTasksWaitingForNodePerStage,
                        dynamicFilterService);

                schedulers.add(scheduler);
            }

            if (!stagesInReverseTopologicalOrder.isEmpty()) {
                verify(!outputStages.isEmpty(), "coordinatorConsumedExchanges is empty");
                List<ListenableFuture<List<ExchangeSourceHandle>>> futures = outputStages.stream()
                        .map(Exchange::getSourceHandles)
                        .map(Exchanges::getAllSourceHandles)
                        .collect(toImmutableList());
                addSuccessCallback(Futures.allAsList(futures), result -> {
                    List<ExchangeSourceHandle> handles = result.stream()
                            .flatMap(List::stream)
                            .collect(toImmutableList());
                    ImmutableList.Builder<ExchangeInput> inputs = ImmutableList.builder();
                    if (!handles.isEmpty()) {
                        inputs.add(new SpoolingExchangeInput(handles));
                    }
                    queryStateMachine.updateInputsForQueryResults(inputs.build(), true);
                });
            }

            return new Scheduler(
                    queryStateMachine,
                    schedulers.build(),
                    stageManager,
                    schedulerStats,
                    nodeAllocator);
        }
        catch (Throwable t) {
            for (FaultTolerantStageScheduler scheduler : schedulers.build()) {
                try {
                    scheduler.abort();
                }
                catch (Throwable closeFailure) {
                    if (t != closeFailure) {
                        t.addSuppressed(closeFailure);
                    }
                }
            }

            try {
                nodeAllocator.close();
            }
            catch (Throwable closeFailure) {
                if (t != closeFailure) {
                    t.addSuppressed(closeFailure);
                }
            }

            for (Exchange exchange : exchanges.values()) {
                try {
                    exchange.close();
                }
                catch (Throwable closeFailure) {
                    if (t != closeFailure) {
                        t.addSuppressed(closeFailure);
                    }
                }
            }
            throw t;
        }
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        throw new UnsupportedOperationException("partial cancel is not supported in fault tolerant mode");
    }

    @Override
    public void failTask(TaskId taskId, Throwable failureCause)
    {
        stageManager.failTaskRemotely(taskId, failureCause);
    }

    @Override
    public BasicStageStats getBasicStageStats()
    {
        return stageManager.getBasicStageStats();
    }

    @Override
    public StageInfo getStageInfo()
    {
        return stageManager.getStageInfo();
    }

    @Override
    public long getUserMemoryReservation()
    {
        return stageManager.getUserMemoryReservation();
    }

    @Override
    public long getTotalMemoryReservation()
    {
        return stageManager.getTotalMemoryReservation();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return stageManager.getTotalCpuTime();
    }

    private static class Scheduler
    {
        private final QueryStateMachine queryStateMachine;
        private final List<FaultTolerantStageScheduler> schedulers;
        private final StageManager stageManager;
        private final SplitSchedulerStats schedulerStats;
        private final NodeAllocator nodeAllocator;

        private Scheduler(
                QueryStateMachine queryStateMachine,
                List<FaultTolerantStageScheduler> schedulers,
                StageManager stageManager,
                SplitSchedulerStats schedulerStats,
                NodeAllocator nodeAllocator)
        {
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
            this.stageManager = requireNonNull(stageManager, "stageManager is null");
            this.schedulers = ImmutableList.copyOf(requireNonNull(schedulers, "schedulers is null"));
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.nodeAllocator = requireNonNull(nodeAllocator, "nodeAllocator is null");
        }

        public void schedule()
        {
            if (schedulers.isEmpty()) {
                queryStateMachine.transitionToFinishing();
                return;
            }

            queryStateMachine.transitionToRunning();

            try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
                List<ListenableFuture<Void>> blockedStages = new ArrayList<>();
                while (!isFinishingOrDone(queryStateMachine)) {
                    blockedStages.clear();
                    boolean atLeastOneStageIsNotBlocked = false;
                    boolean allFinished = true;
                    for (FaultTolerantStageScheduler scheduler : schedulers) {
                        if (scheduler.isFinished()) {
                            stageManager.get(scheduler.getStageId()).finish();
                            continue;
                        }
                        allFinished = false;
                        ListenableFuture<Void> blocked = scheduler.isBlocked();
                        if (!blocked.isDone()) {
                            blockedStages.add(blocked);
                            continue;
                        }
                        try {
                            scheduler.schedule();
                        }
                        catch (Throwable t) {
                            fail(t, Optional.of(scheduler.getStageId()));
                            return;
                        }
                        blocked = scheduler.isBlocked();
                        if (!blocked.isDone()) {
                            blockedStages.add(blocked);
                        }
                        else {
                            atLeastOneStageIsNotBlocked = true;
                        }
                    }
                    if (allFinished) {
                        queryStateMachine.transitionToFinishing();
                        return;
                    }
                    // wait for a state change and then schedule again
                    if (!atLeastOneStageIsNotBlocked) {
                        verify(!blockedStages.isEmpty(), "blockedStages is not expected to be empty here");
                        try (TimeStat.BlockTimer timer = schedulerStats.getSleepTime().time()) {
                            try {
                                tryGetFutureValue(whenAnyComplete(blockedStages), 1, SECONDS);
                            }
                            catch (CancellationException e) {
                                log.debug(
                                        "Scheduling has been cancelled for query %s. Query state: %s",
                                        queryStateMachine.getQueryId(),
                                        queryStateMachine.getQueryState());
                            }
                        }
                    }
                }
            }
            catch (Throwable t) {
                fail(t, Optional.empty());
            }
        }

        public void cancel()
        {
            schedulers.forEach(FaultTolerantStageScheduler::cancel);
            closeNodeAllocator();
        }

        public void abort()
        {
            schedulers.forEach(FaultTolerantStageScheduler::abort);
            closeNodeAllocator();
        }

        private void fail(Throwable t, Optional<StageId> failedStageId)
        {
            abort();
            stageManager.getStagesInTopologicalOrder().forEach(stage -> {
                if (failedStageId.isPresent() && failedStageId.get().equals(stage.getStageId())) {
                    stage.fail(t);
                }
                else {
                    stage.abort();
                }
            });
            queryStateMachine.transitionToFailed(t);
        }

        private void closeNodeAllocator()
        {
            try {
                nodeAllocator.close();
            }
            catch (Throwable t) {
                log.warn(t, "Error closing node allocator for query: %s", queryStateMachine.getQueryId());
            }
        }
    }

    private static Function<PartitioningHandle, BucketToPartition> createBucketToPartitionCache(NodePartitioningManager nodePartitioningManager, Session session, int partitionCount)
    {
        Map<PartitioningHandle, BucketToPartition> cachingMap = new HashMap<>();
        return partitioningHandle ->
                cachingMap.computeIfAbsent(
                        partitioningHandle,
                        handle -> createBucketToPartitionMap(session, partitionCount, handle, nodePartitioningManager));
    }

    private static BucketToPartition createBucketToPartitionMap(
            Session session,
            int partitionCount,
            PartitioningHandle partitioningHandle,
            NodePartitioningManager nodePartitioningManager)
    {
        if (partitioningHandle.equals(FIXED_HASH_DISTRIBUTION)) {
            return new BucketToPartition(Optional.of(IntStream.range(0, partitionCount).toArray()), Optional.empty());
        }
        if (partitioningHandle.getCatalogHandle().isPresent()) {
            BucketNodeMap bucketNodeMap = nodePartitioningManager.getBucketNodeMap(session, partitioningHandle);
            int bucketCount = bucketNodeMap.getBucketCount();
            int[] bucketToPartition = new int[bucketCount];
            // make sure all buckets mapped to the same node map to the same partition, such that locality requirements are respected in scheduling
            Map<InternalNode, Integer> nodeToPartition = new HashMap<>();
            int nextPartitionId = 0;
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                InternalNode node = bucketNodeMap.getAssignedNode(bucket);
                Integer partitionId = nodeToPartition.get(node);
                if (partitionId == null) {
                    partitionId = nextPartitionId;
                    nextPartitionId++;
                    nodeToPartition.put(node, partitionId);
                }
                bucketToPartition[bucket] = partitionId;
            }
            return new BucketToPartition(Optional.of(bucketToPartition), Optional.of(bucketNodeMap));
        }
        return new BucketToPartition(Optional.empty(), Optional.empty());
    }

    private static class BucketToPartition
    {
        private final Optional<int[]> bucketToPartitionMap;
        private final Optional<BucketNodeMap> bucketNodeMap;

        private BucketToPartition(Optional<int[]> bucketToPartitionMap, Optional<BucketNodeMap> bucketNodeMap)
        {
            this.bucketToPartitionMap = requireNonNull(bucketToPartitionMap, "bucketToPartitionMap is null");
            this.bucketNodeMap = requireNonNull(bucketNodeMap, "bucketNodeMap is null");
        }

        public Optional<int[]> getBucketToPartitionMap()
        {
            return bucketToPartitionMap;
        }

        public Optional<BucketNodeMap> getBucketNodeMap()
        {
            return bucketNodeMap;
        }
    }

    private static boolean isFinishingOrDone(QueryStateMachine queryStateMachine)
    {
        QueryState queryState = queryStateMachine.getQueryState();
        return queryState == FINISHING || queryState.isDone();
    }
}
