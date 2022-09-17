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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.trino.client.NodeVersion;
import io.trino.exchange.SpoolingExchangeInput;
import io.trino.execution.TableExecuteContextManager;
import io.trino.execution.scheduler.StageTaskSourceFactory.ArbitraryDistributionTaskSource;
import io.trino.execution.scheduler.StageTaskSourceFactory.HashDistributionTaskSource;
import io.trino.execution.scheduler.StageTaskSourceFactory.SingleDistributionTaskSource;
import io.trino.execution.scheduler.StageTaskSourceFactory.SourceDistributionTaskSource;
import io.trino.execution.scheduler.TestingExchange.TestingExchangeSourceHandle;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.QueryId;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.split.RemoteSplit;
import io.trino.split.SplitSource;
import io.trino.sql.planner.plan.PlanNodeId;
import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.findLast;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getDone;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.execution.scheduler.StageTaskSourceFactory.createRemoteSplits;
import static io.trino.operator.ExchangeOperator.REMOTE_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.guava.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestStageTaskSourceFactory
{
    private static final HostAddress NODE_ADDRESS = HostAddress.fromString("testaddress");
    private static final PlanNodeId PLAN_NODE_1 = new PlanNodeId("planNode1");
    private static final PlanNodeId PLAN_NODE_2 = new PlanNodeId("planNode2");
    private static final PlanNodeId PLAN_NODE_3 = new PlanNodeId("planNode3");
    private static final PlanNodeId PLAN_NODE_4 = new PlanNodeId("planNode4");
    private static final PlanNodeId PLAN_NODE_5 = new PlanNodeId("planNode5");
    public static final long STANDARD_WEIGHT = SplitWeight.standard().getRawValue();

    @Test
    public void testSingleDistributionTaskSource()
    {
        ListMultimap<PlanNodeId, ExchangeSourceHandle> sources = ImmutableListMultimap.<PlanNodeId, ExchangeSourceHandle>builder()
                .put(PLAN_NODE_1, new TestingExchangeSourceHandle(0, 123))
                .put(PLAN_NODE_2, new TestingExchangeSourceHandle(0, 321))
                .put(PLAN_NODE_1, new TestingExchangeSourceHandle(0, 222))
                .build();
        TaskSource taskSource = new SingleDistributionTaskSource(createRemoteSplits(sources), new InMemoryNodeManager(), false);

        assertFalse(taskSource.isFinished());

        List<TaskDescriptor> tasks = getFutureValue(taskSource.getMoreTasks());
        assertThat(tasks).hasSize(1);
        assertTrue(taskSource.isFinished());

        TaskDescriptor task = tasks.get(0);
        assertThat(task.getNodeRequirements().getCatalogHandle()).isEmpty();
        assertThat(task.getNodeRequirements().getAddresses()).isEmpty();
        assertEquals(task.getPartitionId(), 0);
        assertEquals(extractSourceHandles(task.getSplits()), sources);
        assertEquals(extractCatalogSplits(task.getSplits()), ImmutableListMultimap.of());
    }

    @Test
    public void testCoordinatorDistributionTaskSource()
    {
        ListMultimap<PlanNodeId, ExchangeSourceHandle> sources = ImmutableListMultimap.<PlanNodeId, ExchangeSourceHandle>builder()
                .put(PLAN_NODE_1, new TestingExchangeSourceHandle(0, 123))
                .put(PLAN_NODE_2, new TestingExchangeSourceHandle(0, 321))
                .put(PLAN_NODE_1, new TestingExchangeSourceHandle(0, 222))
                .build();
        InternalNodeManager nodeManager = new InMemoryNodeManager();
        TaskSource taskSource = new SingleDistributionTaskSource(createRemoteSplits(sources), nodeManager, true);

        assertFalse(taskSource.isFinished());

        List<TaskDescriptor> tasks = getFutureValue(taskSource.getMoreTasks());
        assertThat(tasks).hasSize(1);
        assertTrue(taskSource.isFinished());

        TaskDescriptor task = tasks.get(0);
        assertThat(task.getNodeRequirements().getCatalogHandle()).isEmpty();
        assertThat(task.getNodeRequirements().getAddresses()).containsExactly(nodeManager.getCurrentNode().getHostAndPort());
        assertEquals(task.getPartitionId(), 0);
        assertEquals(extractSourceHandles(task.getSplits()), sources);
        assertEquals(extractCatalogSplits(task.getSplits()), ImmutableListMultimap.of());
    }

    @Test
    public void testArbitraryDistributionTaskSource()
    {
        TaskSource taskSource = new ArbitraryDistributionTaskSource(
                ImmutableListMultimap.of(),
                ImmutableListMultimap.of(),
                DataSize.of(3, BYTE));
        assertFalse(taskSource.isFinished());
        List<TaskDescriptor> tasks = getFutureValue(taskSource.getMoreTasks());
        assertThat(tasks).isEmpty();
        assertTrue(taskSource.isFinished());

        TestingExchangeSourceHandle sourceHandle1 = new TestingExchangeSourceHandle(0, 1);
        TestingExchangeSourceHandle sourceHandle2 = new TestingExchangeSourceHandle(0, 2);
        TestingExchangeSourceHandle sourceHandle3 = new TestingExchangeSourceHandle(0, 3);
        TestingExchangeSourceHandle sourceHandle4 = new TestingExchangeSourceHandle(0, 4);
        TestingExchangeSourceHandle sourceHandle123 = new TestingExchangeSourceHandle(0, 123);
        TestingExchangeSourceHandle sourceHandle321 = new TestingExchangeSourceHandle(0, 321);
        Multimap<PlanNodeId, ExchangeSourceHandle> nonReplicatedSources = ImmutableListMultimap.of(PLAN_NODE_1, sourceHandle3);
        taskSource = new ArbitraryDistributionTaskSource(
                nonReplicatedSources,
                ImmutableListMultimap.of(),
                DataSize.of(3, BYTE));
        tasks = getFutureValue(taskSource.getMoreTasks());
        assertTrue(taskSource.isFinished());
        assertThat(tasks).hasSize(1);
        assertEquals(tasks.get(0).getPartitionId(), 0);
        assertEquals(tasks.get(0).getNodeRequirements(), new NodeRequirements(Optional.empty(), ImmutableSet.of()));
        assertEquals(extractSourceHandles(tasks.get(0).getSplits()), ImmutableListMultimap.of(PLAN_NODE_1, new TestingExchangeSourceHandle(0, 3)));

        nonReplicatedSources = ImmutableListMultimap.of(PLAN_NODE_1, sourceHandle123);
        taskSource = new ArbitraryDistributionTaskSource(
                nonReplicatedSources,
                ImmutableListMultimap.of(),
                DataSize.of(3, BYTE));
        tasks = getFutureValue(taskSource.getMoreTasks());
        assertThat(tasks).hasSize(1);
        assertEquals(tasks.get(0).getPartitionId(), 0);
        assertEquals(tasks.get(0).getNodeRequirements(), new NodeRequirements(Optional.empty(), ImmutableSet.of()));
        assertEquals(extractSourceHandles(tasks.get(0).getSplits()), ImmutableListMultimap.of(PLAN_NODE_1, new TestingExchangeSourceHandle(0, 123)));

        nonReplicatedSources = ImmutableListMultimap.of(
                PLAN_NODE_1, sourceHandle123,
                PLAN_NODE_2, sourceHandle321);
        taskSource = new ArbitraryDistributionTaskSource(
                nonReplicatedSources,
                ImmutableListMultimap.of(),
                DataSize.of(3, BYTE));
        tasks = getFutureValue(taskSource.getMoreTasks());
        assertThat(tasks).hasSize(2);
        assertEquals(tasks.get(0).getPartitionId(), 0);
        assertEquals(tasks.get(0).getNodeRequirements(), new NodeRequirements(Optional.empty(), ImmutableSet.of()));
        assertEquals(extractSourceHandles(tasks.get(0).getSplits()), ImmutableListMultimap.of(PLAN_NODE_1, new TestingExchangeSourceHandle(0, 123)));
        assertEquals(tasks.get(1).getPartitionId(), 1);
        assertEquals(tasks.get(1).getNodeRequirements(), new NodeRequirements(Optional.empty(), ImmutableSet.of()));
        assertEquals(extractSourceHandles(tasks.get(1).getSplits()), ImmutableListMultimap.of(PLAN_NODE_2, new TestingExchangeSourceHandle(0, 321)));

        nonReplicatedSources = ImmutableListMultimap.of(
                PLAN_NODE_1, sourceHandle1,
                PLAN_NODE_1, sourceHandle2,
                PLAN_NODE_2, sourceHandle4);
        taskSource = new ArbitraryDistributionTaskSource(
                nonReplicatedSources,
                ImmutableListMultimap.of(),
                DataSize.of(3, BYTE));
        tasks = getFutureValue(taskSource.getMoreTasks());
        assertThat(tasks).hasSize(2);
        assertEquals(tasks.get(0).getPartitionId(), 0);
        assertEquals(tasks.get(0).getNodeRequirements(), new NodeRequirements(Optional.empty(), ImmutableSet.of()));
        assertEquals(
                extractSourceHandles(tasks.get(0).getSplits()),
                ImmutableListMultimap.of(
                        PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1),
                        PLAN_NODE_1, new TestingExchangeSourceHandle(0, 2)));
        assertEquals(tasks.get(1).getPartitionId(), 1);
        assertEquals(tasks.get(1).getNodeRequirements(), new NodeRequirements(Optional.empty(), ImmutableSet.of()));
        assertEquals(extractSourceHandles(tasks.get(1).getSplits()), ImmutableListMultimap.of(PLAN_NODE_2, new TestingExchangeSourceHandle(0, 4)));

        nonReplicatedSources = ImmutableListMultimap.of(
                PLAN_NODE_1, sourceHandle1,
                PLAN_NODE_1, sourceHandle3,
                PLAN_NODE_2, sourceHandle4);
        taskSource = new ArbitraryDistributionTaskSource(
                nonReplicatedSources,
                ImmutableListMultimap.of(),
                DataSize.of(3, BYTE));
        tasks = getFutureValue(taskSource.getMoreTasks());
        assertThat(tasks).hasSize(3);
        assertEquals(tasks.get(0).getPartitionId(), 0);
        assertEquals(tasks.get(0).getNodeRequirements(), new NodeRequirements(Optional.empty(), ImmutableSet.of()));
        assertEquals(extractSourceHandles(tasks.get(0).getSplits()), ImmutableListMultimap.of(PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1)));
        assertEquals(tasks.get(1).getPartitionId(), 1);
        assertEquals(tasks.get(1).getNodeRequirements(), new NodeRequirements(Optional.empty(), ImmutableSet.of()));
        assertEquals(extractSourceHandles(tasks.get(1).getSplits()), ImmutableListMultimap.of(PLAN_NODE_1, new TestingExchangeSourceHandle(0, 3)));
        assertEquals(tasks.get(2).getPartitionId(), 2);
        assertEquals(tasks.get(2).getNodeRequirements(), new NodeRequirements(Optional.empty(), ImmutableSet.of()));
        assertEquals(extractSourceHandles(tasks.get(2).getSplits()), ImmutableListMultimap.of(PLAN_NODE_2, new TestingExchangeSourceHandle(0, 4)));

        // with replicated sources
        nonReplicatedSources = ImmutableListMultimap.of(
                PLAN_NODE_1, sourceHandle1,
                PLAN_NODE_1, sourceHandle2,
                PLAN_NODE_1, sourceHandle4);
        Multimap<PlanNodeId, ExchangeSourceHandle> replicatedSources = ImmutableListMultimap.of(
                PLAN_NODE_2, sourceHandle321);
        taskSource = new ArbitraryDistributionTaskSource(
                nonReplicatedSources,
                replicatedSources,
                DataSize.of(3, BYTE));
        tasks = getFutureValue(taskSource.getMoreTasks());
        assertThat(tasks).hasSize(2);
        assertEquals(tasks.get(0).getPartitionId(), 0);
        assertEquals(tasks.get(0).getNodeRequirements(), new NodeRequirements(Optional.empty(), ImmutableSet.of()));
        assertEquals(
                extractSourceHandles(tasks.get(0).getSplits()),
                ImmutableListMultimap.of(
                        PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1),
                        PLAN_NODE_1, new TestingExchangeSourceHandle(0, 2),
                        PLAN_NODE_2, new TestingExchangeSourceHandle(0, 321)));
        assertEquals(tasks.get(1).getPartitionId(), 1);
        assertEquals(tasks.get(1).getNodeRequirements(), new NodeRequirements(Optional.empty(), ImmutableSet.of()));
        assertEquals(
                extractSourceHandles(tasks.get(1).getSplits()),
                ImmutableListMultimap.of(
                        PLAN_NODE_1, new TestingExchangeSourceHandle(0, 4),
                        PLAN_NODE_2, sourceHandle321));
    }

    @Test
    public void testHashDistributionTaskSource()
    {
        TaskSource taskSource = createHashDistributionTaskSource(
                ImmutableMap.of(),
                ImmutableListMultimap.of(),
                ImmutableListMultimap.of(),
                1,
                createPartitioningScheme(4),
                0,
                DataSize.of(3, BYTE));
        assertFalse(taskSource.isFinished());
        assertEquals(getFutureValue(taskSource.getMoreTasks()), ImmutableList.of());
        assertTrue(taskSource.isFinished());

        taskSource = createHashDistributionTaskSource(
                ImmutableMap.of(),
                ImmutableListMultimap.of(
                        PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1),
                        PLAN_NODE_1, new TestingExchangeSourceHandle(1, 1),
                        PLAN_NODE_2, new TestingExchangeSourceHandle(0, 1),
                        PLAN_NODE_2, new TestingExchangeSourceHandle(3, 1)),
                ImmutableListMultimap.of(
                        PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)),
                1,
                createPartitioningScheme(4),
                0,
                DataSize.of(0, BYTE));
        assertFalse(taskSource.isFinished());
        List<TaskDescriptor> tasks = getFutureValue(taskSource.getMoreTasks());
        assertTrue(taskSource.isFinished());
        assertThat(tasks).hasSize(3);
        assertEquals(tasks.get(0).getPartitionId(), 0);
        assertEquals(tasks.get(0).getNodeRequirements(), new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of()));
        assertEquals(extractSourceHandles(
                        tasks.get(0).getSplits()),
                ImmutableListMultimap.of(
                        PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1),
                        PLAN_NODE_2, new TestingExchangeSourceHandle(0, 1),
                        PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)));
        assertEquals(tasks.get(1).getPartitionId(), 1);
        assertEquals(tasks.get(1).getNodeRequirements(), new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of()));
        assertEquals(
                extractSourceHandles(tasks.get(1).getSplits()),
                ImmutableListMultimap.of(
                        PLAN_NODE_1, new TestingExchangeSourceHandle(1, 1),
                        PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)));
        assertEquals(tasks.get(2).getPartitionId(), 2);
        assertEquals(tasks.get(2).getNodeRequirements(), new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of()));
        assertEquals(
                extractSourceHandles(tasks.get(2).getSplits()),
                ImmutableListMultimap.of(
                        PLAN_NODE_2, new TestingExchangeSourceHandle(3, 1),
                        PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)));

        Split bucketedSplit1 = createBucketedSplit(0, 0);
        Split bucketedSplit2 = createBucketedSplit(0, 2);
        Split bucketedSplit3 = createBucketedSplit(0, 3);
        Split bucketedSplit4 = createBucketedSplit(0, 1);

        taskSource = createHashDistributionTaskSource(
                ImmutableMap.of(
                        PLAN_NODE_4, new TestingSplitSource(TEST_CATALOG_HANDLE, ImmutableList.of(bucketedSplit1, bucketedSplit2, bucketedSplit3)),
                        PLAN_NODE_5, new TestingSplitSource(TEST_CATALOG_HANDLE, ImmutableList.of(bucketedSplit4))),
                ImmutableListMultimap.of(),
                ImmutableListMultimap.of(
                        PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)),
                1,
                createPartitioningScheme(4, 4),
                0,
                DataSize.of(0, BYTE));
        assertFalse(taskSource.isFinished());
        tasks = getFutureValue(taskSource.getMoreTasks());
        assertTrue(taskSource.isFinished());
        assertThat(tasks).hasSize(4);
        assertEquals(tasks.get(0).getPartitionId(), 0);
        assertEquals(tasks.get(0).getNodeRequirements(), new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of(NODE_ADDRESS)));
        assertEquals(extractCatalogSplits(tasks.get(0).getSplits()), ImmutableListMultimap.of(PLAN_NODE_4, bucketedSplit1));
        assertEquals(extractSourceHandles(tasks.get(0).getSplits()), ImmutableListMultimap.of(PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)));
        assertEquals(tasks.get(1).getPartitionId(), 1);
        assertEquals(tasks.get(1).getNodeRequirements(), new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of(NODE_ADDRESS)));
        assertEquals(extractCatalogSplits(tasks.get(1).getSplits()), ImmutableListMultimap.of(PLAN_NODE_5, bucketedSplit4));
        assertEquals(extractSourceHandles(tasks.get(1).getSplits()), ImmutableListMultimap.of(PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)));
        assertEquals(tasks.get(2).getPartitionId(), 2);
        assertEquals(tasks.get(2).getNodeRequirements(), new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of(NODE_ADDRESS)));
        assertEquals(extractCatalogSplits(tasks.get(2).getSplits()), ImmutableListMultimap.of(PLAN_NODE_4, bucketedSplit2));
        assertEquals(extractSourceHandles(tasks.get(2).getSplits()), ImmutableListMultimap.of(PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)));
        assertEquals(tasks.get(3).getPartitionId(), 3);
        assertEquals(tasks.get(3).getNodeRequirements(), new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of(NODE_ADDRESS)));
        assertEquals(extractCatalogSplits(tasks.get(3).getSplits()), ImmutableListMultimap.of(PLAN_NODE_4, bucketedSplit3));
        assertEquals(extractSourceHandles(tasks.get(3).getSplits()), ImmutableListMultimap.of(PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)));

        taskSource = createHashDistributionTaskSource(
                ImmutableMap.of(
                        PLAN_NODE_4, new TestingSplitSource(TEST_CATALOG_HANDLE, ImmutableList.of(bucketedSplit1, bucketedSplit2, bucketedSplit3)),
                        PLAN_NODE_5, new TestingSplitSource(TEST_CATALOG_HANDLE, ImmutableList.of(bucketedSplit4))),
                ImmutableListMultimap.of(
                        PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1),
                        PLAN_NODE_1, new TestingExchangeSourceHandle(1, 1),
                        PLAN_NODE_2, new TestingExchangeSourceHandle(0, 1),
                        PLAN_NODE_2, new TestingExchangeSourceHandle(3, 1)),
                ImmutableListMultimap.of(
                        PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)),
                1,
                createPartitioningScheme(4, 4),
                0,
                DataSize.of(0, BYTE));
        assertFalse(taskSource.isFinished());
        tasks = getFutureValue(taskSource.getMoreTasks());
        assertTrue(taskSource.isFinished());
        assertThat(tasks).hasSize(4);
        assertEquals(tasks.get(0).getPartitionId(), 0);
        assertEquals(tasks.get(0).getNodeRequirements(), new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of(NODE_ADDRESS)));
        assertEquals(extractCatalogSplits(tasks.get(0).getSplits()), ImmutableListMultimap.of(PLAN_NODE_4, bucketedSplit1));
        assertEquals(extractSourceHandles(tasks.get(0).getSplits()), ImmutableListMultimap.of(
                PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1),
                PLAN_NODE_2, new TestingExchangeSourceHandle(0, 1),
                PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)));
        assertEquals(tasks.get(1).getPartitionId(), 1);
        assertEquals(tasks.get(1).getNodeRequirements(), new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of(NODE_ADDRESS)));
        assertEquals(extractCatalogSplits(tasks.get(1).getSplits()), ImmutableListMultimap.of(PLAN_NODE_5, bucketedSplit4));
        assertEquals(extractSourceHandles(tasks.get(1).getSplits()), ImmutableListMultimap.of(
                PLAN_NODE_1, new TestingExchangeSourceHandle(1, 1),
                PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)));
        assertEquals(tasks.get(2).getPartitionId(), 2);
        assertEquals(tasks.get(2).getNodeRequirements(), new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of(NODE_ADDRESS)));
        assertEquals(extractCatalogSplits(tasks.get(2).getSplits()), ImmutableListMultimap.of(PLAN_NODE_4, bucketedSplit2));
        assertEquals(extractSourceHandles(tasks.get(2).getSplits()), ImmutableListMultimap.of(PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)));
        assertEquals(tasks.get(3).getPartitionId(), 3);
        assertEquals(tasks.get(3).getNodeRequirements(), new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of(NODE_ADDRESS)));
        assertEquals(extractCatalogSplits(tasks.get(3).getSplits()), ImmutableListMultimap.of(PLAN_NODE_4, bucketedSplit3));
        assertEquals(extractSourceHandles(tasks.get(3).getSplits()), ImmutableListMultimap.of(
                PLAN_NODE_2, new TestingExchangeSourceHandle(3, 1),
                PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)));

        taskSource = createHashDistributionTaskSource(
                ImmutableMap.of(
                        PLAN_NODE_4, new TestingSplitSource(TEST_CATALOG_HANDLE, ImmutableList.of(bucketedSplit1, bucketedSplit2, bucketedSplit3)),
                        PLAN_NODE_5, new TestingSplitSource(TEST_CATALOG_HANDLE, ImmutableList.of(bucketedSplit4))),
                ImmutableListMultimap.of(
                        PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1),
                        PLAN_NODE_1, new TestingExchangeSourceHandle(1, 1),
                        PLAN_NODE_2, new TestingExchangeSourceHandle(0, 1)),
                ImmutableListMultimap.of(
                        PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)),
                2,
                createPartitioningScheme(2, 4),
                0, DataSize.of(0, BYTE));
        assertFalse(taskSource.isFinished());
        tasks = getFutureValue(taskSource.getMoreTasks());
        assertTrue(taskSource.isFinished());
        assertThat(tasks).hasSize(2);
        assertEquals(tasks.get(0).getPartitionId(), 0);
        assertEquals(tasks.get(0).getNodeRequirements(), new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of(NODE_ADDRESS)));
        assertEquals(extractCatalogSplits(tasks.get(0).getSplits()), ImmutableListMultimap.of(
                PLAN_NODE_4, bucketedSplit1,
                PLAN_NODE_4, bucketedSplit2));
        assertEquals(extractSourceHandles(tasks.get(0).getSplits()), ImmutableListMultimap.of(
                PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1),
                PLAN_NODE_2, new TestingExchangeSourceHandle(0, 1),
                PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)));
        assertEquals(tasks.get(1).getPartitionId(), 1);
        assertEquals(tasks.get(1).getNodeRequirements(), new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of(NODE_ADDRESS)));
        assertEquals(extractCatalogSplits(tasks.get(1).getSplits()), ImmutableListMultimap.of(
                PLAN_NODE_4, bucketedSplit3,
                PLAN_NODE_5, bucketedSplit4));
        assertEquals(extractSourceHandles(tasks.get(1).getSplits()), ImmutableListMultimap.of(
                PLAN_NODE_1, new TestingExchangeSourceHandle(1, 1),
                PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)));

        // join based on split target split weight
        taskSource = createHashDistributionTaskSource(
                ImmutableMap.of(
                        PLAN_NODE_4, new TestingSplitSource(TEST_CATALOG_HANDLE, ImmutableList.of(bucketedSplit1, bucketedSplit2, bucketedSplit3)),
                        PLAN_NODE_5, new TestingSplitSource(TEST_CATALOG_HANDLE, ImmutableList.of(bucketedSplit4))),
                ImmutableListMultimap.of(
                        PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1),
                        PLAN_NODE_1, new TestingExchangeSourceHandle(1, 1),
                        PLAN_NODE_2, new TestingExchangeSourceHandle(1, 1),
                        PLAN_NODE_2, new TestingExchangeSourceHandle(2, 1),
                        PLAN_NODE_2, new TestingExchangeSourceHandle(3, 1)),
                ImmutableListMultimap.of(
                        PLAN_NODE_3, new TestingExchangeSourceHandle(17, 1)),
                2,
                createPartitioningScheme(4, 4),
                2 * STANDARD_WEIGHT,
                DataSize.of(100, GIGABYTE));
        assertFalse(taskSource.isFinished());
        tasks = getFutureValue(taskSource.getMoreTasks());
        assertTrue(taskSource.isFinished());
        assertThat(tasks).hasSize(2);
        assertEquals(tasks.get(0).getPartitionId(), 0);
        assertEquals(tasks.get(0).getNodeRequirements(), new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of(NODE_ADDRESS)));
        assertEquals(extractCatalogSplits(tasks.get(0).getSplits()), ImmutableListMultimap.of(
                PLAN_NODE_4, bucketedSplit1,
                PLAN_NODE_5, bucketedSplit4));
        assertEquals(extractSourceHandles(tasks.get(0).getSplits()), ImmutableListMultimap.of(
                PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1),
                PLAN_NODE_1, new TestingExchangeSourceHandle(1, 1),
                PLAN_NODE_2, new TestingExchangeSourceHandle(1, 1),
                PLAN_NODE_3, new TestingExchangeSourceHandle(17, 1)));
        assertEquals(tasks.get(1).getPartitionId(), 1);
        assertEquals(tasks.get(1).getNodeRequirements(), new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of(NODE_ADDRESS)));
        assertEquals(extractCatalogSplits(tasks.get(1).getSplits()), ImmutableListMultimap.of(
                PLAN_NODE_4, bucketedSplit2,
                PLAN_NODE_4, bucketedSplit3));
        assertEquals(extractSourceHandles(tasks.get(1).getSplits()), ImmutableListMultimap.of(
                PLAN_NODE_2, new TestingExchangeSourceHandle(2, 1),
                PLAN_NODE_2, new TestingExchangeSourceHandle(3, 1),
                PLAN_NODE_3, new TestingExchangeSourceHandle(17, 1)));

        // join based on target exchange size
        taskSource = createHashDistributionTaskSource(
                ImmutableMap.of(
                        PLAN_NODE_4, new TestingSplitSource(TEST_CATALOG_HANDLE, ImmutableList.of(bucketedSplit1, bucketedSplit2, bucketedSplit3)),
                        PLAN_NODE_5, new TestingSplitSource(TEST_CATALOG_HANDLE, ImmutableList.of(bucketedSplit4))),
                ImmutableListMultimap.of(
                        PLAN_NODE_1, new TestingExchangeSourceHandle(0, 20),
                        PLAN_NODE_1, new TestingExchangeSourceHandle(1, 30),
                        PLAN_NODE_2, new TestingExchangeSourceHandle(1, 20),
                        PLAN_NODE_2, new TestingExchangeSourceHandle(2, 99),
                        PLAN_NODE_2, new TestingExchangeSourceHandle(3, 30)),
                ImmutableListMultimap.of(
                        PLAN_NODE_3, new TestingExchangeSourceHandle(17, 1)),
                2,
                createPartitioningScheme(4, 4),
                100 * STANDARD_WEIGHT,
                DataSize.of(100, BYTE));
        assertFalse(taskSource.isFinished());
        tasks = getFutureValue(taskSource.getMoreTasks());
        assertTrue(taskSource.isFinished());
        assertThat(tasks).hasSize(3);
        assertEquals(tasks.get(0).getPartitionId(), 0);
        assertEquals(tasks.get(0).getNodeRequirements(), new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of(NODE_ADDRESS)));
        assertEquals(extractCatalogSplits(tasks.get(0).getSplits()), ImmutableListMultimap.of(
                PLAN_NODE_4, bucketedSplit1,
                PLAN_NODE_5, bucketedSplit4));
        assertEquals(extractSourceHandles(tasks.get(0).getSplits()), ImmutableListMultimap.of(
                PLAN_NODE_1, new TestingExchangeSourceHandle(0, 20),
                PLAN_NODE_1, new TestingExchangeSourceHandle(1, 30),
                PLAN_NODE_2, new TestingExchangeSourceHandle(1, 20),
                PLAN_NODE_3, new TestingExchangeSourceHandle(17, 1)));
        assertEquals(tasks.get(1).getPartitionId(), 1);
        assertEquals(tasks.get(1).getNodeRequirements(), new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of(NODE_ADDRESS)));
        assertEquals(extractCatalogSplits(tasks.get(1).getSplits()), ImmutableListMultimap.of(PLAN_NODE_4, bucketedSplit2));
        assertEquals(extractSourceHandles(tasks.get(1).getSplits()), ImmutableListMultimap.of(
                PLAN_NODE_2, new TestingExchangeSourceHandle(2, 99),
                PLAN_NODE_3, new TestingExchangeSourceHandle(17, 1)));
        assertEquals(tasks.get(2).getPartitionId(), 2);
        assertEquals(tasks.get(2).getNodeRequirements(), new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of(NODE_ADDRESS)));
        assertEquals(extractCatalogSplits(tasks.get(2).getSplits()), ImmutableListMultimap.of(PLAN_NODE_4, bucketedSplit3));
        assertEquals(extractSourceHandles(tasks.get(2).getSplits()), ImmutableListMultimap.of(
                PLAN_NODE_2, new TestingExchangeSourceHandle(3, 30),
                PLAN_NODE_3, new TestingExchangeSourceHandle(17, 1)));
    }

    private static HashDistributionTaskSource createHashDistributionTaskSource(
            Map<PlanNodeId, SplitSource> splitSources,
            ListMultimap<PlanNodeId, ExchangeSourceHandle> partitionedExchangeSources,
            ListMultimap<PlanNodeId, ExchangeSourceHandle> replicatedExchangeSources,
            int splitBatchSize,
            FaultTolerantPartitioningScheme sourcePartitioningScheme,
            long targetPartitionSplitWeight,
            DataSize targetPartitionSourceSize)
    {
        return new HashDistributionTaskSource(
                splitSources,
                partitionedExchangeSources,
                replicatedExchangeSources,
                splitBatchSize,
                getSplitsTime -> {},
                sourcePartitioningScheme,
                Optional.of(TEST_CATALOG_HANDLE),
                targetPartitionSplitWeight,
                targetPartitionSourceSize,
                directExecutor());
    }

    @Test
    public void testSourceDistributionTaskSource()
    {
        TaskSource taskSource = createSourceDistributionTaskSource(ImmutableList.of(), ImmutableListMultimap.of(), 2, 0, 3 * STANDARD_WEIGHT, 1000);
        assertFalse(taskSource.isFinished());
        assertEquals(getFutureValue(taskSource.getMoreTasks()), ImmutableList.of());
        assertTrue(taskSource.isFinished());

        Split split1 = createSplit(1);
        Split split2 = createSplit(2);
        Split split3 = createSplit(3);

        taskSource = createSourceDistributionTaskSource(
                ImmutableList.of(split1),
                ImmutableListMultimap.of(),
                2,
                0,
                2 * STANDARD_WEIGHT,
                1000);
        assertEquals(getFutureValue(taskSource.getMoreTasks()), ImmutableList.of(new TaskDescriptor(
                0,
                ImmutableListMultimap.of(PLAN_NODE_1, split1),
                new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of()))));
        assertTrue(taskSource.isFinished());

        taskSource = createSourceDistributionTaskSource(
                ImmutableList.of(split1, split2, split3),
                ImmutableListMultimap.of(),
                3,
                0,
                2 * STANDARD_WEIGHT,
                1000);

        List<TaskDescriptor> tasks = readAllTasks(taskSource);
        assertThat(tasks).hasSize(2);
        assertThat(tasks.get(0).getSplits().values()).hasSize(2);
        assertThat(tasks.get(1).getSplits().values()).hasSize(1);
        assertThat(tasks).allMatch(taskDescriptor -> taskDescriptor.getNodeRequirements().equals(new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of())));
        assertThat(tasks).allMatch(taskDescriptor -> extractSourceHandles(taskDescriptor.getSplits()).isEmpty());
        assertThat(flattenSplits(tasks)).hasSameEntriesAs(ImmutableMultimap.of(
                PLAN_NODE_1, split1,
                PLAN_NODE_1, split2,
                PLAN_NODE_1, split3));
        assertTrue(taskSource.isFinished());

        ImmutableListMultimap<PlanNodeId, ExchangeSourceHandle> replicatedSources = ImmutableListMultimap.of(PLAN_NODE_2, new TestingExchangeSourceHandle(0, 1));
        taskSource = createSourceDistributionTaskSource(
                ImmutableList.of(split1, split2, split3),
                replicatedSources,
                2,
                0,
                2 * STANDARD_WEIGHT,
                1000);

        tasks = readAllTasks(taskSource);
        assertThat(tasks).hasSize(2);
        assertThat(tasks.get(0).getSplits().values()).hasSize(3);
        assertThat(tasks.get(1).getSplits().values()).hasSize(2);
        assertThat(tasks).allMatch(taskDescriptor -> taskDescriptor.getNodeRequirements().equals(new NodeRequirements(Optional.of(TEST_CATALOG_HANDLE), ImmutableSet.of())));
        assertThat(tasks).allMatch(taskDescriptor -> extractSourceHandles(taskDescriptor.getSplits()).equals(replicatedSources));
        assertThat(extractCatalogSplits(flattenSplits(tasks))).hasSameEntriesAs(ImmutableMultimap.of(
                PLAN_NODE_1, split1,
                PLAN_NODE_1, split2,
                PLAN_NODE_1, split3));
        assertTrue(taskSource.isFinished());

        // non remotely accessible splits
        ImmutableList<Split> splits = ImmutableList.of(
                createSplit(1, "host1:8080", "host2:8080"),
                createSplit(2, "host2:8080"),
                createSplit(3, "host1:8080", "host3:8080"),
                createSplit(4, "host3:8080", "host1:8080"),
                createSplit(5, "host1:8080", "host2:8080"),
                createSplit(6, "host2:8080", "host3:8080"),
                createSplit(7, "host3:8080", "host4:8080"));
        taskSource = createSourceDistributionTaskSource(splits, ImmutableListMultimap.of(), 3, 0, 2 * STANDARD_WEIGHT, 1000);

        tasks = readAllTasks(taskSource);

        assertThat(tasks).hasSize(4);
        assertThat(tasks.stream()).allMatch(taskDescriptor -> extractSourceHandles(taskDescriptor.getSplits()).isEmpty());
        assertThat(flattenSplits(tasks)).hasSameEntriesAs(Multimaps.index(splits, split -> PLAN_NODE_1));
        assertThat(tasks).allMatch(task -> task.getSplits().values().stream().allMatch(split -> {
            HostAddress requiredAddress = getOnlyElement(task.getNodeRequirements().getAddresses());
            return split.getAddresses().contains(requiredAddress);
        }));
        assertTrue(taskSource.isFinished());
    }

    @Test
    public void testSourceDistributionTaskSourceWithWeights()
    {
        Split split1 = createWeightedSplit(1, STANDARD_WEIGHT);
        long heavyWeight = 2 * STANDARD_WEIGHT;
        Split heavySplit1 = createWeightedSplit(11, heavyWeight);
        Split heavySplit2 = createWeightedSplit(12, heavyWeight);
        Split heavySplit3 = createWeightedSplit(13, heavyWeight);
        long lightWeight = (long) (0.5 * STANDARD_WEIGHT);
        Split lightSplit1 = createWeightedSplit(21, lightWeight);
        Split lightSplit2 = createWeightedSplit(22, lightWeight);
        Split lightSplit3 = createWeightedSplit(23, lightWeight);
        Split lightSplit4 = createWeightedSplit(24, lightWeight);

        // no limits
        TaskSource taskSource = createSourceDistributionTaskSource(
                ImmutableList.of(lightSplit1, lightSplit2, split1, heavySplit1, heavySplit2, lightSplit4),
                ImmutableListMultimap.of(),
                1, // single split per batch for predictable results
                0,
                (long) (1.9 * STANDARD_WEIGHT),
                1000);
        List<TaskDescriptor> tasks = readAllTasks(taskSource);
        assertThat(tasks).hasSize(4);
        assertThat(tasks).allMatch(task -> getOnlyElement(task.getSplits().keySet()).equals(PLAN_NODE_1));
        assertThat(tasks.get(0).getSplits().values()).containsExactlyInAnyOrder(lightSplit1, lightSplit2, split1);
        assertThat(tasks.get(1).getSplits().values()).containsExactlyInAnyOrder(heavySplit1);
        assertThat(tasks.get(2).getSplits().values()).containsExactlyInAnyOrder(heavySplit2);
        assertThat(tasks.get(3).getSplits().values()).containsExactlyInAnyOrder(lightSplit4); // remainder
        assertTrue(taskSource.isFinished());

        // min splits == 2
        taskSource = createSourceDistributionTaskSource(
                ImmutableList.of(heavySplit1, heavySplit2, heavySplit3, lightSplit1, lightSplit2, lightSplit3, lightSplit4),
                ImmutableListMultimap.of(),
                1, // single split per batch for predictable results
                2,
                2 * STANDARD_WEIGHT,
                1000);

        tasks = readAllTasks(taskSource);
        assertThat(tasks).hasSize(3);
        assertThat(tasks).allMatch(task -> getOnlyElement(task.getSplits().keySet()).equals(PLAN_NODE_1));
        assertThat(tasks.get(0).getSplits().values()).containsExactlyInAnyOrder(heavySplit1, heavySplit2);
        assertThat(tasks.get(1).getSplits().values()).containsExactlyInAnyOrder(heavySplit3, lightSplit1);
        assertThat(tasks.get(2).getSplits().values()).containsExactlyInAnyOrder(lightSplit2, lightSplit3, lightSplit4);
        assertTrue(taskSource.isFinished());

        // max splits == 3
        taskSource = createSourceDistributionTaskSource(
                ImmutableList.of(lightSplit1, lightSplit2, lightSplit3, heavySplit1, lightSplit4),
                ImmutableListMultimap.of(),
                1, // single split per batch for predictable results
                0,
                2 * STANDARD_WEIGHT,
                3);

        tasks = readAllTasks(taskSource);
        assertThat(tasks).hasSize(3);
        assertThat(tasks).allMatch(task -> getOnlyElement(task.getSplits().keySet()).equals(PLAN_NODE_1));
        assertThat(tasks.get(0).getSplits().values()).containsExactlyInAnyOrder(lightSplit1, lightSplit2, lightSplit3);
        assertThat(tasks.get(1).getSplits().values()).containsExactlyInAnyOrder(heavySplit1);
        assertThat(tasks.get(2).getSplits().values()).containsExactlyInAnyOrder(lightSplit4);
        assertTrue(taskSource.isFinished());

        // with addresses
        Split split1a1 = createWeightedSplit(1, STANDARD_WEIGHT, "host1:8080");
        Split split2a2 = createWeightedSplit(2, STANDARD_WEIGHT, "host2:8080");
        Split split3a1 = createWeightedSplit(3, STANDARD_WEIGHT, "host1:8080");
        Split split3a12 = createWeightedSplit(3, STANDARD_WEIGHT, "host1:8080", "host2:8080");
        Split heavySplit2a2 = createWeightedSplit(12, heavyWeight, "host2:8080");
        Split lightSplit1a1 = createWeightedSplit(21, lightWeight, "host1:8080");

        taskSource = createSourceDistributionTaskSource(
                ImmutableList.of(split1a1, heavySplit2a2, split3a1, lightSplit1a1),
                ImmutableListMultimap.of(),
                1, // single split per batch for predictable results
                0,
                2 * STANDARD_WEIGHT,
                3);

        tasks = readAllTasks(taskSource);
        assertThat(tasks).hasSize(3);
        assertThat(tasks).allMatch(task -> getOnlyElement(task.getSplits().keySet()).equals(PLAN_NODE_1));
        assertThat(tasks.get(0).getSplits().values()).containsExactlyInAnyOrder(heavySplit2a2);
        assertThat(tasks.get(1).getSplits().values()).containsExactlyInAnyOrder(split1a1, split3a1);
        assertThat(tasks.get(2).getSplits().values()).containsExactlyInAnyOrder(lightSplit1a1);
        assertTrue(taskSource.isFinished());

        // with addresses with multiple matching
        taskSource = createSourceDistributionTaskSource(
                ImmutableList.of(split1a1, split3a12, split2a2),
                ImmutableListMultimap.of(),
                1, // single split per batch for predictable results
                0,
                2 * STANDARD_WEIGHT,
                3);

        tasks = readAllTasks(taskSource);
        assertThat(tasks).hasSize(2);
        assertThat(tasks).allMatch(task -> getOnlyElement(task.getSplits().keySet()).equals(PLAN_NODE_1));
        assertThat(tasks.get(0).getSplits().values()).containsExactlyInAnyOrder(split1a1, split3a12);
        assertThat(tasks.get(1).getSplits().values()).containsExactlyInAnyOrder(split2a2);
        assertTrue(taskSource.isFinished());
    }

    @Test
    public void testSourceDistributionTaskSourceLastIncompleteTaskAlwaysCreated()
    {
        for (int targetSplitsPerTask = 1; targetSplitsPerTask <= 21; targetSplitsPerTask++) {
            List<Split> splits = new ArrayList<>();
            for (int i = 0; i < targetSplitsPerTask + 1 /* to make last task incomplete with only a single split */; i++) {
                splits.add(createWeightedSplit(i, STANDARD_WEIGHT));
            }
            for (int finishDelayIterations = 1; finishDelayIterations < 20; finishDelayIterations++) {
                for (int splitBatchSize = 1; splitBatchSize <= 5; splitBatchSize++) {
                    TaskSource taskSource = createSourceDistributionTaskSource(
                            new TestingSplitSource(TEST_CATALOG_HANDLE, splits, finishDelayIterations),
                            ImmutableListMultimap.of(),
                            splitBatchSize,
                            targetSplitsPerTask,
                            STANDARD_WEIGHT * targetSplitsPerTask,
                            targetSplitsPerTask);
                    List<TaskDescriptor> tasks = readAllTasks(taskSource);
                    assertThat(tasks).hasSize(2);
                    TaskDescriptor lastTask = findLast(tasks.stream()).orElseThrow();
                    assertThat(lastTask.getSplits()).hasSize(1);
                }
            }
        }
    }

    @Test
    public void testSourceDistributionTaskSourceWithAsyncSplitSource()
    {
        SettableFuture<List<Split>> splitsFuture = SettableFuture.create();
        TaskSource taskSource = createSourceDistributionTaskSource(
                new TestingSplitSource(TEST_CATALOG_HANDLE, splitsFuture, 0),
                ImmutableListMultimap.of(),
                2,
                0,
                2 * STANDARD_WEIGHT,
                1000);
        ListenableFuture<List<TaskDescriptor>> tasksFuture = taskSource.getMoreTasks();
        assertThat(tasksFuture).isNotDone();

        splitsFuture.set(ImmutableList.of(createSplit(1), createSplit(2), createSplit(3)));
        List<TaskDescriptor> tasks = getDone(tasksFuture);
        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0).getSplits()).hasSize(2);

        tasksFuture = taskSource.getMoreTasks();
        assertThat(tasksFuture).isDone();
        tasks = getDone(tasksFuture);
        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0).getSplits()).hasSize(1);
        assertThat(taskSource.isFinished()).isTrue();
    }

    @Test
    public void testHashDistributionTaskSourceWithAsyncSplitSource()
    {
        SettableFuture<List<Split>> splitsFuture1 = SettableFuture.create();
        SettableFuture<List<Split>> splitsFuture2 = SettableFuture.create();
        TaskSource taskSource = createHashDistributionTaskSource(
                ImmutableMap.of(
                        PLAN_NODE_1, new TestingSplitSource(TEST_CATALOG_HANDLE, splitsFuture1, 0),
                        PLAN_NODE_2, new TestingSplitSource(TEST_CATALOG_HANDLE, splitsFuture2, 0)),
                ImmutableListMultimap.of(),
                ImmutableListMultimap.of(
                        PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)),
                1,
                createPartitioningScheme(4, 4),
                0,
                DataSize.of(0, BYTE));
        ListenableFuture<List<TaskDescriptor>> tasksFuture = taskSource.getMoreTasks();
        assertThat(tasksFuture).isNotDone();

        Split bucketedSplit1 = createBucketedSplit(0, 0);
        Split bucketedSplit2 = createBucketedSplit(0, 2);
        Split bucketedSplit3 = createBucketedSplit(0, 3);
        splitsFuture1.set(ImmutableList.of(bucketedSplit1, bucketedSplit2, bucketedSplit3));
        assertThat(tasksFuture).isNotDone();

        Split bucketedSplit4 = createBucketedSplit(0, 1);
        splitsFuture2.set(ImmutableList.of(bucketedSplit4));
        List<TaskDescriptor> tasks = getDone(tasksFuture);
        assertThat(tasks).hasSize(4);
        tasks.forEach(task -> assertThat(task.getSplits()).hasSize(2));
        assertThat(taskSource.isFinished()).isTrue();
    }

    private static SourceDistributionTaskSource createSourceDistributionTaskSource(
            List<Split> splits,
            ListMultimap<PlanNodeId, ExchangeSourceHandle> replicatedSources,
            int splitBatchSize,
            int minSplitsPerTask,
            long splitWeightPerTask,
            int maxSplitsPerTask)
    {
        return createSourceDistributionTaskSource(
                new TestingSplitSource(TEST_CATALOG_HANDLE, splits),
                replicatedSources,
                splitBatchSize,
                minSplitsPerTask,
                splitWeightPerTask,
                maxSplitsPerTask);
    }

    private static SourceDistributionTaskSource createSourceDistributionTaskSource(
            SplitSource splitSource,
            ListMultimap<PlanNodeId, ExchangeSourceHandle> replicatedSources,
            int splitBatchSize,
            int minSplitsPerTask,
            long splitWeightPerTask,
            int maxSplitsPerTask)
    {
        return new SourceDistributionTaskSource(
                new QueryId("query"),
                PLAN_NODE_1,
                new TableExecuteContextManager(),
                splitSource,
                createRemoteSplits(replicatedSources),
                splitBatchSize,
                getSplitsTime -> {},
                Optional.of(TEST_CATALOG_HANDLE),
                minSplitsPerTask,
                splitWeightPerTask,
                maxSplitsPerTask,
                directExecutor());
    }

    private static Split createSplit(int id, String... addresses)
    {
        return new Split(TEST_CATALOG_HANDLE, new TestingConnectorSplit(id, OptionalInt.empty(), addressesList(addresses)));
    }

    private static Split createWeightedSplit(int id, long weight, String... addresses)
    {
        return new Split(TEST_CATALOG_HANDLE, new TestingConnectorSplit(id, OptionalInt.empty(), addressesList(addresses), weight));
    }

    private static Split createBucketedSplit(int id, int bucket)
    {
        return new Split(TEST_CATALOG_HANDLE, new TestingConnectorSplit(id, OptionalInt.of(bucket), Optional.empty()));
    }

    private List<TaskDescriptor> readAllTasks(TaskSource taskSource)
    {
        ImmutableList.Builder<TaskDescriptor> tasks = ImmutableList.builder();
        while (!taskSource.isFinished()) {
            tasks.addAll(getFutureValue(taskSource.getMoreTasks()));
        }
        return tasks.build();
    }

    private ListMultimap<PlanNodeId, Split> flattenSplits(List<TaskDescriptor> tasks)
    {
        return tasks.stream()
                .flatMap(taskDescriptor -> taskDescriptor.getSplits().entries().stream())
                .collect(toImmutableListMultimap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Optional<List<HostAddress>> addressesList(String... addresses)
    {
        requireNonNull(addresses, "addresses is null");
        if (addresses.length == 0) {
            return Optional.empty();
        }
        return Optional.of(Arrays.stream(addresses)
                .map(HostAddress::fromString)
                .collect(toImmutableList()));
    }

    private static FaultTolerantPartitioningScheme createPartitioningScheme(int partitionCount)
    {
        return new FaultTolerantPartitioningScheme(
                partitionCount,
                Optional.of(IntStream.range(0, partitionCount).toArray()),
                Optional.empty(),
                Optional.empty());
    }

    private static FaultTolerantPartitioningScheme createPartitioningScheme(int partitionCount, int bucketCount)
    {
        int[] bucketToPartitionMap = new int[bucketCount];
        for (int i = 0; i < bucketCount; i++) {
            bucketToPartitionMap[i] = i % partitionCount;
        }
        return new FaultTolerantPartitioningScheme(
                partitionCount,
                Optional.of(bucketToPartitionMap),
                Optional.of(split -> ((TestingConnectorSplit) split.getConnectorSplit()).getBucket().orElseThrow()),
                Optional.of(nCopies(partitionCount, new InternalNode("local", URI.create("local://" + NODE_ADDRESS), NodeVersion.UNKNOWN, true))));
    }

    private static ListMultimap<PlanNodeId, ExchangeSourceHandle> extractSourceHandles(ListMultimap<PlanNodeId, Split> splits)
    {
        ImmutableListMultimap.Builder<PlanNodeId, ExchangeSourceHandle> result = ImmutableListMultimap.builder();
        splits.forEach(((planNodeId, split) -> {
            if (split.getCatalogHandle().equals(REMOTE_CATALOG_HANDLE)) {
                RemoteSplit remoteSplit = (RemoteSplit) split.getConnectorSplit();
                SpoolingExchangeInput input = (SpoolingExchangeInput) remoteSplit.getExchangeInput();
                result.putAll(planNodeId, input.getExchangeSourceHandles());
            }
        }));
        return result.build();
    }

    private static ListMultimap<PlanNodeId, Split> extractCatalogSplits(ListMultimap<PlanNodeId, Split> splits)
    {
        ImmutableListMultimap.Builder<PlanNodeId, Split> result = ImmutableListMultimap.builder();
        splits.forEach((planNodeId, split) -> {
            if (!split.getCatalogHandle().equals(REMOTE_CATALOG_HANDLE)) {
                result.put(planNodeId, split);
            }
        });
        return result.build();
    }

    private static class TestingConnectorSplit
            implements ConnectorSplit
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(TestingConnectorSplit.class).instanceSize();

        private final int id;
        private final OptionalInt bucket;
        private final Optional<List<HostAddress>> addresses;
        private final SplitWeight weight;

        public TestingConnectorSplit(int id, OptionalInt bucket, Optional<List<HostAddress>> addresses)
        {
            this(id, bucket, addresses, SplitWeight.standard().getRawValue());
        }

        public TestingConnectorSplit(int id, OptionalInt bucket, Optional<List<HostAddress>> addresses, long weight)
        {
            this.id = id;
            this.bucket = requireNonNull(bucket, "bucket is null");
            this.addresses = addresses.map(ImmutableList::copyOf);
            this.weight = SplitWeight.fromRawValue(weight);
        }

        public int getId()
        {
            return id;
        }

        public OptionalInt getBucket()
        {
            return bucket;
        }

        @Override
        public boolean isRemotelyAccessible()
        {
            return addresses.isEmpty();
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            return addresses.orElse(ImmutableList.of());
        }

        @Override
        public SplitWeight getSplitWeight()
        {
            return weight;
        }

        @Override
        public Object getInfo()
        {
            return null;
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE
                    + sizeOf(bucket)
                    + sizeOf(addresses, value -> estimatedSizeOf(value, HostAddress::getRetainedSizeInBytes));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestingConnectorSplit that = (TestingConnectorSplit) o;
            return id == that.id && weight == that.weight && Objects.equals(bucket, that.bucket) && Objects.equals(addresses, that.addresses);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, bucket, addresses, weight);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("id", id)
                    .add("bucket", bucket)
                    .add("addresses", addresses)
                    .add("weight", weight)
                    .toString();
        }
    }
}
