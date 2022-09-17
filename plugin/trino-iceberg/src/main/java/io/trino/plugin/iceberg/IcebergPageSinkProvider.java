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
package io.trino.plugin.iceberg;

import io.airlift.json.JsonCodec;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.iceberg.procedure.IcebergOptimizeHandle;
import io.trino.plugin.iceberg.procedure.IcebergTableExecuteHandle;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.io.LocationProvider;

import javax.inject.Inject;

import java.util.Map;

import static com.google.common.collect.Maps.transformValues;
import static io.trino.plugin.iceberg.IcebergUtil.getLocationProvider;
import static java.util.Objects.requireNonNull;

public class IcebergPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final PageIndexerFactory pageIndexerFactory;
    private final int maxOpenPartitions;

    @Inject
    public IcebergPageSinkProvider(
            TrinoFileSystemFactory fileSystemFactory,
            JsonCodec<CommitTaskData> jsonCodec,
            IcebergFileWriterFactory fileWriterFactory,
            PageIndexerFactory pageIndexerFactory,
            IcebergConfig config)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        this.maxOpenPartitions = config.getMaxPartitionsPerWriter();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        return createPageSink(session, (IcebergWritableTableHandle) outputTableHandle);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        return createPageSink(session, (IcebergWritableTableHandle) insertTableHandle);
    }

    private ConnectorPageSink createPageSink(ConnectorSession session, IcebergWritableTableHandle tableHandle)
    {
        Schema schema = SchemaParser.fromJson(tableHandle.getSchemaAsJson());
        String partitionSpecJson = tableHandle.getPartitionsSpecsAsJson().get(tableHandle.getPartitionSpecId());
        PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, partitionSpecJson);
        LocationProvider locationProvider = getLocationProvider(tableHandle.getName(), tableHandle.getOutputPath(), tableHandle.getStorageProperties());
        return new IcebergPageSink(
                schema,
                partitionSpec,
                locationProvider,
                fileWriterFactory,
                pageIndexerFactory,
                fileSystemFactory.create(session),
                tableHandle.getInputColumns(),
                jsonCodec,
                session,
                tableHandle.getFileFormat(),
                tableHandle.getStorageProperties(),
                maxOpenPartitions);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        IcebergTableExecuteHandle executeHandle = (IcebergTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.getProcedureId()) {
            case OPTIMIZE:
                IcebergOptimizeHandle optimizeHandle = (IcebergOptimizeHandle) executeHandle.getProcedureHandle();
                Schema schema = SchemaParser.fromJson(optimizeHandle.getSchemaAsJson());
                PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, optimizeHandle.getPartitionSpecAsJson());
                LocationProvider locationProvider = getLocationProvider(executeHandle.getSchemaTableName(),
                        executeHandle.getTableLocation(), optimizeHandle.getTableStorageProperties());
                return new IcebergPageSink(
                        schema,
                        partitionSpec,
                        locationProvider,
                        fileWriterFactory,
                        pageIndexerFactory,
                        fileSystemFactory.create(session),
                        optimizeHandle.getTableColumns(),
                        jsonCodec,
                        session,
                        optimizeHandle.getFileFormat(),
                        optimizeHandle.getTableStorageProperties(),
                        maxOpenPartitions);
            case DROP_EXTENDED_STATS:
            case EXPIRE_SNAPSHOTS:
            case REMOVE_ORPHAN_FILES:
                // handled via ConnectorMetadata.executeTableExecute
        }
        throw new IllegalArgumentException("Unknown procedure: " + executeHandle.getProcedureId());
    }

    @Override
    public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle mergeHandle)
    {
        IcebergMergeTableHandle merge = (IcebergMergeTableHandle) mergeHandle;
        IcebergWritableTableHandle tableHandle = merge.getInsertTableHandle();
        LocationProvider locationProvider = getLocationProvider(tableHandle.getName(), tableHandle.getOutputPath(), tableHandle.getStorageProperties());
        Schema schema = SchemaParser.fromJson(tableHandle.getSchemaAsJson());
        Map<Integer, PartitionSpec> partitionsSpecs = transformValues(tableHandle.getPartitionsSpecsAsJson(), json -> PartitionSpecParser.fromJson(schema, json));
        ConnectorPageSink pageSink = createPageSink(session, tableHandle);

        return new IcebergMergeSink(
                locationProvider,
                fileWriterFactory,
                fileSystemFactory.create(session),
                jsonCodec,
                session,
                tableHandle.getFileFormat(),
                tableHandle.getStorageProperties(),
                schema,
                partitionsSpecs,
                pageSink,
                tableHandle.getInputColumns().size());
    }
}
