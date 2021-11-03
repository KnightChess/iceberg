/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.StrictMetricsEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException;
import org.apache.spark.sql.catalyst.analysis.PartitionAlreadyExistsException;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsDelete;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.catalog.SupportsPartitionManagement;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.iceberg.catalog.SupportsMerge;
import org.apache.spark.sql.connector.iceberg.write.MergeBuilder;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.DELETE_MODE;
import static org.apache.iceberg.TableProperties.DELETE_MODE_DEFAULT;
import static org.apache.iceberg.TableProperties.MERGE_MODE;
import static org.apache.iceberg.TableProperties.MERGE_MODE_DEFAULT;
import static org.apache.iceberg.TableProperties.UPDATE_MODE;
import static org.apache.iceberg.TableProperties.UPDATE_MODE_DEFAULT;

public class SparkTable implements org.apache.spark.sql.connector.catalog.Table,
    SupportsRead, SupportsWrite, SupportsDelete, SupportsMerge, SupportsMetadataColumns, SupportsPartitionManagement {

  private static final Logger LOG = LoggerFactory.getLogger(SparkTable.class);

  private static final Set<String> RESERVED_PROPERTIES =
      ImmutableSet.of("provider", "format", "current-snapshot-id", "location", "sort-order");
  private static final Set<TableCapability> CAPABILITIES = ImmutableSet.of(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.MICRO_BATCH_READ,
      TableCapability.STREAMING_WRITE,
      TableCapability.OVERWRITE_BY_FILTER,
      TableCapability.OVERWRITE_DYNAMIC);

  private final Table icebergTable;
  private final StructType requestedSchema;
  private final boolean refreshEagerly;
  private StructType lazyTableSchema = null;
  private SparkSession lazySpark = null;

  public SparkTable(Table icebergTable, boolean refreshEagerly) {
    this(icebergTable, null, refreshEagerly);
  }

  public SparkTable(Table icebergTable, StructType requestedSchema, boolean refreshEagerly) {
    this.icebergTable = icebergTable;
    this.requestedSchema = requestedSchema;
    this.refreshEagerly = refreshEagerly;

    if (requestedSchema != null) {
      // convert the requested schema to throw an exception if any requested fields are unknown
      SparkSchemaUtil.convert(icebergTable.schema(), requestedSchema);
    }
  }

  private SparkSession sparkSession() {
    if (lazySpark == null) {
      this.lazySpark = SparkSession.active();
    }

    return lazySpark;
  }

  public Table table() {
    return icebergTable;
  }

  @Override
  public String name() {
    return icebergTable.toString();
  }

  @Override
  public StructType schema() {
    if (lazyTableSchema == null) {
      if (requestedSchema != null) {
        this.lazyTableSchema = SparkSchemaUtil.convert(SparkSchemaUtil.prune(icebergTable.schema(), requestedSchema));
      } else {
        this.lazyTableSchema = SparkSchemaUtil.convert(icebergTable.schema());
      }
    }

    return lazyTableSchema;
  }

  @Override
  public Transform[] partitioning() {
    return Spark3Util.toTransforms(icebergTable.spec());
  }

  @Override
  public Map<String, String> properties() {
    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();

    String fileFormat = icebergTable.properties()
        .getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    propsBuilder.put("format", "iceberg/" + fileFormat);
    propsBuilder.put("provider", "iceberg");
    String currentSnapshotId = icebergTable.currentSnapshot() != null ?
        String.valueOf(icebergTable.currentSnapshot().snapshotId()) : "none";
    propsBuilder.put("current-snapshot-id", currentSnapshotId);
    propsBuilder.put("location", icebergTable.location());

    if (!icebergTable.sortOrder().isUnsorted()) {
      propsBuilder.put("sort-order", Spark3Util.describe(icebergTable.sortOrder()));
    }

    icebergTable.properties().entrySet().stream()
        .filter(entry -> !RESERVED_PROPERTIES.contains(entry.getKey()))
        .forEach(propsBuilder::put);

    return propsBuilder.build();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }

  @Override
  public MetadataColumn[] metadataColumns() {
    DataType sparkPartitionType = SparkSchemaUtil.convert(Partitioning.partitionType(table()));
    return new MetadataColumn[] {
        new SparkMetadataColumn(MetadataColumns.SPEC_ID.name(), DataTypes.IntegerType, false),
        new SparkMetadataColumn(MetadataColumns.PARTITION_COLUMN_NAME, sparkPartitionType, true),
        new SparkMetadataColumn(MetadataColumns.FILE_PATH.name(), DataTypes.StringType, false),
        new SparkMetadataColumn(MetadataColumns.ROW_POSITION.name(), DataTypes.LongType, false)
    };
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    if (options.containsKey(SparkReadOptions.FILE_SCAN_TASK_SET_ID)) {
      // skip planning the job and fetch already staged file scan tasks
      return new SparkFilesScanBuilder(sparkSession(), icebergTable, options);
    }

    if (refreshEagerly) {
      icebergTable.refresh();
    }

    SparkScanBuilder scanBuilder = new SparkScanBuilder(sparkSession(), icebergTable, options);

    if (requestedSchema != null) {
      scanBuilder.pruneColumns(requestedSchema);
    }

    return scanBuilder;
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    if (info.options().containsKey(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID)) {
      // replace data files in the given file scan task set with new files
      return new SparkRewriteBuilder(sparkSession(), icebergTable, info);
    } else {
      return new SparkWriteBuilder(sparkSession(), icebergTable, info);
    }
  }

  @Override
  public MergeBuilder newMergeBuilder(String operation, LogicalWriteInfo info) {
    String mode = getRowLevelOperationMode(operation);
    ValidationException.check(mode.equals("copy-on-write"), "Unsupported mode for %s: %s", operation, mode);
    return new SparkMergeBuilder(sparkSession(), icebergTable, operation, info);
  }

  private String getRowLevelOperationMode(String operation) {
    Map<String, String> props = icebergTable.properties();
    if (operation.equalsIgnoreCase("delete")) {
      return props.getOrDefault(DELETE_MODE, DELETE_MODE_DEFAULT);
    } else if (operation.equalsIgnoreCase("update")) {
      return props.getOrDefault(UPDATE_MODE, UPDATE_MODE_DEFAULT);
    } else if (operation.equalsIgnoreCase("merge")) {
      return props.getOrDefault(MERGE_MODE, MERGE_MODE_DEFAULT);
    } else {
      throw new IllegalArgumentException("Unsupported operation: " + operation);
    }
  }

  @Override
  public boolean canDeleteWhere(Filter[] filters) {
    Expression deleteExpr = Expressions.alwaysTrue();

    for (Filter filter : filters) {
      Expression expr = SparkFilters.convert(filter);
      if (expr != null) {
        deleteExpr = Expressions.and(deleteExpr, expr);
      } else {
        return false;
      }
    }

    return deleteExpr == Expressions.alwaysTrue() || canDeleteUsingMetadata(deleteExpr);
  }

  // a metadata delete is possible iff matching files can be deleted entirely
  private boolean canDeleteUsingMetadata(Expression deleteExpr) {
    boolean caseSensitive = Boolean.parseBoolean(sparkSession().conf().get("spark.sql.caseSensitive"));
    TableScan scan = table().newScan()
        .filter(deleteExpr)
        .caseSensitive(caseSensitive)
        .includeColumnStats()
        .ignoreResiduals();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Map<Integer, Evaluator> evaluators = Maps.newHashMap();
      StrictMetricsEvaluator metricsEvaluator = new StrictMetricsEvaluator(table().schema(), deleteExpr);

      return Iterables.all(tasks, task -> {
        DataFile file = task.file();
        PartitionSpec spec = task.spec();
        Evaluator evaluator = evaluators.computeIfAbsent(
            spec.specId(),
            specId -> new Evaluator(spec.partitionType(), Projections.strict(spec).project(deleteExpr)));
        return evaluator.eval(file.partition()) || metricsEvaluator.eval(file);
      });

    } catch (IOException ioe) {
      LOG.warn("Failed to close task iterable", ioe);
      return false;
    }
  }

  @Override
  public void deleteWhere(Filter[] filters) {
    Expression deleteExpr = SparkFilters.convert(filters);

    if (deleteExpr == Expressions.alwaysFalse()) {
      LOG.info("Skipping the delete operation as the condition is always false");
      return;
    }

    try {
      icebergTable.newDelete()
          .set("spark.app.id", sparkSession().sparkContext().applicationId())
          .deleteFromRowFilter(deleteExpr)
          .commit();
    } catch (ValidationException e) {
      throw new IllegalArgumentException("Failed to cleanly delete data files matching: " + deleteExpr, e);
    }
  }

  @Override
  public StructType partitionSchema() {
    List<Types.NestedField> fields = icebergTable.spec().partitionType().fields();
    StructField[] structFields = new StructField[fields.size()];
    int index = 0;
    for (Types.NestedField field : fields) {
      StructField structField = new StructField(field.name(), SparkSchemaUtil.convert(field.type()), true,
          Metadata.empty());
      structFields[index] = structField;
      ++index;
    }
    return new StructType(structFields);
  }

  @Override
  public void createPartition(InternalRow ident, Map<String, String> properties)
      throws PartitionAlreadyExistsException, UnsupportedOperationException {
    throw new UnsupportedOperationException("not support create partition, use addFile procedure to refresh");
  }

  @Override
  public boolean dropPartition(InternalRow ident) {
    throw new UnsupportedOperationException("not support drop partition, use delete sql instead of it");
  }

  @Override
  public void replacePartitionMetadata(InternalRow ident, Map<String, String> properties)
      throws NoSuchPartitionException, UnsupportedOperationException {
    throw new UnsupportedOperationException("not support replace partition metadata");
  }

  @Override
  public Map<String, String> loadPartitionMetadata(InternalRow ident) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("not support load partition metadata");
  }

  @Override
  public InternalRow[] listPartitionIdentifiers(String[] names, InternalRow ident) {
    Table table = this.table();

    // get possition by partition name
    StructType partitionSchema = this.partitionSchema();
    StructField[] partitionFileds = partitionSchema.fields();
    Map<String, Integer> fieldToPosition = new HashMap<>(8);
    for (int i = 0; i < partitionFileds.length; i++) {
      fieldToPosition.put(partitionFileds[i].name(), i);
    }

    Set<InternalRow> set = Sets.newHashSet();
    GenericInternalRow filterPartitionRow = new GenericInternalRow(names.length);
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {

      // filer ident
      for (DataFile dataFile : CloseableIterable.transform(tasks, FileScanTask::file)) {
        StructLike structLike = dataFile.partition();
        for (int i = 0; i < names.length; ++i) {
          Integer indexId = fieldToPosition.get(names[i]);
          Object value = structLike.get(indexId, Object.class);
          if (value instanceof String) {
            filterPartitionRow.update(i, UTF8String.fromString((String) value));
          } else {
            filterPartitionRow.update(i, value);
          }
        }

        // match ident: none or equal
        if (names.length < 1 || filterPartitionRow.equals(ident)) {
          InternalRow internalRow = constructPartitionRow(structLike);
          set.add(internalRow);
        }
      }
    } catch (IOException e) {
      LOG.error("listPartitionIdentifiers error", e);
    }
    return set.toArray(new InternalRow[0]);
  }

  private InternalRow constructPartitionRow(StructLike structLike) {
    GenericInternalRow partitionRow = new GenericInternalRow(structLike.size());
    for (int i = 0; i < structLike.size(); i++) {
      Object value = structLike.get(i, Object.class);
      if (value instanceof String) {
        partitionRow.update(i, UTF8String.fromString((String) value));
      } else {
        partitionRow.update(i, value);
      }
    }
    return partitionRow;
  }

  @Override
  public String toString() {
    return icebergTable.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    // use only name in order to correctly invalidate Spark cache
    SparkTable that = (SparkTable) other;
    return icebergTable.name().equals(that.icebergTable.name());
  }

  @Override
  public int hashCode() {
    // use only name in order to correctly invalidate Spark cache
    return icebergTable.name().hashCode();
  }
}
