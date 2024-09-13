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

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ChangelogUtil;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.DeletedRowsScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.JoinedRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.unsafe.types.UTF8String;

class ChangelogRowReader extends BaseRowReader<ChangelogScanTask>
    implements PartitionReader<InternalRow> {

  private NestedField[] columns;
  private DataType[] sparkColumnTypes;

  ChangelogRowReader(SparkInputPartition partition) {
    this(
        partition.table(),
        partition.taskGroup(),
        SnapshotUtil.schemaFor(partition.table(), partition.branch()),
        partition.expectedSchema(),
        partition.isCaseSensitive());
  }

  ChangelogRowReader(
      Table table,
      ScanTaskGroup<ChangelogScanTask> taskGroup,
      Schema tableSchema,
      Schema expectedSchema,
      boolean caseSensitive) {
    super(
        table,
        taskGroup,
        tableSchema,
        ChangelogUtil.dropChangelogMetadata(expectedSchema),
        caseSensitive);
    columns = expectedSchema().columns().stream().toArray(NestedField[]::new);
    sparkColumnTypes = computeColumnTypes();
  }

  private DataType[] computeColumnTypes() {
    DataType[] types = new DataType[columns.length];

    for (int i = 0; i < columns.length; i++) {
      types[i] = SparkSchemaUtil.convert(columns[i].type());
    }

    return types;
  }

  // rowSchema is expected to contain all the columns of the expected schema
  private int[] indexesInRow(Schema rowSchema) {
    int[] indexes = new int[columns.length];
    NestedField[] columnsInRow = rowSchema.columns().stream().toArray(NestedField[]::new);

    for (int i = 0; i < columns.length; i++) {
      NestedField column = columns[i];
      for (int j = 0; j < columnsInRow.length; j++) {
        if (column.equals(columnsInRow[j])) {
          indexes[i] = j;
          break;
        }
      }
    }

    return indexes;
  }

  @Override
  protected CloseableIterator<InternalRow> open(ChangelogScanTask task) {
    JoinedRow cdcRow = new JoinedRow();

    cdcRow.withRight(changelogMetadata(task));

    CloseableIterable<InternalRow> rows = openChangelogScanTask(task);
    CloseableIterable<InternalRow> cdcRows = CloseableIterable.transform(rows, cdcRow::withLeft);

    return cdcRows.iterator();
  }

  private static InternalRow changelogMetadata(ChangelogScanTask task) {
    InternalRow metadataRow = new GenericInternalRow(3);

    metadataRow.update(0, UTF8String.fromString(task.operation().name()));
    metadataRow.update(1, task.changeOrdinal());
    metadataRow.update(2, task.commitSnapshotId());

    return metadataRow;
  }

  private CloseableIterable<InternalRow> openChangelogScanTask(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return openAddedRowsScanTask((AddedRowsScanTask) task);

    } else if (task instanceof DeletedRowsScanTask) {
      return openDeletedRowsScanTask((DeletedRowsScanTask) task);

    } else if (task instanceof DeletedDataFileScanTask) {
      return openDeletedDataFileScanTask((DeletedDataFileScanTask) task);

    } else {
      throw new IllegalArgumentException(
          "Unsupported changelog scan task type: " + task.getClass().getName());
    }
  }

  private InternalRow projectRow(InternalRow row, int[] indexes) {
    InternalRow expectedRow = new GenericInternalRow(columns.length);

    for (int i = 0; i < columns.length; i++) {
      expectedRow.update(i, row.get(indexes[i], sparkColumnTypes[i]));
    }

    return expectedRow;
  }

  private CloseableIterable<InternalRow> openAddedRowsScanTask(AddedRowsScanTask task) {
    String filePath = task.file().path().toString();
    SparkDeleteFilter deletes = new SparkDeleteFilter(filePath, task.deletes(), counter());
    int[] indexes = indexesInRow(deletes.requiredSchema());

    return CloseableIterable.transform(
        deletes.filter(rows(task, deletes.requiredSchema())), row -> projectRow(row, indexes));
  }

  private CloseableIterable<InternalRow> openDeletedDataFileScanTask(DeletedDataFileScanTask task) {
    String filePath = task.file().path().toString();
    SparkDeleteFilter deletes = new SparkDeleteFilter(filePath, task.existingDeletes(), counter());
    int[] indexes = indexesInRow(deletes.requiredSchema());

    return CloseableIterable.transform(
        deletes.filter(rows(task, deletes.requiredSchema())), row -> projectRow(row, indexes));
  }

  private CloseableIterable<InternalRow> openDeletedRowsScanTask(DeletedRowsScanTask task) {
    String filePath = task.file().path().toString();
    SparkDeleteFilter existingDeletes =
        new SparkDeleteFilter(filePath, task.existingDeletes(), counter());
    SparkDeleteFilter newDeletes = new SparkDeleteFilter(filePath, task.addedDeletes(), counter());
    Schema requiredSchema =
        TypeUtil.join(existingDeletes.requiredSchema(), newDeletes.requiredSchema());
    int[] indexes = indexesInRow(requiredSchema);

    return CloseableIterable.transform(
        // first, apply the existing deletes and get the rows remaining
        // then, see what rows are deleted by applying the new deletes
        newDeletes.filterDeleted(existingDeletes.filter(rows(task, requiredSchema))),
        row -> projectRow(row, indexes));
  }

  private CloseableIterable<InternalRow> rows(ContentScanTask<DataFile> task, Schema readSchema) {
    Map<Integer, ?> idToConstant = constantsMap(task, readSchema);

    String filePath = task.file().path().toString();

    // update the current file for Spark's filename() function
    InputFileBlockHolder.set(filePath, task.start(), task.length());

    InputFile location = getInputFile(filePath);
    Preconditions.checkNotNull(location, "Could not find InputFile");
    return newIterable(
        location,
        task.file().format(),
        task.start(),
        task.length(),
        task.residual(),
        readSchema,
        idToConstant);
  }

  @Override
  protected Stream<ContentFile<?>> referencedFiles(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return addedRowsScanTaskFiles((AddedRowsScanTask) task);

    } else if (task instanceof DeletedRowsScanTask) {
      return deletedRowsScanTaskFiles((DeletedRowsScanTask) task);

    } else if (task instanceof DeletedDataFileScanTask) {
      return deletedDataFileScanTaskFiles((DeletedDataFileScanTask) task);

    } else {
      throw new IllegalArgumentException(
          "Unsupported changelog scan task type: " + task.getClass().getName());
    }
  }

  private static Stream<ContentFile<?>> deletedDataFileScanTaskFiles(DeletedDataFileScanTask task) {
    DataFile file = task.file();
    List<DeleteFile> existingDeletes = task.existingDeletes();
    return Stream.concat(Stream.of(file), existingDeletes.stream());
  }

  private static Stream<ContentFile<?>> addedRowsScanTaskFiles(AddedRowsScanTask task) {
    DataFile file = task.file();
    List<DeleteFile> deletes = task.deletes();
    return Stream.concat(Stream.of(file), deletes.stream());
  }

  private static Stream<ContentFile<?>> deletedRowsScanTaskFiles(DeletedRowsScanTask task) {
    DataFile file = task.file();
    List<DeleteFile> deletes = task.addedDeletes();
    return Stream.concat(Stream.of(file), deletes.stream());
  }
}
