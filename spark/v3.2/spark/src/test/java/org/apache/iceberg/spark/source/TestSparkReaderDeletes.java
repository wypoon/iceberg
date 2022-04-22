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
import java.util.List;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkStructLike;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSparkReaderDeletes extends SparkDeleteReadTestsBase {

  public TestSparkReaderDeletes(String format, boolean vectorized) {
    super(format, vectorized);
  }

  @Parameterized.Parameters(name = "format = {0}, vectorized = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] {"parquet", false},
        new Object[] {"parquet", true}
    };
  }

  @Override
  public StructLikeSet rowSet(String name, Table table, String... columns) throws IOException {
    Dataset<Row> df = spark.read()
        .format("iceberg")
        .load(TableIdentifier.of("default", name).toString())
        .selectExpr(columns);

    Types.StructType projection = table.schema().select(columns).asStruct();
    StructLikeSet set = StructLikeSet.create(projection);
    df.collectAsList().forEach(row -> {
      SparkStructLike rowWrapper = new SparkStructLike(projection);
      set.add(rowWrapper.wrap(row));
    });

    return set;
  }

  @Test
  public void testEqualityDeleteWithFilter() throws IOException {
    String tableName = table.name().substring(table.name().lastIndexOf(".") + 1);
    Schema deleteRowSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes = Lists.newArrayList(
        dataDelete.copy("data", "a"), // id = 29
        dataDelete.copy("data", "d"), // id = 89
        dataDelete.copy("data", "g") // id = 122
    );

    DeleteFile eqDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), TestHelpers.Row.of(0), dataDeletes, deleteRowSchema);

    table.newRowDelta()
        .addDeletes(eqDeletes)
        .commit();

    Types.StructType projection = table.schema().select("*").asStruct();
    Dataset<Row> df = spark.read()
        .format("iceberg")
        .load(TableIdentifier.of("default", tableName).toString())
        .filter("data = 'a'") // select a deleted row
        .selectExpr("*");

    StructLikeSet actual = StructLikeSet.create(projection);
    df.collectAsList().forEach(row -> {
      SparkStructLike rowWrapper = new SparkStructLike(projection);
      actual.add(rowWrapper.wrap(row));
    });

    Assert.assertEquals("Table should contain no rows", 0, actual.size());
  }

  @Test
  public void testReadEqualityDeleteRows() throws IOException {
    Schema deleteSchema1 = table.schema().select("data");
    Record dataDelete = GenericRecord.create(deleteSchema1);
    List<Record> dataDeletes = Lists.newArrayList(
        dataDelete.copy("data", "a"), // id = 29
        dataDelete.copy("data", "d") // id = 89
    );

    Schema deleteSchema2 = table.schema().select("id");
    Record idDelete = GenericRecord.create(deleteSchema2);
    List<Record> idDeletes = Lists.newArrayList(
        idDelete.copy("id", 121), // id = 121
        idDelete.copy("id", 122) // id = 122
    );

    DeleteFile eqDelete1 = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), TestHelpers.Row.of(0), dataDeletes, deleteSchema1);

    DeleteFile eqDelete2 = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), TestHelpers.Row.of(0), idDeletes, deleteSchema2);

    table.newRowDelta()
        .addDeletes(eqDelete1)
        .addDeletes(eqDelete2)
        .commit();

    StructLikeSet expectedRowSet = rowSetWithIds(29, 89, 121, 122);

    Types.StructType type = table.schema().asStruct();
    StructLikeSet actualRowSet = StructLikeSet.create(type);

    CloseableIterable<CombinedScanTask> tasks = TableScanUtil.planTasks(
        table.newScan().planFiles(),
        TableProperties.METADATA_SPLIT_SIZE_DEFAULT,
        TableProperties.SPLIT_LOOKBACK_DEFAULT,
        TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);

    for (CombinedScanTask task : tasks) {
      try (EqualityDeleteRowReader reader = new EqualityDeleteRowReader(task, table, table.schema(), false)) {
        while (reader.next()) {
          actualRowSet.add(new InternalRowWrapper(SparkSchemaUtil.convert(table.schema())).wrap(reader.get().copy()));
        }
      }
    }

    Assert.assertEquals("should include 4 deleted row", 4, actualRowSet.size());
    Assert.assertEquals("deleted row should be matched", expectedRowSet, actualRowSet);
  }

  @Test
  public void testPosDeletesAllRowsInBatch() throws IOException {
    // read.parquet.vectorization.batch-size is set to 4, so the 4 rows in the first batch are all deleted.
    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList(
        Pair.of(dataFile.path(), 0L), // id = 29
        Pair.of(dataFile.path(), 1L), // id = 43
        Pair.of(dataFile.path(), 2L), // id = 61
        Pair.of(dataFile.path(), 3L) // id = 89
    );

    Pair<DeleteFile, CharSequenceSet> posDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), TestHelpers.Row.of(0), deletes);

    table.newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    StructLikeSet expected = rowSetWithoutIds(table, records, 29, 43, 61, 89);
    StructLikeSet actual = rowSet(tableName, table, "*");

    Assert.assertEquals("Table should contain expected rows", expected, actual);
    long expectedDeletes = 4L;
    checkDeleteCount(expectedDeletes);
  }
}
