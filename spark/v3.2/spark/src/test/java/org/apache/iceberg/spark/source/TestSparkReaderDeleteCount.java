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
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSparkReaderDeleteCount extends SparkDeleteReadTestsBase {

  private long deleteCount;

  public TestSparkReaderDeleteCount(String format, boolean vectorized) {
    super(format, vectorized);
  }

  @Parameterized.Parameters(name = "format = {0}, vectorized = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] {"parquet", true},
        new Object[] {"orc", false},
        new Object[] {"avro", false}
    };
  }

  @Override
  protected boolean countDeletes() {
    return true;
  }

  private void setDeleteCount(long count) {
    deleteCount = count;
  }

  @Override
  protected long deleteCount() {
    return deleteCount;
  }

  @Override
  public StructLikeSet rowSet(String name, Table table, String... columns) throws IOException {
    Schema schema = table.schema().select(columns);
    StructType sparkSchema = SparkSchemaUtil.convert(schema);
    Types.StructType type = schema.asStruct();
    StructLikeSet set = StructLikeSet.create(type);

    SparkScanBuilder scanBuilder = new SparkScanBuilder(spark, table, CaseInsensitiveStringMap.empty());
    scanBuilder.pruneColumns(sparkSchema);
    SparkScan scan = (SparkScan) scanBuilder.buildMergeOnReadScan();
    List<CombinedScanTask> tasks = scan.tasks();

    long delCount = 0L;
    for (CombinedScanTask task : tasks) {
      if (format.equals("parquet") && vectorized) {
        try (BatchDataReader reader = new BatchDataReader(task, table, schema, false, 4)) {
          while (reader.next()) {
            ColumnarBatch columnarBatch = reader.get();
            Iterator<InternalRow> iter = columnarBatch.rowIterator();
            while (iter.hasNext()) {
              set.add(new InternalRowWrapper(sparkSchema).wrap(iter.next().copy()));
            }
          }
          delCount += (reader.counter().get());
        }
      } else {
        try (RowDataReader reader = new RowDataReader(task, table, schema, false)) {
          while (reader.next()) {
            set.add(new InternalRowWrapper(sparkSchema).wrap(reader.get().copy()));
          }
          delCount += (reader.counter().get());
        }
      }
    }
    setDeleteCount(delCount);

    return set;
  }

}
