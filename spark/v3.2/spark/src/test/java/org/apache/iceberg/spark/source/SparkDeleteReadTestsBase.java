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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.DeleteReadTests;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

public abstract class SparkDeleteReadTestsBase extends DeleteReadTests {

  protected static TestHiveMetastore metastore = null;
  protected static SparkSession spark = null;
  protected static HiveCatalog catalog = null;
  protected final String format;
  protected final boolean vectorized;

  public SparkDeleteReadTestsBase(String format, boolean vectorized) {
    this.format = format;
    this.vectorized = vectorized;
  }

  @BeforeClass
  public static void startMetastoreAndSpark() {
    metastore = new TestHiveMetastore();
    metastore.start();
    HiveConf hiveConf = metastore.hiveConf();

    spark = SparkSession.builder()
        .master("local[2]")
        .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
        .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
        .enableHiveSupport()
        .getOrCreate();

    catalog = (HiveCatalog)
        CatalogUtil.loadCatalog(HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);

    try {
      catalog.createNamespace(Namespace.of("default"));
    } catch (AlreadyExistsException ignored) {
      // the default namespace already exists. ignore the create error
    }
  }

  @AfterClass
  public static void stopMetastoreAndSpark() throws Exception {
    catalog = null;
    metastore.stop();
    metastore = null;
    spark.stop();
    spark = null;
  }

  @Override
  protected Table createTable(String name, Schema schema, PartitionSpec spec) {
    Table table = catalog.createTable(TableIdentifier.of("default", name), schema);
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));
    table.updateProperties()
        .set(TableProperties.DEFAULT_FILE_FORMAT, format)
        .commit();
    if (format.equals("parquet") && vectorized) {
      table.updateProperties()
          .set(TableProperties.PARQUET_VECTORIZATION_ENABLED, "true")
          .set(TableProperties.PARQUET_BATCH_SIZE, "4") // split 7 records to two batches to cover more code paths
          .commit();
    }
    return table;
  }

  @Override
  protected void dropTable(String name) {
    catalog.dropTable(TableIdentifier.of("default", name));
  }

}
