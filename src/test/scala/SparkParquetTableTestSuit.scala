package dct.spark

import dct.json.ADFMapping
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.net.{URLDecoder => decoder}

class SparkParquetTableTestSuit extends AnyFunSuite with BeforeAndAfterAll {
  @transient var testParquetFiles: Array[String] = _
  @transient var testMapFiles: Array[String] = _
  @transient var smallSparkTable: SparkParquetTable = _
  @transient var largeSparkTable: SparkParquetTable = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val testResourceRoot = decoder.
      decode(getClass.getResource("/").
        getPath, "UTF-8")

    // keeping it out of project resources due to size
    val largeParquetFilePath =
      "C:\\Users\\patraden\\OneDrive - Mars Inc\\Documents\\DevProjects\\" +
        "TANDER_SALES_COMPETITORS.large.parquet"

    testParquetFiles = new java.io.File(testResourceRoot).
      listFiles.
      filter(f => f.isFile && f.getPath.endsWith(".parquet")).
      map(_.getPath) :+ largeParquetFilePath

    testMapFiles = new java.io.File(testResourceRoot).
      listFiles.
      filter(f => f.isFile && f.getPath.endsWith(".json")).
      map(_.getPath)
  }

  test ("test user schema application") {

    val tableName = "TANDER_SALES_TOTAL_FACTS"
    val paths = testParquetFiles.
      filter(p => p.contains(tableName) && p.contains("small")).
      toSeq

    val mappingSchemaOpt = Option(StructType(Seq(
      StructField("DAY", TimestampType,nullable = true),
      StructField("SELLOUT_ITEM", DecimalType(20,0),nullable = true),
      StructField("SELLOUT_RSV", DecimalType(20,2),nullable = true))))

    val fileSchemaOpt = Option(StructType(Seq(
      StructField("__SKU_HASH_ROW__", DecimalType(20,0),nullable = true),
      StructField("__POS_HASH_ROW__", DecimalType(20,0),nullable = true),
      StructField("__SKU_ID__", DecimalType(20,0),nullable = true),
      StructField("__POS_ID__", DecimalType(20,0),nullable = true),
      StructField("__PRICE_ID__", DecimalType(20,0),nullable = true),
      StructField("__HASH_ROW__", DecimalType(20,0),nullable = true),
      StructField("DAY", TimestampType,nullable = true),
      StructField("SELLOUT_ITEM", DecimalType(20,0),nullable = true),
      StructField("SELLOUT_RSV", DecimalType(20,2),nullable = true),
      StructField("__LATEST_UPDATE__", TimestampType,nullable = true),
      StructField("__INGEST_TS__", TimestampType,nullable = true))))

    val withoutMappingSchemaTable = new SparkParquetTable(tableName, paths)
    val withMappingSchemaTable = new SparkParquetTable(tableName, paths, mappingSchemaOpt)

    assertResult(fileSchemaOpt.get)(withoutMappingSchemaTable.schema)
    assertResult(mappingSchemaOpt.get)(withMappingSchemaTable.schema)
  } // end of test

  test("test reader") {

    val tableName = "DICTIONARY_POS"
    val paths = testParquetFiles.
      filter(p => p.contains(tableName) && p.contains("small")).
      toSeq

    val mappingSchemaOpt = Option(StructType(Seq(
      StructField("POS_NAME", StringType,nullable = true),
      StructField("POS_CODE", StringType,nullable = true),
      StructField("DC", StringType,nullable = true),
      StructField("Outlets_GLN", StringType,nullable = true))))

    val withMappingSchemaTable = new SparkParquetTable(tableName, paths, mappingSchemaOpt)
    val withoutMappingSchemaTable = new SparkParquetTable(tableName, paths)

    val readerWithMapping = withMappingSchemaTable.rowReader(withMappingSchemaTable.splitFiles.head)
    val readerWithoutMapping = withoutMappingSchemaTable.rowReader(withoutMappingSchemaTable.splitFiles.head)

    if (readerWithMapping.next()) {
      val row = readerWithMapping.get().toSeq
      val rowStr = row.mkString(",")
      val rowSize = row.length
      assertResult("Жмакин,400063,РЦ Тула,null")(rowStr)
      assertResult(4)(rowSize)
    }

    if (readerWithoutMapping.next()) {
      val row = readerWithoutMapping.get().toSeq
      val rowSize = row.length
      assertResult(33)(rowSize)
    }
  } // end of test

  test ("apply schema from mapping file") {

    val tableName = "DICTIONARY_POS"
    val mapFilePath = testMapFiles.filter(_.contains(tableName)).head
    val adfMap = ADFMapping(mapFilePath).mappingSchema

    val parquetFilePath = testParquetFiles.
      filter(p => p.contains(tableName) && p.contains("small")).
      toSeq

    val parquetTable = new SparkParquetTable(tableName, parquetFilePath, adfMap)
    val reader = parquetTable.rowReader(parquetTable.splitFiles.head)

    if (reader.next()) {
      val row = reader.get().toSeq
      assert(adfMap.get.size != parquetTable.inferredSchema.get.size)
      assertResult(adfMap.get.size)(row.length)
    }
    reader.close()


  } // end of test

  test ("metadata tests") {

    val largeTableName = "TANDER_SALES_COMPETITORS"
    val largeTablePaths = testParquetFiles.
      filter(p => p.contains(largeTableName) && p.contains("large")).
      toSeq
    largeSparkTable = new SparkParquetTable(largeTableName, largeTablePaths)
    assertResult(4)(largeSparkTable.parallelism)
    assertResult(6561046L)(largeSparkTable.totalRowsCount)

    val smallTableName = "DICTIONARY_POS"
    val smallTablePaths = testParquetFiles.
      filter(p => p.contains(smallTableName) && p.contains("small")).
      toSeq
    smallSparkTable = new SparkParquetTable(smallTableName, smallTablePaths)
    assertResult(1)(smallSparkTable.parallelism)
    assertResult(14261)(smallSparkTable.totalRowsCount)

  } // end of test

}