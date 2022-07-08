package dct.akkastream

import akka.stream.alpakka.slick.javadsl.SlickSession
import dct.slick.{ConnectionProvider, defaultDBConfig}
import dct.spark.SparkPGSQLUtils._
import dct.spark.SparkParquetTable
import org.scalatest._
import org.scalatest.funsuite.AsyncFunSuite

import java.net.{URLDecoder => decoder}
import java.sql.Connection
import scala.concurrent.Future

class SparkParquetTableSourceTestSuit
  extends AsyncFunSuite with BeforeAndAfterAll {

  implicit val session: SlickSession = SlickSession.forConfig(defaultDBConfig)
  @transient implicit val provider: ConnectionProvider = ConnectionProvider()
  @transient implicit var conn: Connection = _
  @transient var testParquetFiles: Array[String] = _
  @transient var smallSparkTable: SparkParquetTable = _
  @transient var sparkTable: SparkParquetTable = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    conn = provider.acquireBase().get

    val testResourceRoot = decoder.
      decode(getClass.getResource("/").
        getPath, "UTF-8")

    // Keep it aside if size is substantial
    val largeParquetFilePath = "/Users/patraden/Downloads/TANDER_SALES_COMPETITORS.large.parquet"

    testParquetFiles = new java.io.File(testResourceRoot).
      listFiles.
      filter(f => f.isFile && f.getPath.endsWith(".parquet")).
      map(_.getPath) :+ largeParquetFilePath
  }

  override def afterAll(): Unit = {
    super.afterAll()
    provider.release(None)
  }

  test("copy small file") {
    val schema = "public"
    val table = "tander_dictionaries"
    val tableName = schema + "." + table
    val paths = testParquetFiles.filter(p => p.contains(table.toUpperCase)).toSeq

    if (paths.length == 1)
      sparkTable = new SparkParquetTable(tableName, paths)

    if (!tableExists(tableName))
      createTable(tableName, Option(sparkTable.schema))
    else
      truncateTable(tableName)

    Future.sequence(
      SparkParquetTableToPGCopyStream(sparkTable).buildStreams.flatMap(_.run())
    ).map(seq => assert(seq.sum == 14261L))
  }

  ignore("copy large file") {
    val schema = "public"
    val table = "tander_sales_competitors"
    val tableName = schema + "." + table
    val paths = testParquetFiles.filter(p => p.contains(table.toUpperCase)).toSeq

    if (paths.length == 1)
      sparkTable = new SparkParquetTable(tableName, paths)

    if (!tableExists(tableName))
      createTable(tableName, Option(sparkTable.schema))
    else
      truncateTable(tableName)

    Future.sequence(
      SparkParquetTableToPGCopyStream(sparkTable).buildStreams.flatMap(_.run())
    ).map(seq => assert(seq.sum == 6561046L))

  }

  ignore("Copy small table within a db") {
    PGCopyTableStream("public.tander_dictionaries", "dct_test.tander_dictionaries_copy").
      buildStream().run().map(res => assert(res == 14261L))
  }

  ignore("Copy large table within a db") {
    PGCopyTableStream("public.tander_sales_competitors", "dct_test.tander_sales_competitors_copy").
      buildStream().run().map(res => assert(res == 6561046L))
  }
}
