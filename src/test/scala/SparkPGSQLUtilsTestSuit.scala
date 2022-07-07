package dct.spark

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest._

import java.sql.Connection
import dct.slick.{ConnectionProvider, defaultDBConfig}
import org.apache.spark.sql.types._
import SparkPGSQLUtils._
import akka.stream.alpakka.slick.scaladsl.SlickSession

import scala.util.{Failure, Success}

class SparkPGSQLUtilsTestSuit
  extends AnyFunSuite with BeforeAndAfterAll {

  @transient implicit var connection: Connection = _
  implicit val session: SlickSession = SlickSession.forConfig(defaultDBConfig)
  @transient val connProvider: ConnectionProvider = ConnectionProvider()
  @transient val schemaName: String = "test"
  @transient val newSchemaName: String = "public"
  @transient val tableName: String = "dct_test_table"
  @transient val newTableName: String = "dct_test_table_renamed"
  @transient val schema: StructType =
    StructType(
      StructField("Decimal", DecimalType.SYSTEM_DEFAULT) ::
      StructField("String", StringType) ::
      StructField("Boolean", BooleanType) ::
      StructField("Bynary", BinaryType) ::
      StructField("Short", ShortType) ::
      StructField("Integer", IntegerType) ::
      StructField("Long", LongType) ::
      StructField("Float", FloatType) ::
      StructField("Double", DoubleType) ::
      StructField("Array", ArrayType(StringType)) ::
      StructField("Timestamp", TimestampType) ::
      StructField("Date", DateType) :: Nil
    )

  override def beforeAll(): Unit = {
    super.beforeAll()
    connProvider.acquireBase() match {
      case Success(conn) => connection = conn
      case Failure(ex) => throw new NullPointerException(s"""
           |Failed to establish connection to DB due to:
           |${ex.getMessage}
           |""".stripMargin)
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    connProvider.release(None)
  }

  test("drop table") {
    dropTable(schemaName + "." + tableName)
    dropTable(newSchemaName + "." + newTableName)
  }

  test("create table") {
    createTable(schemaName + "." + tableName, Option(schema))
  }

  ignore("change table schema") {
    changeTableSchema(tableName, schemaName, newSchemaName)
    dropTable(newSchemaName + "." + tableName)
  }

  ignore("insert table into table") {
    insertIntoTable(
      "test.tander_sales_competitors",
      "test.tander_sales_competitors_copy")
  }

  ignore("get table schema") {
    val schemaOpt = getSchemaOption(schemaName + "." + tableName)
    val derivedSchema = schemaOpt.getOrElse(new StructType())
    assert(!(schema !=== derivedSchema))
  }

  ignore("rename table") {
    renameTable(tableName, newTableName)
    assertResult(true)(tableExists(newTableName))
    assertResult(false)(tableExists(tableName))
  }

  ignore("truncate table") {
    truncateTable(newTableName)
  }

}
