package dct.spark

import java.sql.Connection
import scala.util.{Failure, Success}
import akka.stream.alpakka.slick.scaladsl.SlickSession
import dct.slick.{ConnectionProvider, defaultDBConfig}
import dct.spark.SparkPGSQLUtils._
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class SparkPGSQLUtilsTestSuit
  extends AnyFunSuite with BeforeAndAfterAll {

  @transient implicit var connection: Connection = _
  implicit val session: SlickSession = SlickSession.forConfig(defaultDBConfig)
  @transient val connProvider: ConnectionProvider = ConnectionProvider()
  @transient val oldSchemaName: String = "public"
  @transient val oldTableName: String = "dct_test_table"
  @transient val newSchemaName: String = "dct_test"
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
    dropTable(oldSchemaName + "." + oldTableName)
    dropTable(newSchemaName + "." + newTableName)
  }

  test("create table") {
    createTable(oldSchemaName + "." + oldTableName, Option(schema))
  }

  test("change table schema") {
    changeTableSchema(oldTableName, oldSchemaName, newSchemaName)
  }

  ignore("insert table into table") {
    insertIntoTable(
      "test.tander_sales_competitors",
      "test.tander_sales_competitors_copy")
  }

  test("get table schema") {
    val schemaOpt = getSchemaOption(newSchemaName + "." + oldTableName)
    val derivedSchema = schemaOpt.getOrElse(new StructType())
    assert(!(schema !=== derivedSchema))
  }

  test("rename table") {
    renameTable(newSchemaName + "." + oldTableName, newTableName)
    assertResult(true)(tableExists(newSchemaName + "." + newTableName))
    assertResult(false)(tableExists(newSchemaName + "." + oldTableName))
  }

  test("truncate table") {
    truncateTable(newSchemaName + "." + newTableName)
  }

}
