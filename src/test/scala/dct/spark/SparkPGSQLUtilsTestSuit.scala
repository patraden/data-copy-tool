package dct.spark

import java.sql.Connection
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
  @transient implicit val connProvider: ConnectionProvider = ConnectionProvider()
  @transient val oldSchemaName: String = "public"
  @transient val oldTableName: String = "dct_test_table"
  @transient val fullOldTableName: String = oldSchemaName + "." + oldTableName
  @transient val newSchemaName: String = "dct_test"
  @transient val newTableName: String = "dct_test_table_renamed"
  @transient val fullNewTableName: String = newSchemaName + "." + newTableName
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

  override def afterAll(): Unit = {
    super.afterAll()
    session.close()
  }

  test("drop table") {
    dropTable(fullOldTableName)
    dropTable(fullNewTableName)
  }

  test("create table") {
    createTable(fullOldTableName, Option(schema))
  }

  test("change table schema") {
    changeTableSchema(fullOldTableName, newSchemaName)
  }

  test("get table schema") {
    val derivedSchema = getSchema(newSchemaName + "." + oldTableName)
    assert(!(schema !=== derivedSchema))
  }

  test("rename table") {
    renameTable(newSchemaName + "." + oldTableName, newTableName)
    assertResult(true)(tableExists(fullNewTableName))
    assertResult(false)(tableExists(newSchemaName + "." + oldTableName))
  }

  test("truncate table") {
    truncateTable(fullNewTableName)
  }

}
