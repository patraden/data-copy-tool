package dct.akkastream

import akka.stream.alpakka.slick.scaladsl.SlickSession
import akka.stream.scaladsl.{Keep, RunnableGraph}
import dct.slick.ConnectionProvider
import dct.spark.SparkPGSQLUtils.{createTable, getSchemaOption, tableExists, truncateTable}
import org.apache.spark.sql.types.StructType

import java.sql.Connection
import scala.concurrent.Future

case class PGCopyTableStream(oldTableName: String, newTableName: String)
                            (implicit val session: SlickSession) {

  private implicit val conn: Connection = {
    val tryConn = ConnectionProvider().acquireBase()
    if (tryConn.isSuccess) tryConn.get
    else throw tryConn.failed.get
  }

  def buildStream(): RunnableGraph[Future[Long]] = {
    val schemaOpt: Option[StructType] = getSchemaOption(oldTableName)
    if (!tableExists(newTableName)) createTable(newTableName, schemaOpt)
    else truncateTable(newTableName)
    PGCopySource(oldTableName).
      toMat(PGCopySink(newTableName, schemaOpt.get))(Keep.right)
  }
}