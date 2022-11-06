package dct.spark

import org.postgresql.PGConnection
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.nio.charset.StandardCharsets
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.postgresql.copy.CopyOut
import dct.slick.PGConnectionProvider


class PostgresSparkStreamingTestSuit extends AnyFunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  ignore("practicing with Java api for PGConn") {
    val connection: PGConnection = PGConnectionProvider("dct.postgres").acquire().get
    val columnNames = Seq("raw")
    val tableName = "public.\"1c_validation\""
    val query = s"""COPY (SELECT ${columnNames.mkString(",")} FROM $tableName) TO STDOUT"""
    val copyOut: CopyOut = connection.getCopyAPI.copyOut(query)
    for (_ <- 1 to 5) {
      val input = new String(copyOut.readFromCopy(), StandardCharsets.UTF_8)
      println(input.split("\t").toSeq)
    }
  }

  test ("custom spark receiver") {
    val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(1))
    val columnNames = Seq("server_name", "server_ip", "address_type", "platform")
    val tableName = "public.\"servers_ip\""
    val query = s"""COPY (SELECT ${columnNames.mkString(",")} FROM $tableName) TO STDOUT"""
    val customReceiverStream = ssc.receiverStream(new PGSQLReceiver(query, PGConnectionProvider("dct.postgres")))
    customReceiverStream.count().print()
    ssc.start()
    ssc.awaitTerminationOrTimeout(5000L)
  }
}
