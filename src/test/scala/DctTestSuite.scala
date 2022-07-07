package dct

import akka.stream.alpakka.slick.scaladsl.SlickSession
import dct.cli.CLIConfig
import dct.slick.defaultDBConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.{AnyFunSuite, AsyncFunSuite}

import java.net.{URLDecoder => decoder}
import scala.concurrent.Future

class DctTestSuite extends AnyFunSuite with BeforeAndAfterAll {

  val testResourceRoot: String = decoder.decode(getClass.getResource("/").getPath, "UTF-8")

  @transient val testMapFiles: Array[String] = new java.io.File(testResourceRoot).
    listFiles.
    filter(f => f.isFile && f.getPath.endsWith(".json")).
    map(_.getPath)

  @transient val testParquetFiles: Array[String] = new java.io.File(testResourceRoot).
    listFiles.
    filter(f => f.isFile && f.getPath.endsWith(".parquet")).
    map(_.getPath)

  @transient var args: Seq[String] = _

  test("create pattern") {
    args = Seq(
      "-m", "create",
      "--table", "test.pos_dictionary",
      "--parquet", testParquetFiles.head
    )
    val cliConfig = CLIConfig(args)
    implicit val session: SlickSession = SlickSession.forConfig(defaultDBConfig)
    println(testParquetFiles.head)
    ParquetToPGCopyCreate(
      cliConfig.sqlTable,
      cliConfig.parquetFilePath,
      cliConfig.adfMappingFilePath,
      cliConfig.jdbcURL
    ).runStreaming()
  }

}
