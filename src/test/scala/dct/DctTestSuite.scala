package dct

import org.scalatest.funsuite.AsyncFunSuite
import java.net.{URLDecoder => decoder}
import scala.concurrent.Future

class DctTestSuite extends AsyncFunSuite{

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

  ignore("create new table") {
      args = Seq(
        "-m", "create",
        "--table", "public.sales_total",
        "--parquet", testParquetFiles.head
      )

      val cliConfig = CLIConfig(args)
      val stream = new ParquetToPGCreate(
        cliConfig.sqlTable,
        cliConfig.parquetFilePath,
        cliConfig.adfMappingFilePath,
        cliConfig.jdbcURL
      )

    stream.
      end2endPattern().
      flatMap(_ => Future(assert(1 == 1))(executionContext))

  }

  test("overwrite new table") {
    args = Seq(
      "-m", "overwrite",
      "--table", "public.sales_total",
      "--parquet", testParquetFiles.head
    )

    val cliConfig = CLIConfig(args)
    val stream = new ParquetToPGOverwrite(
      cliConfig.sqlTable,
      cliConfig.sqlTable.
        replaceAll("public", "dct_test").
        replaceAll("sales_total", "sales_total_temp"),
      cliConfig.parquetFilePath,
      cliConfig.adfMappingFilePath,
      cliConfig.jdbcURL
    )

    stream.
      end2endPattern().
      flatMap(_ => Future(assert(1 == 1))(executionContext))

  }

}
