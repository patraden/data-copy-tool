package dct.cli

import dct.CLIConfig
import org.scalatest.funsuite.AnyFunSuite

import java.net.{URLDecoder => decoder}

class CLIParsingTestSuit extends AnyFunSuite{

  val testResourceRoot: String =
    decoder.decode(getClass.getResource("/").getPath, "UTF-8")

  @transient val testMapFiles: Array[String] =
    new java.io.File(testResourceRoot).
      listFiles.
      filter(f => f.isFile && f.getPath.endsWith(".json")).
      map(_.getPath)

  @transient val testParquetFiles: Array[String] =
    new java.io.File(testResourceRoot).
      listFiles.
      filter(f => f.isFile && f.getPath.endsWith(".parquet")).
      map(_.getPath)

  test("all args validated") {
    val testRes = CLIConfig(Seq(
      "-m", "append",
      "--table", "test.tableName",
      "--parquet", testParquetFiles.head,
      "--mapping", testMapFiles.head,
      "--url", "jdbc.url"
    ))

    val expectedRes = CLIConfig(
      "append",
      "test.tableName",
      testParquetFiles.head,
      Some(testMapFiles.head),
      Some("jdbc.url")
    )
    assertResult(expectedRes)(testRes)
  }

  test("invalid file paths") {
    assertResult(null)(
      CLIConfig(Seq(
        "-m", "append",
        "--table", "test.tableName",
        "--parquet", "\\d",
        "--mapping", "\\f",
        "--url", "jdbc.url"))
    )
  }

  test("display help") {
    CLIConfig(Seq("-help"))
  }


}
