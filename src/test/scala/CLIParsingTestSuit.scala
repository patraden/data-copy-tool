package dct.cli

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.net.{URLDecoder => decoder}

class CLIParsingTestSuit
    extends AnyFunSuite with BeforeAndAfterAll{

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

  test("all args validated") {
    args = Seq(
      "-m", "append",
      "--table", "test.tableName",
      "--parquet", testParquetFiles.head,
      "--mapping", testMapFiles.head,
      "--url", "jdbc.url"
    )

    val testRes = CLIConfig(args)
    val expectedRes = CLIConfig(
      "append",
      "test.tableName",
      "C:\\Users\\patraden\\OneDrive - Mars Inc\\Documents\\DevProjects\\copytops\\target\\scala-2.13\\test-classes\\TANDER_DICTIONARIES_UPDATE_SYNAPSE_COPY_DICTIONARY_POS.small.snappy.parquet",
      Some("C:\\Users\\patraden\\OneDrive - Mars Inc\\Documents\\DevProjects\\copytops\\target\\scala-2.13\\test-classes\\MAPPING_CHICAGO_MODELLING_PIPE_UPLOAD_MONITOTIG_DETAIL_BASE_ACT.json"),
      Some("jdbc.url")
    )

    assertResult(expectedRes)(testRes)
  }

  test("not valid file paths") {
    args = Seq(
      "-m", "append",
      "--table", "test.tableName",
      "--parquet", "/dir1",
      "--mapping", "/file2",
      "--url", "jdbc.url"
    )
    assertThrows[Exception](CLIConfig(args))
  }

  test("display help") {
    CLIConfig(Seq("-help"))
  }


}
