package dct.json

import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest._

import java.net.{URLDecoder => decoder}

class JsonADFMappingConverterTestSuit extends AnyFunSuite with BeforeAndAfterAll {

  @transient var testMapFiles: Array[String] = _
  @transient var mappingSchema: Option[StructType] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val testResourceRoot = decoder.
      decode(getClass.getResource("/").
      getPath, "UTF-8")

    testMapFiles = new java.io.File(testResourceRoot).
      listFiles.
      filter(f => f.isFile && f.getPath.endsWith(".json")).
      map(_.getPath)
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("deserialize all adf map files") {
    assertResult(41)(testMapFiles.map(ADFMapping(_).mappingSchema).length)
  }

  test("print map file") {
    val mapPath = testMapFiles.filter(_.contains("DICTIONARY_POS")).head
    val adfMap = ADFMapping(mapPath)
    val schema = adfMap.mappingSchema.get
    val aliases = adfMap.aliasMap
    aliases.foreach(println)
  }

}
