package dct.json

import dct.spark.Logger
import java.io.InputStreamReader
import java.io.BufferedReader
import org.json4s.jackson.JsonMethods.parse
import org.json4s.DefaultFormats
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types._

/**
 * ADF json mapping file deserializer and converter to [[StructType]]
 * @param path hdfs path to mapping file
 */

//TODO get read of extension of Logger, make it through val
case class ADFMapping(path: String)
  extends Logger{

  implicit val jsonFormats: DefaultFormats.type = DefaultFormats

  private def content: String = {
    val hdfs = FileSystem.get(new Configuration())
    val fileStream = hdfs.open(new Path(path))
    val linesStream = new BufferedReader(new InputStreamReader(fileStream)).lines()
    try linesStream.toArray.foldLeft("")((op, a) => op + a)
    finally linesStream.close()
  }

  private lazy val adfMap: List[Map[String, Map[String, String]]] = {
    val jValue = parse(content)
    try {
      val jsonList = jValue.extract[List[Map[String, Map[String, String]]]]
      assert(jsonList.forall(m =>
          m.keySet.contains("sink") &&
          m.keySet.contains("source") &&
          m("sink").keySet.contains("name") &&
          m("source").keySet.contains("name")))
      jsonList
    } catch {
      case _: Exception =>
        logWarning(
          s""" Failed to parse ADF mapping json.
             | Please ensure mapping structure is correct: $content
             | """.stripMargin)
        List.empty[Map[String, Map[String, String]]]
    }
  }

  val aliasMap: Map[String, String] =
    adfMap.map(m => (m("source")("name"), m("sink")("name"))).toMap

  val mappingSchema: Option[StructType] =
    if (adfMap.isEmpty)
      None
    else {
      Some(
        StructType(
          adfMap.map{
            elem =>
              val sinkType =
                if (elem("sink").keySet.contains("type"))
                  elem("sink")("type")
                else
                  "String"
              StructField(
                elem("source")("name"),
                sinkType match {
                  case "Decimal" => DecimalType(DecimalType.MAX_PRECISION, DecimalType.MAX_SCALE)
                  case "Boolean" => BooleanType
                  case "Byte[]" => ByteType
                  case "DateTime" => TimestampType
                  case "Double" => DoubleType
                  case "Guid" => StringType //TODO validate this type mapping after migration
                  case "Int32" => IntegerType
                  case "Int64" => LongType
                  case "Single" => StringType //TODO validate this type mapping after migration
                  case "String" => StringType
                  case t =>
                    logWarning(
                      s"""Unknown ADF sink column type $t.
                         |Spark StringType will be applied in sink schema.
                         |""".stripMargin)
                    StringType
                }
              )
          }
        )
      )
    }

}
