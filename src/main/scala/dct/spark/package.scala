package dct

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{Metadata, StructField, StructType}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

package object spark {
  class PGSQLException(val message: String, val cause: Throwable)
    extends Exception(message, cause)

  val MAX_PARALLELISM = 8

  //TODO move this to application.conf
  System.setProperty("spark.app.name", "data copy tool")
  System.setProperty("spark.master", "local")
  System.setProperty("spark.ui.enabled", "false")
  System.setProperty("spark.eventLog.enabled", "false")
  System.setProperty("spark.driver.memory", "64m")
  System.setProperty("spark.executor.memory", "512m")
  System.setProperty("spark.sql.codegen.wholeStage","false")
  lazy val spark: SparkSession =
    SparkSession.builder().config(new SparkConf(true)).getOrCreate()

  implicit val sparkSchemaOrdering: Ordering[StructField] =
    (x: StructField, y: StructField) => {
      if (x.name == y.name) 0
      else if (x.name < y.name) -1
      else 1
    }

  implicit class StructTypeExtra(self: StructType) {
    def !===(other: StructType): Boolean = {
      StructType(self.map(_.copy(metadata = Metadata.empty))) !=
      StructType(other.map(_.copy(metadata = Metadata.empty)))
    }
  }

  def hadoopDirExists(path: String): Boolean = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)
    try fs.exists(new Path(path))
    finally fs.close()
  }
}
