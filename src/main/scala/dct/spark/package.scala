package dct

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{Metadata, StructField, StructType}

package object spark {
  /**
   * Spark utility trait for classes that want to log data. Creates a SLF4J logger for the class and allows
   * logging messages at different levels using methods that only evaluate parameters lazily if the
   * log level is enabled.
   */
  trait Logger extends org.apache.spark.internal.Logging

  /**
   * application.conf from dct package.
   */
  val appConfig: Config = ConfigFactory.load().getConfig("dct")

  /**
   * Spark configuration with minimal settings as
   * dct does not use spark cluster but re-uses some of spark internal classes and objects.
   */
  val sparkConf = new SparkConf(false)
  ( "spark.app.name" ::
    "spark.master" ::
    "spark.ui.enabled" ::
    "spark.eventLog.enabled" ::
    "spark.driver.memory" ::
    "spark.executor.memory" ::
    "spark.sql.codegen.wholeStage" ::
    Nil
    ).foreach( key =>
    sparkConf.set(key, appConfig.getString(key))
  )

  lazy val spark: SparkSession =
    SparkSession.builder().config(sparkConf).getOrCreate()

  /**
   * Parquet read parallelism restricts concurrent parquet row groups load into memory (heap).
   * Thus this parameter should be adjusted inline with available heap size.
   * Assuming that parquet.block.size has it is default value of 256Mb the total heap size dedicated to
   * Data Copy Tool should be determined throw this approximate formula:
   * Max heap size (-Xmx) = (256Mb * MAX_PARQUET_READ_PARALLELISM) * 1.2
   */
  val MazParquetReadParallelism: Int =
    appConfig.getInt("parquet.maxReadParallelism")

  /**
   * Ordering function for spark schema.
   */
  implicit val sparkSchemaOrdering: Ordering[StructField] =
    (x: StructField, y: StructField) => {
      if (x.name == y.name) 0
      else if (x.name < y.name) -1
      else 1
    }

  /**
   * Compares two StructTypes in terms of name, datatype and nullable flags equality.
   * Standard [[StructType.equals()]] does not work as it also compares metadata.
   */
  implicit class StructTypeExtra(self: StructType) {
    def !===(other: StructType): Boolean = {
      StructType(self.map(_.copy(metadata = Metadata.empty))) !=
      StructType(other.map(_.copy(metadata = Metadata.empty)))
    }
  }

  /**
   * Hadoop file/dir resource existence validator.
   * @param path hadoop file/dir path.
   */
  def hadoopDirExists(path: String): Boolean = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)
    try fs.exists(new Path(path))
    finally fs.close()
  }
}
