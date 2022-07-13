package dct.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus

import org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.hadoop.ParquetInputFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetReadSupport}
import org.apache.spark.sql.execution.datasources.v2.parquet.{ParquetPartitionReaderFactory, ParquetScan, ParquetTable}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.execution.datasources.parquet.ParquetFooterReader

/**
 * Parquet read support class as extension of standard [[ParquetTable]].
 * Allows to get full parquet schema as [[StructType]] including partitioning.
 * Besides provides useful methods to split parquet files equally
 * and read them in parallel via spark [[PartitionReader]]
 * @param tableName alias name for the instance of parquet table.
 * @param paths hdfs paths to parquet files.
 * @param userSpecifiedSchema optional user schema.
 * @param options standard spark parquet read options.
 */

class SparkParquetTable(
     val tableName: String,
     override val paths: Seq[String],
     override val userSpecifiedSchema: Option[StructType] = None,
     override val options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty()) extends ParquetTable(
  tableName,
  spark,
  options,
  paths,
  userSpecifiedSchema,
  classOf[ParquetFileFormat]) {

  override lazy val dataSchema: StructType = {
    val schema = userSpecifiedSchema.map { schema =>
      val partitionSchema = fileIndex.partitionSchema
      val resolver = sparkSession.sessionState.conf.resolver
      val infSchema: StructType = inferredSchema.getOrElse(new StructType())

      val validSchema = StructType(
        schema.
          filter(f => infSchema.exists(i => resolver(i.name, f.name))).
          map { f =>
            val i = infSchema.find(_.name == f.name).get
            if (i.dataType == f.dataType) f else i
          }.filterNot(f => partitionSchema.exists(p => resolver(p.name, f.name)))
      )
      if (validSchema.isEmpty) infSchema else validSchema
    }.orElse {
      inferSchema(fileIndex.allFiles())
    }.getOrElse {
      throw QueryCompilationErrors.dataSchemaNotSpecifiedError(formatName)
    }
    schema
  }

  def parallelism: Int = MAX_PARQUET_READ_PARALLELISM min
    parquetBlocksMetaData.
      groupBy { case (status, _) => status}.
      map { case (_, v) => v.length }.iterator.max

  def totalRowsCount: Long =
    parquetBlocksMetaData.
      foldLeft(0L){ case (op, (_, block)) => op + block.getRowCount}

  private val parquetScan =
    newScanBuilder(options).build().asInstanceOf[ParquetScan]

  private val readerFactory =
    parquetScan.createReaderFactory().asInstanceOf[ParquetPartitionReaderFactory]

  val splitFiles: Seq[PartitionedFile] = fileIndex
    .listFiles(parquetScan.partitionFilters, parquetScan.dataFilters)
    .flatMap { partition =>
      partition.files.flatMap { file =>
        val splitBytes = file.getLen / parallelism + parallelism
        val filePath = file.getPath
        PartitionedFileUtil.splitFiles(
          sparkSession = spark,
          file = file,
          filePath = filePath,
          isSplitable = true,
          maxSplitBytes = splitBytes,
          partitionValues = partition.values)
      }
    }

  lazy val inferredSchema: Option[StructType] = inferSchema(fileIndex.allFiles())

  /**
   * Standard spark parquet [[PartitionReader]]
   */
  private lazy val internalRowReader: PartitionedFile => PartitionReader[InternalRow] =
    readerFactory.buildReader

  val rowReader: PartitionedFile => SparkRowReader = (file: PartitionedFile) =>
    new SparkRowReader(internalRowReader(file), rowConverter)

  /**
   * Standard spark encoder from [[InternalRow]] to [[Row]]
   */
  lazy val rowConverter: ExpressionEncoder.Deserializer[Row] =
    RowEncoder(schema).resolveAndBind().createDeserializer()

  /**
   * @return n-th partition of parquet files split
   */
  def splitFilesFilter: Int => Seq[PartitionedFile] =
    (value: Int) => splitFiles.
      lazyZip(splitFiles.indices).
      filter { case (_, index) => index % parallelism == value }.
      map { case (file, _) => file }

  /**
   * Row group metadata for each parquet file.
   */
  lazy val parquetBlocksMetaData: Seq[(FileStatus, BlockMetaData)] = {
    import scala.jdk.CollectionConverters._
    val hadoopConf = new Configuration()
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, spark.sessionState.conf.sessionLocalTimeZone)
    hadoopConf.setBoolean(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key, spark.sessionState.conf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(SQLConf.CASE_SENSITIVE.key, spark.sessionState.conf.caseSensitiveAnalysis)
    hadoopConf.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key, spark.sessionState.conf.isParquetBinaryAsString)
    hadoopConf.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, spark.sessionState.conf.isParquetINT96AsTimestamp)
    parquetScan.fileIndex.allFiles().
      flatMap {
        status =>
          ParquetFooterReader.
            readFooter(hadoopConf, status, NO_FILTER).
            getBlocks.
            asScala.
            map(block => (status, block))
      }
  }

}