package dct.cli

import scopt.OParser
import dct.spark.hadoopDirExists
import dct.slick.isValidJdbcURL
import org.apache.log4j.{Level, Logger}

case class CLIConfig(mode: String = "create",
                     sqlTable: String = null.asInstanceOf[String],
                     parquetFilePath: String = null.asInstanceOf[String],
                     adfMappingFilePath: Option[String] = None,
                     jdbcURL: Option[String] = None)

object CLIConfig extends dct.spark.Logger {

  val logger: Logger = Logger.getRootLogger

  private val builder = OParser.builder[CLIConfig]
  private val parser: OParser[Unit, CLIConfig] = {
    import builder._
    OParser.sequence(
      programName("dct"),
      head("Data Copy Tool", "0.1.1-dev"),

      // copy mode argument
      opt[String]('m', "mode").
        required().
        validate( m =>
          if (("create" :: "overwrite" :: "append" :: Nil).contains(m.toLowerCase()))
            success
          else
            failure("value <mode> must be either of [create, overwrite, append]")
        ).
        action((m, c) => c.copy(mode = m.toLowerCase)).
        text("copy mode [create, overwrite, append]."),

      // sql table name
      // TODO make proper table name validator
      opt[String]("table").
        required().
        valueName("<schema.name>").
        validate(t =>
          if (t.split('.').length == 2) success
          else failure("not a valid sql table name")
        ).
        action((t, c) => c.copy(sqlTable = t)).
        text("target postgreSQL table name."),

      // parquet argument
      opt[String]("parquet").
        required().
        valueName("<dir>|<file>").
        validate( p =>
          if (hadoopDirExists(p)) success
          else failure(s"full hdfs path $p does not exists")
        ).
        action((p, c) => c.copy(parquetFilePath = p)).
        text("hdfs path to source parquet directory or file."),

      // mapping argument
      opt[String]("mapping").
        optional().
        valueName("<file>").
        validate( map =>
          if (hadoopDirExists(map)) success
          else failure(s"hdfs path $map does not exists")
        ).
        action((m, c) => c.copy(adfMappingFilePath = Option(m))).
        text("full hdfs path to ADF mapping json file (Optional)."),

      // jdbc URL
      opt[String]("url").
        optional().
        valueName("<jdbcURL>").
        validate ( url =>
          if (isValidJdbcURL(url)) success
          else failure("not a valid url, please ensure correctness here: https://jdbc.postgresql.org/documentation/80/connect.html")
        ).
        action((u, c) => c.copy(jdbcURL = Option(u))).
        text("postgreSQL jdbc driver url (Optional). DATABASE_URL env variable used by default.")
      ,
        help('h', "help").
          text("prints this usage text.")
    )
  }

    def apply(args: Seq[String]): CLIConfig = {
      val lvl = logger.getLevel
      logger.setLevel(Level.OFF)
      OParser.parse(parser, args, CLIConfig()) match {
      case Some(config) => logger.setLevel(lvl); config
      case _ =>
        logWarning("Failed to parse arguments")
        null.asInstanceOf[CLIConfig]
      }
    }

}
