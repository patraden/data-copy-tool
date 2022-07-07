package dct

object DataCopyTool extends App {
  import dct.cli.CLIConfig
  CLIConfig(args) match {
    case null => System.exit(-1)
    case CLIConfig(mode, table, parquet, adfmap, url) =>
      mode match {
        case "create" =>
          ParquetToPGCopyCreate(table, parquet, adfmap, url).runStreaming()
        case "overwrite" => ParquetToPGCopyOverwrite(table, parquet, adfmap, url).runStreaming()
        case "append" => ???
      }
  }

}