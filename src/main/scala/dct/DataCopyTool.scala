package dct

object DataCopyTool extends App {
  CLIConfig(args.toIndexedSeq) match {
    case null => System.exit(-1)
    case CLIConfig(mode, table, parquet, adfmap, url) =>
      mode match {
        case "create"    => new ParquetToPGCreate(table, parquet, adfmap, url).execute()
        case "overwrite" => new ParquetToPGOverwrite(table, table, parquet, adfmap, url).execute() // TODO handle temporary table case
        case "append"    => ???
      }
  }

}