package dct.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.connector.read.PartitionReader
import java.io.Closeable

/**
 * Wrapper for spark [[PartitionReader]] which returns [[Row]] instead of [[InternalRow]].
 * @param reader original spark reader.
 * @param converter original spark decoder from InternalRow to Row.
 */
class SparkPartitionReader(private val reader: PartitionReader[InternalRow],
                           private val converter: ExpressionEncoder.Deserializer[Row]
                          ) extends Closeable {

  def next(): Boolean = reader.next()
  def get(): Row = converter(reader.get())
  override def close(): Unit = reader.close()
}
