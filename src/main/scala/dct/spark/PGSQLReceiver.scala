package dct.spark

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import java.nio.charset.StandardCharsets
import dct.slick.PGConnectionProvider

/**
 * Custom PostgresSQL jdbc [[DStream]] receiver leveraging jdbc driver [[CopyManager]] api.
 * @param query copy command query "COPY FROM  ... to STDOUT"
 * @param connectionProvider [[PGConnection]] factory class.
 */
class PGSQLReceiver(query: String,
                    connectionProvider: PGConnectionProvider)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

    def onStart(): Unit = {
      new Thread("PG jdbc Receiver") {
        override def run(): Unit = {
          receive()
        }
      }.start()
    }

    def onStop(): Unit = {
      connectionProvider.release()
    }

    private def receive(): Unit = {
      try {
        val conn = connectionProvider.acquire()
        if (conn.isFailure)
          log.error("Failed to establish connection to PG jdbc")
        else {
          val copyOut = conn.get.getCopyAPI.copyOut(query)
          var input = copyOut.readFromCopy()
          while (!isStopped() && input != null) {
            store(new String(input, StandardCharsets.UTF_8))
            input = copyOut.readFromCopy()
          }
        }
      } catch {
        case t: Throwable => reportError(s"Error receiving data from PG jdbc", t)
      }
    }
  }