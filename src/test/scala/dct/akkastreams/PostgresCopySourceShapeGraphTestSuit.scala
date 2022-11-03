package dct.akkastreams

import akka.stream.alpakka.slick.scaladsl.SlickSession
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.ByteString
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.Future

class PostgresCopySourceShapeGraphTestSuit extends AsyncFunSuite with BeforeAndAfterAll {
  @transient implicit var session: SlickSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    session = SlickSession.forConfig(dct.slick.defaultDBConfig)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    session.close()
  }

  test("postgres table stream init") {
    val schema = "public"
    val table = "\"1c_validation\""
    val tableName = s"$schema.$table"
    val source: Source[ByteString, Future[Long]] = PostgresCopySourceShapeGraph.
      lazySourceForTable(tableName, Seq("raw"))

    val totalRowsRead =source.
      map(bs => bs.decodeString("utf-8")).
      toMat(Sink.foreach(println))(Keep.left).
      run()

    totalRowsRead.map(q => assert(q === 1250))

  }

}
