package dct.slick

import org.scalatest.funsuite.AnyFunSuite
import akka.stream.alpakka.slick.scaladsl.SlickSession
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import java.sql.{SQLException, SQLTransientConnectionException}

class ConnectionProviderTestSuit
  extends AnyFunSuite {

  @transient implicit var session: SlickSession = _

  test("establish connection") {
    session = SlickSession.forConfig(defaultDBConfig)
    val connToSucceed = ConnectionProvider().acquire()
    assert(connToSucceed.isSuccess)
    session.close()
  }

  test("wrong url") {
    val wrongURL = "jdbc:postgresql://localhost:5433/postgres?user=postgres&password=postgres"
    session = SlickSession.forConfig(
      forURL(wrongURL).
        withValue("db.connectionTimeout", ConfigValueFactory.fromAnyRef(5000.toString))
    )
    val connToFail = ConnectionProvider().acquire()
    assert(connToFail.isFailure)
    session.close()
  }

  test("connection pool with limited capacity") {
    session = SlickSession.forConfig(
      defaultDBConfig.
      withValue("db.numThreads", ConfigValueFactory.fromAnyRef(2.toString)).
      withValue("db.connectionTimeout", ConfigValueFactory.fromAnyRef(5000.toString))
    )
    val provider1 = ConnectionProvider()
    val provider2 = ConnectionProvider()
    val provider3 = ConnectionProvider()

    assert(provider1.acquire().get.getBackendPID != provider2.acquire().get.getBackendPID)
    assertThrows[SQLException](provider3.acquire().get.getBackendPID)
    session.close()

  }

  test("connection released to the pool") {
    session = SlickSession.forConfig(
      defaultDBConfig.
        withValue("db.numThreads", ConfigValueFactory.fromAnyRef(2.toString))
    )
    val provider1 = ConnectionProvider()
    val provider2 = ConnectionProvider()
    val provider3 = ConnectionProvider()

    assert(provider1.acquire().get.getBackendPID != provider2.acquire().get.getBackendPID)
    provider2.release(None)
    assert(provider1.acquire().get.getBackendPID != provider3.acquire().get.getBackendPID)
    session.close()

  }

  // run this test individually
  test("no url environment variable provided") {
    session = SlickSession.forConfig(defaultDBConfig.
      withValue("db.connectionTimeout", ConfigValueFactory.fromAnyRef(5000.toString))
    )

    assertResult(true)(ConnectionProvider().acquire().isFailure)
    assertThrows[SQLTransientConnectionException](ConnectionProvider().acquire().get)
    session.close()
  }

}
