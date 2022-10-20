package kce.flink.operator

import kce.conf.ZIOK8sLayer
import kce.flink.operator.FlinkK8sOperatorImpl._
import kce.testkit.STSpec

class FlinkK8sOperatorSpec extends STSpec {

  // TODO unsafe
  "retrieveRestEndpoint" in {
    retrieveRestEndpoint("session-01", "fdev")
      .provide(ZIOK8sLayer.live)
      .debug
      .run
  }

  "test" in {}

}
