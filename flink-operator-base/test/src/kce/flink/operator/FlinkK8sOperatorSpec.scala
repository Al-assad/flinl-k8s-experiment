package kce.flink.operator

import kce.conf.K8sLayer
import kce.flink.operator.FlinkK8sOperatorImpl._
import kce.testkit.STSpec

class FlinkK8sOperatorSpec extends STSpec {

  // unsafe
  "retrieveRestEndpoint" in {
    retrieveRestEndpoint("session-01", "fdev")
      .provide(K8sLayer.live)
      .debug
      .run
  }

  "test" in {}

}
