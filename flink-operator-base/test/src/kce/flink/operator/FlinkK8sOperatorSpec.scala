package kce.flink.operator

import kce.conf.{K8sClient, KceConf}
import kce.testkit.STSpec

class FlinkK8sOperatorSpec extends STSpec {

  val layers = Array(FlinkK8sOperator.live, KceConf.live, K8sClient.live)

  // todo unsafe
  "retrieveRestEndpoint" in {
    FlinkK8sOperator
      .retrieveRestEndpoint("session-01", "fdev")
      .debug
      .provide(layers: _*)
      .run
  }

  "test" in {}

}
