package kce.flink.operator

import kce.conf.{K8sClient, KceConf}
import kce.flink.operator.entity.{FlinkSessClusterDef, FlinkVer}
import kce.testkit.STSpec

class FlinkK8sOperatorSpec extends STSpec {

  val layers = (KceConf.live >+> K8sClient.live) >+> FlinkK8sOperator.live

  // TODO unsafe
  "retrieveRestEndpoint" in {
    FlinkK8sOperator
      .retrieveRestEndpoint("session-01", "fdev")
      .provide(layers)
      .debug
      .run
  }

  // TODO unsafe

  "deploy session cluster" should {
    "test-1" in {
      FlinkK8sOperator
        .deploySessionCluster(
          FlinkSessClusterDef(
            flinkVer = FlinkVer("1.15.2"),
            clusterId = "session-t1",
            namespace = "fdev",
            image = "flink:1.15.2"
          ))
        .provide(layers)
        .debug
        .run
    }

  }

}
