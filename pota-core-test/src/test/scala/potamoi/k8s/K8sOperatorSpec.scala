package potamoi.k8s

import potamoi.testkit.{PotaDev, STSpec}
import potamoi.syntax._

// TODO unsafe
class K8sOperatorSpec extends STSpec {

  val layer = PotaDev.conf >>> K8sClient.live >>> K8sOperator.live

  "K8sOperatorSpec" should {

    "getPodMetrics" in {
      K8sOperator
        .getPodMetrics("app-t1-taskmanager-1-1", "fdev")
        .debug
        .provide(layer)
        .runSpec
    }

    "getDeploymentSpec" in {
      K8sOperator
        .getDeploymentSpec("app-t1", "fdev")
        .map(_.toPrettyStr)
        .debug
        .provide(layer)
        .runSpec
    }
  }

}
