package potamoi.flink.observer

import potamoi.cluster.PotaActorSystem
import potamoi.conf.PotaConf
import potamoi.k8s.K8sClient
import potamoi.logger.PotaLogger
import potamoi.testkit.{STSpec, UnsafeEnv}

// TODO unsafe
class FlinkK8sObserverSpec extends STSpec {

  import FlinkK8sObserver._

  val layers = {
    PotaConf.dev >+>
    PotaLogger.live ++
    PotaActorSystem.live ++
    K8sClient.live >+>
    FlinkK8sObserverImpl.live
  }

  "FlinkK8sObserver" should {

    "retrieve flink rest endpoint" taggedAs UnsafeEnv in {
      val ef2 = retrieveRestEndpoint("session-01" -> "fdev").debug *>
        retrieveRestEndpoint("session-14" -> "fdev").debug
      ef2.provide(layers).run
    }

  }

}
