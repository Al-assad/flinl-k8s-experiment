package potamoi.flink.observer

import potamoi.cluster.PotaActorSystem
import potamoi.k8s.K8sClient
import potamoi.logger.PotaLogger
import potamoi.testkit.{PotaDev, STSpec, UnsafeEnv}
import zio.ZIO

// TODO unsafe
class FlinkObserverSpec extends STSpec {

  import FlinkObserver._

  val layers = {
    PotaDev.conf >+>
    PotaLogger.live ++
    PotaActorSystem.live ++
    K8sClient.live >+>
    FlinkObserver.live
  }

  "FlinkK8sObserver" should {

    "retrieve flink rest endpoint" taggedAs UnsafeEnv in {
      ZIO
        .serviceWithZIO[FlinkObserver] { obr =>
          obr.restEndpoint.get("session-01" -> "fdev").debug *>
          obr.restEndpoint.get("session-14" -> "fdev").debug
        }
        .provide(layers)
        .run
    }

  }

}
