package potamoi.flink.observer

import potamoi.cluster.PotaActorSystem
import potamoi.k8s.K8sClient
import potamoi.testkit.{PotaDev, STSpec, UnsafeEnv}
import zio.Schedule.spaced
import zio.{durationInt, IO, ZIO}

// TODO unsafe
class FlinkObserverSpec extends STSpec {

  private def testObr[E, A](effect: FlinkObserver => IO[E, A]): Unit = {
    ZIO
      .serviceWithZIO[FlinkObserver](effect(_))
      .provide(
        PotaDev.conf,
        PotaActorSystem.live,
        K8sClient.live,
        FlinkObserver.live
      )
      .runSpec
  }

  "FlinkK8sObserver" should {

    "get flink rest endpoint" taggedAs UnsafeEnv in testObr { obr =>
      obr.restEndpoint.get("session-01" -> "fdev").debug *>
      obr.restEndpoint.get("app-t1" -> "fdev").debug
    }

    "track job overview" taggedAs UnsafeEnv in testObr { obr =>
      obr.tracker.trackCluster("app-t1" -> "fdev") *>
//      obr.tracker.listTrackedCluster.map(_.toPrettyString).debug.schedule(spaced(1.seconds)).forever.fork *>
      obr.jobOverview.listInCluster("app-t1" -> "fdev").map(_.toString()).debug.schedule(spaced(1.seconds)).forever
    }
  }

}
