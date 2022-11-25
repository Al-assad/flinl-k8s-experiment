package potamoi.flink.observer

import potamoi.cluster.PotaActorSystem
import potamoi.flink.share.model.{Fcid, Fjid}
import potamoi.k8s.K8sClient
import potamoi.testkit.{PotaDev, STSpec, UnsafeEnv}
import zio.Schedule.spaced
import zio.{durationInt, IO, ZIO}
import potamoi.syntax._

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

  "FlinkK8sObserver" when {

    "Track/Untrack cluster" taggedAs UnsafeEnv in testObr { obr =>
      obr.manager.trackCluster("app-t1" -> "fdev") *>
      obr.manager.trackCluster("app-t2" -> "fdev") *>
      obr.manager.listTrackedCluster.map(_ shouldBe Set(Fcid("app-t1", "fdev"), Fcid("app-t2", "fdev"))) *>
      obr.manager.untrackCluster("app-t2" -> "fdev") *>
      obr.manager.listTrackedCluster.map(_ shouldBe Set(Fcid("app-t1", "fdev")))
    }

    "Query RestEndpoint" taggedAs UnsafeEnv in testObr { obr =>
      obr.restEndpoint.get("session-01" -> "fdev").debug *>
      obr.restEndpoint.get("app-t1" -> "fdev").debug *>
      obr.restEndpoint.get("app-t1" -> "fdev").debug
    }

    "Query JobOverview" should {
      "list ov in the same cluster" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.jobOverview.list("app-t1" -> "fdev").map(_.toPrettyStr).debug.repeat(spaced(1.seconds))
      }

      "get ov" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.jobOverview.get(Fjid("app-t1", "fdev", "e5b1721d95a810ee799ea248b0b46a5c")).map(_.toPrettyStr).debug.repeat(spaced(1.seconds))
      }

      "list all tracked ov" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.manager.trackCluster("app-t2" -> "fdev") *>
        obr.jobOverview.listAll.map(_.toString).debug.repeat(spaced(1.seconds))
      }

      "list jobId in the same cluster" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.jobOverview.listJobIds("app-t1" -> "fdev").map(_.toString).debug.repeat(spaced(1.seconds))
      }

      "list jobId of all tracked clusters" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.manager.trackCluster("app-t2" -> "fdev") *>
        obr.jobOverview.listAllJobIds.map(_.toString).debug.repeat(spaced(1.seconds))
      }
    }

    // continue
  }

}
