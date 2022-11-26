package potamoi.flink.observer

import potamoi.cluster.PotaActorSystem
import potamoi.common.Order.{asc, desc}
import potamoi.common.PageReq
import potamoi.flink.share.model.{Fcid, Fjid}
import potamoi.k8s.K8sClient
import potamoi.syntax._
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

  "FlinkK8sObserver" when {

    "Track/Untrack cluster" taggedAs UnsafeEnv in testObr { obr =>
      obr.manager.trackCluster("app-t1" -> "fdev") *>
      obr.manager.trackCluster("app-t2" -> "fdev") *>
      obr.manager.listTrackedCluster.map(_ shouldBe Set(Fcid("app-t1", "fdev"), Fcid("app-t2", "fdev"))) *>
      obr.manager.untrackCluster("app-t2" -> "fdev") *>
      obr.manager.listTrackedCluster.map(_ shouldBe Set(Fcid("app-t1", "fdev")))
    }

    "Query RestEndpoint" taggedAs UnsafeEnv in testObr { obr =>
      obr.restEndpoints.get("session-01" -> "fdev").debug *>
      obr.restEndpoints.get("app-t1" -> "fdev").debug *>
      obr.restEndpoints.get("app-t1" -> "fdev").debug
    }

    "Query JobOverview" should {
      "list ov in the same cluster" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.jobs.listOverview("app-t1" -> "fdev").map(_.toPrettyStr).debug.repeat(spaced(1.seconds))
      }

      "get ov" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.jobs.getOverview(Fjid("app-t1", "fdev", "e5b1721d95a810ee799ea248b0b46a5c")).map(_.toPrettyStr).debug.repeat(spaced(1.seconds))
      }

      "list all tracked ov" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.manager.trackCluster("app-t2" -> "fdev") *>
        obr.jobs.listAllOverview.map(_.toString).debug.repeat(spaced(1.seconds))
      }

      "list jobId in the same cluster" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.jobs.listJobId("app-t1" -> "fdev").map(_.toString).debug.repeat(spaced(1.seconds))
      }

      "list jobId of all tracked clusters" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.manager.trackCluster("app-t2" -> "fdev") *>
        obr.jobs.listAllJobId.map(_.toString).debug.repeat(spaced(1.seconds))
      }

      "select job overview" taggedAs UnsafeEnv in testObr { obr =>
        import JobQryTerm._
        import SortField._
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.manager.trackCluster("app-t2" -> "fdev") *>
        obr.manager.trackCluster("session-01" -> "fdev") *>
        obr.jobs
          .selectOverview(
            filter = Filter(
              jobNameContains = Some("State machine"),
              jobIdIn = Set("5849a3ce9fccb3289688718122d098ae", "e5b1721d95a810ee799ea248b0b46a5c")
            ),
            orders = Vector(startTs -> desc, jobName -> asc)
          )
          .map(_.toString)
          .debug
          .repeat(spaced(1.seconds))
      }

      "paging select job overview" taggedAs UnsafeEnv in testObr { obr =>
        import JobQryTerm._
        import SortField._
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.manager.trackCluster("app-t2" -> "fdev") *>
        obr.manager.trackCluster("session-01" -> "fdev") *>
        obr.jobs
          .pageSelectOverview(
            filter = JobQryTerm.Filter(jobNameContains = Some("State machine")),
            pageReq = PageReq(pagNum = 1, pagSize = 2),
            orders = Vector(startTs -> desc, jobName -> asc)
          )
          .map(_.toString)
          .debug
          .repeat(spaced(1.seconds))
      }
    }

    // continue
  }

}
