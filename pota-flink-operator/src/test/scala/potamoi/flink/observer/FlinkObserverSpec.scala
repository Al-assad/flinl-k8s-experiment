package potamoi.flink.observer

import potamoi.cluster.PotaActorSystem
import potamoi.common.Order.{asc, desc}
import potamoi.common.PageReq
import potamoi.flink.share.FlinkIO
import potamoi.flink.share.model.{Fcid, Fjid, Ftid}
import potamoi.k8s.{K8sClient, K8sOperator}
import potamoi.logger.PotaLogger
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
        PotaLogger.live,
        PotaActorSystem.live,
        K8sClient.live,
        K8sOperator.live,
        FlinkObserver.live
      )
      .run
  }

  implicit class IOWrapper[A](io: FlinkIO[A]) {
    def watch: FlinkIO[Unit]       = io.debug.repeat(spaced(1.seconds)).unit
    def watchPretty: FlinkIO[Unit] = io.map(toPrettyString(_)).debug.repeat(spaced(1.seconds)).unit
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
        obr.jobs.listOverview("app-t1" -> "fdev").map(_.toPrettyStr).watch
      }

      "get ov" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.jobs.getOverview(Fjid("app-t1", "fdev", "e5b1721d95a810ee799ea248b0b46a5c")).watchPretty
      }

      "list all tracked ov" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.manager.trackCluster("app-t2" -> "fdev") *>
        obr.jobs.listAllOverview.map(_.toString).watch
      }

      "list jobId in the same cluster" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.jobs.listJobId("app-t1" -> "fdev").watch
      }

      "list jobId of all tracked clusters" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.manager.trackCluster("app-t2" -> "fdev") *>
        obr.jobs.listAllJobId.map(_.toString).watch
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
          .watch
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
          .watch
      }

      "get job metrics" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.jobs.getMetrics(Fjid("app-t1", "fdev", "e5b1721d95a810ee799ea248b0b46a5c")).watchPretty
      }

      "list job metrics" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.jobs.listMetrics("app-t1" -> "fdev").watchPretty
      }
    }

    "Query ClusterOverview" should {
      "get cluster overview" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.clusters.getOverview("app-t1" -> "fdev").watchPretty
      }

      "list cluster overview" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.manager.trackCluster("app-t2" -> "fdev") *>
        obr.clusters.listOverview.watch
      }
    }

    "Query TaskManager details" should {
      "list task manager id" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.clusters.listTmIds("app-t1" -> "fdev").watchPretty
      }

      "list task manager details" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.clusters.listTmDetails("app-t1" -> "fdev").watch
      }

      "get task manager details" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.clusters.getTmDetail(Ftid("app-t1", "fdev", "app-t1-taskmanager-1-1")).watchPretty
      }
    }

    "Query JobManager metrics" taggedAs UnsafeEnv in testObr { obr =>
      obr.manager.trackCluster("app-t1" -> "fdev") *>
      obr.clusters.getJmMetrics("app-t1" -> "fdev").watchPretty
    }

    "Query Taskmanager metrics" should {
      "get task manager metrics" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.clusters.getTmMetrics(Ftid("app-t1", "fdev", "app-t1-taskmanager-1-1")).watchPretty
      }

      "list task manager metrics" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.clusters.listTmMetrics("app-t1" -> "fdev").watch
      }
    }

    "Query k8s ref" should {
      "get referent resource" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.k8sRefs.getRef("app-t1" -> "fdev").watchPretty
      }

      "get referent resource snapshot" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.k8sRefs.getRefSnapshot("app-t1" -> "fdev").watchPretty
      }

      "list referent resource" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.manager.trackCluster("app-t2" -> "fdev") *>
        obr.k8sRefs.listRefs.watchPretty
      }

      "list referent resource snapshot" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.manager.trackCluster("app-t2" -> "fdev") *>
        obr.k8sRefs.listRefSnapshots.watch
      }

      "get deployment spec" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.k8sRefs.getDeploymentSnap("app-t1" -> "fdev", "app-t1").repeatWhile(_.isEmpty) *>
        obr.k8sRefs.getDeploymentSpec("app-t1" -> "fdev", "app-t1").map(_.toPrettyStr).debug
      }
    }

    "Query k8s pod metrics" should {
      "list pod metrics" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.k8sRefs.listPodMetrics("app-t1" -> "fdev").watchPretty
      }

      "get pod metrics" taggedAs UnsafeEnv in testObr { obr =>
        obr.manager.trackCluster("app-t1" -> "fdev") *>
        obr.k8sRefs.getPodMetrics("app-t1" -> "fdev", "app-t1-7b94664566-4t79h").watchPretty
      }
    }
  }

}
