package potamoi.flink.observer

import com.softwaremill.quicklens.ModifyPimp
import potamoi.cluster.PotaActorSystem
import potamoi.flink.observer.FlinkObserverMultiVMSpec.{potaConf1, potaConf2}
import potamoi.flink.share.FlinkIO
import potamoi.k8s.{K8sClient, K8sOperator}
import potamoi.logger.PotaLogger
import potamoi.testkit.{PotaDev, STSpec, UnsafeEnv}
import zio.{durationInt, IO, ZIO}
import potamoi.syntax._
import zio.Schedule.spaced

object FlinkObserverMultiVMSpec {
  val potaConf1 = PotaDev.conf.change(
    _.modify(_.akka.port)
      .setTo(3301)
      .modify(_.akka.seedsAddress)
      .setTo(Set("127.0.0.1:3301"))
  )
  val potaConf2 = PotaDev.conf.change(
    _.modify(_.akka.port)
      .setTo(3302)
      .modify(_.akka.seedsAddress)
      .setTo(Set("127.0.0.1:3301"))
  )
}

class FlinkObserverMultiVMSpec extends STSpec {

  implicit class IOWrapper[A](io: FlinkIO[A]) {
    def watch: FlinkIO[Unit]       = io.debug.repeat(spaced(1.seconds)).unit
    def watchPretty: FlinkIO[Unit] = io.map(toPrettyString(_)).debug.repeat(spaced(1.seconds)).unit
  }

  private def testObrN1[E, A](effect: FlinkObserver => IO[E, A]): Unit = {
    ZIO
      .serviceWithZIO[FlinkObserver](effect(_))
      .provide(
        potaConf1,
        PotaLogger.live,
        PotaActorSystem.live,
        K8sClient.live,
        K8sOperator.live,
        FlinkObserver.live
      )
      .run
  }

  private def testObrN2[E, A](effect: FlinkObserver => IO[E, A]): Unit = {
    ZIO
      .serviceWithZIO[FlinkObserver](effect(_))
      .provide(
        potaConf2,
        PotaLogger.live,
        PotaActorSystem.live,
        K8sClient.live,
        K8sOperator.live,
        FlinkObserver.live
      )
      .run
  }

  "node 1" taggedAs UnsafeEnv in testObrN1 { obr =>
    obr.manager.trackCluster("app-t1" -> "fdev") *>
    obr.manager.trackCluster("app-t2" -> "fdev") *>
    ZIO.never
  }

  "node 2" taggedAs UnsafeEnv in testObrN2 { obr =>
//    obr.restEndpoints.retrieve("app-t1" -> "fdev").map(_.toPrettyStr).debug.ignore.repeat(spaced(1.seconds)).unit
    obr.jobs.listAllOverview.map(_.toString).watch
  }
}
