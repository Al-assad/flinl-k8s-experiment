package potamoi.flink

import com.softwaremill.quicklens.ModifyPimp
import potamoi.cluster.PotaActorSystem
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.operator.FlinkOperator
import potamoi.flink.share.{FlinkRestProxyService, FlinkRestReviseProxy}
import potamoi.fs.S3Operator
import potamoi.k8s.{K8sClient, K8sOperator}
import potamoi.logger.PotaLogger
import potamoi.testkit.{PotaDev, STSpec, UnsafeEnv}
import zio._
import zio.http._
import potamoi.syntax._

// todo unsafe
class FlinkRestReviseProxySpec extends STSpec {

  private val potaConf = PotaDev.conf.change(
    _.modify(_.akka.port)
      .setTo(3301)
      .modify(_.akka.seedsAddress)
      .setTo(Set("127.0.0.1:3301", "127.0.0.1:3302"))
  )

  private def testing[E, A](effect: (FlinkOperator, FlinkObserver) => IO[E, A]): Unit = {
    val ef = for {
      opr <- ZIO.service[FlinkOperator]
      obr <- ZIO.service[FlinkObserver]
      rs  <- effect(opr, obr)
    } yield rs
    ef.provide(
      potaConf,
      PotaActorSystem.live,
      K8sClient.live,
      K8sOperator.live,
      S3Operator.live,
      FlinkObserver.live,
      FlinkOperator.live
    ).runSpec
  }

  "flink rest revise proxy" should {

    // need tracking it first
    "enable proxy" taggedAs UnsafeEnv in testing { (opr, obr) =>
      obr.manager.trackCluster("app-t1" -> "fdev") *>
      obr.manager.trackCluster("app-t2" -> "fdev") *>
      opr.restProxy.enable("app-t1" -> "fdev") *>
      opr.restProxy.enable("app-t2" -> "fdev") *>
      opr.restProxy.list.map(_.toPrettyStr).debug *>
      ZIO.never
    }

    "disable proxy" taggedAs UnsafeEnv in testing { (opr, _) =>
      opr.restProxy.disable("app-t1" -> "fdev") *>
      opr.restProxy.list.map(_.toPrettyStr).debug *>
      ZIO.never
    }
  }
}

object FlinkRestReviseProxyApp extends ZIOAppDefault {

  private val potaConf = PotaDev.conf.change(
    _.modify(_.akka.port)
      .setTo(3302)
      .modify(_.akka.seedsAddress)
      .setTo(Set("127.0.0.1:3301", "127.0.0.1:3302"))
  )

  val app = FlinkRestReviseProxy.route
  val run = Server
    .serve(app)
    .provide(Server.default, Client.default, Scope.default, potaConf, PotaLogger.live, PotaActorSystem.live, FlinkRestProxyService.live)
}
