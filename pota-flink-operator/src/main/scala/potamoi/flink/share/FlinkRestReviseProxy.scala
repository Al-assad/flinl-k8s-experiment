package potamoi.flink.share

import akka.actor.typed.{ActorRef, Scheduler}
import akka.util.Timeout
import potamoi.cluster.PotaActorSystem.{ActorGuardian, ActorGuardianExtension}
import potamoi.common.ActorInteropException
import potamoi.config.{FlinkConf, PotaConf}
import potamoi.flink.share.cache.{FlinkRestEndpointCache, FlinkRestProxyCache}
import potamoi.flink.share.model.{Fcid, FlinkRestSvcEndpoint}
import potamoi.timex._
import zio.ZIO.succeed
import zio.http._
import zio.http.model.Status
import zio.{IO, ZIO, ZLayer}

/**
 * Flink rest endpoint reverse proxy service.
 */
object FlinkRestReviseProxy {

  val route = Http.collectZIO[Request] { case req @ _ -> "" /: "proxy" /: "flink" /: namespace /: clusterId /: path =>
    ZIO.serviceWithZIO[FlinkRestProxyService](_.proxy(Fcid(clusterId, namespace), path, req))
  }
}

trait FlinkRestProxyService {
  def proxy(fcid: Fcid, path: Path, req: Request): ZIO[Client, Throwable, Response]
}

object FlinkRestProxyService {

  val live = ZLayer {
    for {
      potaConf      <- ZIO.service[PotaConf]
      guardian      <- ZIO.service[ActorGuardian]
      restEndpoints <- guardian.spawn(FlinkRestEndpointCache(potaConf.akka.ddata.getFlinkRestEndpoint), "flkRestEndpointCache-px")
      restProxySet  <- guardian.spawn(FlinkRestProxyCache(potaConf.akka.ddata.getFlinkRestProxy), "flkRestProxyCache-px")
      sc         = guardian.scheduler
      askTimeout = potaConf.akka.defaultAskTimeout
    } yield Live(restEndpoints, restProxySet)(sc, askTimeout, potaConf.flink)
  }

  case class Live(
      restEndpoints: ActorRef[FlinkRestEndpointCache.Cmd],
      restProxySet: ActorRef[FlinkRestProxyCache.Cmd]
    )(implicit sc: Scheduler,
      askTimeout: Timeout,
      flinkConf: FlinkConf)
      extends FlinkRestProxyService {

    private def findEndpoint(fcid: Fcid): IO[ActorInteropException, Option[FlinkRestSvcEndpoint]] =
      restProxySet.containsEle(fcid).flatMap {
        case false => succeed(None)
        case true  => restEndpoints.get(fcid)
      }

    def proxy(fcid: Fcid, path: Path, req: Request): ZIO[Client, Throwable, Response] = {
      findEndpoint(fcid).flatMap {
        case None => succeed(Response.status(Status.NotFound))
        case Some(ept) =>
          val forwardUrl = req.url.setHost(ept.chooseHost).setPort(ept.port).setPath(path)
          Client.request(req.copy(url = forwardUrl))
      }
    }
  }

}


