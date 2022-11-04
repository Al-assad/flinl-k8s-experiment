package potamoi.cluster

import akka.actor.typed.ActorSystem
import potamoi.conf.PotaConf
import zio.ZIO.attempt
import zio.{ZIO, ZLayer}

/**
 * Potamoi ActorSystem layer.
 */
object PotaActorSystem {

  // TODO resolve conf from PotaConf
  val live: ZLayer[PotaConf, Throwable, ActorSystem[ActorGuardian.Cmd]] =
    ZLayer.scoped {
      for {
        potaConf <- ZIO.service[PotaConf]
        system <-
          ZIO.acquireRelease {
            attempt(ActorSystem(ActorGuardian(), potaConf.akka.sysName))
          } { system =>
            attempt(system.terminate()).ignore
          }
      } yield system
    }
}
