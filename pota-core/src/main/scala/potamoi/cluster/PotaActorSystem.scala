package potamoi.cluster

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.util.Timeout
import potamoi.cluster.ActorCradle.{Ack, SpawnActor, SpawnAnonymousActor}
import potamoi.common.ActorExtension.ActorRefWrapper
import potamoi.common.ActorInteropException
import potamoi.config.{AkkaConf, PotaConf}
import potamoi.timex._
import zio.ZIO.attempt
import zio._
import zio.config.syntax.ZIOConfigNarrowOps

/**
 * Potamoi ActorSystem layer.
 */
object PotaActorSystem {

  type ActorGuardian = ActorSystem[ActorCradle.Cmd]

  val clive: ZLayer[AkkaConf, Throwable, ActorGuardian] =
    ZLayer.scoped {
      for {
        akkaConf    <- ZIO.service[AkkaConf]
        rawAkkaConf <- akkaConf.toAkkaRawConfig
        system <-
          ZIO.acquireRelease {
            attempt(ActorSystem(ActorCradle(), akkaConf.systemName, rawAkkaConf))
          } { system =>
            attempt(system.terminate()).ignore
          }
      } yield system
    }

  val live: ZLayer[PotaConf, Throwable, ActorGuardian] = ZLayer.service[PotaConf].narrow(_.akka) >>> clive

  implicit class ActorGuardianExtension(system: ActorGuardian) {

    private val defaultSpawnActorTimeout = 20.seconds
    private val defaultStopActorTimeout  = 30.seconds

    /**
     * Spawn actor in zio effect.
     */
    def spawn[T](
        behavior: Behavior[T],
        name: String
      )(implicit timeout: Timeout = defaultSpawnActorTimeout): IO[ActorInteropException, ActorRef[T]] = {
      system.askZIO[ActorRef[T]](SpawnActor(behavior, name, _))(system.scheduler, timeout)
    }

    /**
     * Spawn actor in zio effect.
     */
    def spawnAnonymous[T](behavior: Behavior[T])(implicit timeout: Timeout = defaultSpawnActorTimeout): IO[ActorInteropException, ActorRef[T]] = {
      system.askZIO[ActorRef[T]](SpawnAnonymousActor(behavior, _))(system.scheduler, timeout)
    }

    /**
     * Stop actor in zio effect.
     */
    def stop[T](ref: ActorRef[T])(implicit timeout: Timeout = defaultStopActorTimeout): IO[ActorInteropException, Ack.type] = {
      system.askZIO[Ack.type](ActorCradle.StopActor(ref, _))(system.scheduler, timeout)
    }
  }

}
