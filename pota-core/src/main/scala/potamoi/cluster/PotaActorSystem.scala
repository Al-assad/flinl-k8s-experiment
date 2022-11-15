package potamoi.cluster

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.util.Timeout
import potamoi.cluster.ActorCradle.{Ack, SpawnActor, SpawnAnonymousActor}
import potamoi.common.ActorExtension.ActorRefWrapper
import potamoi.common.ActorInteropException
import potamoi.config.PotaConf
import potamoi.timex._
import zio.ZIO.attempt
import zio._

/**
 * Potamoi ActorSystem layer.
 */
object PotaActorSystem {

  type ActorGuardian = ActorSystem[ActorCradle.Cmd]

  val live: ZLayer[PotaConf, Throwable, ActorGuardian] =
    ZLayer.scoped {
      for {
        potaConf   <- ZIO.service[PotaConf]
        akkaConfig <- potaConf.akka.toAkkaRawConfig
        system <-
          ZIO.acquireRelease {
            attempt(ActorSystem(ActorCradle(), potaConf.akka.systemName, akkaConfig))
          } { system =>
            attempt(system.terminate()).ignore
          }
      } yield system
    }

  implicit class ActorGuardianExtension(system: ActorGuardian) {

    private val defaultSpawnActorTimeout = 20.seconds
    private val defaultStopActorTimeout  = 30.seconds

    /**
     * Spawn actor in zio effect.
     */
    def spawn[T](behavior: Behavior[T], name: String)(
        implicit timeout: Timeout = defaultSpawnActorTimeout): IO[ActorInteropException, ActorRef[T]] = {
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
