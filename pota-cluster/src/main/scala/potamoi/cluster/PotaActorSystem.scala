package potamoi.cluster

import akka.actor.typed.{ActorRef, ActorSystem, Behavior, Scheduler}
import akka.util.Timeout
import potamoi.cluster.ActorGuardian.{Ack, SpawnActor, SpawnAnonymousActor}
import potamoi.common.ActorExtension.ActorRefWrapper
import potamoi.common.ActorInteropException
import potamoi.conf.PotaConf
import zio.ZIO.attempt
import zio._
import potamoi.timex._

/**
 * Potamoi ActorSystem layer.
 */
object PotaActorSystem {

  val live: ZLayer[PotaConf, Throwable, ActorSystem[ActorGuardian.Cmd]] =
    ZLayer.scoped {
      for {
        potaConf <- ZIO.service[PotaConf]
        system <-
          ZIO.acquireRelease {
            attempt(ActorSystem(ActorGuardian(), potaConf.akka.sysName, potaConf.akka.rawActorConfig))
          } { system =>
            attempt(system.terminate()).ignore
          }
      } yield system
    }

  implicit class ActorGuardianExtension(system: ActorSystem[ActorGuardian.Cmd]) {

    /**
     * Spawn actor in zio effect.
     */
    def spawn[T](behavior: Behavior[T], name: String)(implicit timeout: Timeout = 15.seconds): IO[ActorInteropException, ActorRef[T]] = {
      system.askZIO[ActorRef[T]](SpawnActor(behavior, name, _))(system.scheduler, timeout)
    }

    /**
     * Spawn actor in zio effect.
     */
    def spawnAnonymous[T](behavior: Behavior[T])(implicit timeout: Timeout): IO[ActorInteropException, ActorRef[T]] = {
      system.askZIO[ActorRef[T]](SpawnAnonymousActor(behavior, _))(system.scheduler, timeout)
    }

    /**
     * Stop actor in zio effect.
     */
    def stop[T](ref: ActorRef[T])(implicit timeout: Timeout): IO[ActorInteropException, Ack.type] = {
      system.askZIO[Ack.type](ActorGuardian.StopActor(ref, _))(system.scheduler)
    }

    /**
     * Sync run [[spawn]] effect.
     */
    @throws[FiberFailure]
    def spawnNow[T](name: String, behavior: Behavior[T])(implicit sc: Scheduler, timeout: Timeout): ActorRef[T] = {
      Unsafe.unsafe { implicit u =>
        Runtime.default.unsafe.run(spawn(behavior, name)).getOrThrowFiberFailure()
      }
    }

    /**
     * Sync run [[stop]] effect.
     */
    @throws[FiberFailure]
    def stopNow[T](ref: ActorRef[T])(implicit sc: Scheduler, timeout: Timeout): Unit = {
      Unsafe.unsafe { implicit u =>
        Runtime.default.unsafe.run(stop(ref)).getOrThrowFiberFailure()
      }
    }
  }

}
