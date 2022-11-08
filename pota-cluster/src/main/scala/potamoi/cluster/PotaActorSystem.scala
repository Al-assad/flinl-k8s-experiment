package potamoi.cluster

import akka.actor.typed.{ActorRef, ActorSystem, Behavior, Scheduler}
import akka.util.Timeout
import potamoi.cluster.ActorGuardian.{Ack, SpawnActor}
import potamoi.common.ActorExtension.ActorRefWrapper
import potamoi.common.ActorInteropException
import potamoi.conf.PotaConf
import zio.ZIO.attempt
import zio._

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
    def spawn[T](name: String, behavior: Behavior[T])(implicit sc: Scheduler, timeout: Timeout): IO[ActorInteropException, ActorRef[T]] = {
      system.askZIO[ActorRef[T]](SpawnActor(name, behavior, _))
    }

    /**
     * Stop actor in zio effect.
     */
    def stop[T](ref: ActorRef[T])(implicit sc: Scheduler, timeout: Timeout): IO[ActorInteropException, Ack.type] = {
      system.askZIO[Ack.type](ActorGuardian.StopActor(ref, _))
    }

    /**
     * Sync run [[spawn]] effect.
     */
    @throws[FiberFailure]
    def spawnNow[T](name: String, behavior: Behavior[T])(implicit sc: Scheduler, timeout: Timeout): ActorRef[T] = {
      Unsafe.unsafe { implicit u =>
        Runtime.default.unsafe.run(spawn(name, behavior)).getOrThrowFiberFailure()
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
