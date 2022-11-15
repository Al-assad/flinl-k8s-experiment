package potamoi.cluster

import akka.actor.typed._
import akka.actor.typed.scaladsl.Behaviors
import potamoi.actorx._

/**
 * Potamoi actor system guardian behavior using to provides
 * spawning/stopping of actor instances from [[ActorSystem]].
 */
object ActorCradle {

  sealed trait Cmd
  final case class SpawnActor[T](behavior: Behavior[T], name: String, reply: ActorRef[ActorRef[T]], props: Props = Props.empty) extends Cmd
  final case class SpawnAnonymousActor[T](behavior: Behavior[T], reply: ActorRef[ActorRef[T]], props: Props = Props.empty)      extends Cmd
  final case class StopActor[T](ref: ActorRef[T], reply: ActorRef[Ack.type])                                                    extends Cmd
  final private case class ActorStopped(reply: ActorRef[Ack.type])                                                              extends Cmd

  case object Ack

  def apply(): Behavior[Cmd] = Behaviors.setup { ctx =>
    ctx.log.info("Potamoi ActorGuardian started.")

    Behaviors
      .receiveMessage[Cmd] {
        case SpawnActor(behavior, name, reply, props) =>
          reply ! ctx.spawn(behavior, name, props)
          Behaviors.same

        case SpawnAnonymousActor(behavior, reply, props) =>
          reply ! ctx.spawnAnonymous(behavior, props)
          Behaviors.same

        case StopActor(ref, reply) =>
          ctx.watchWith(ref, ActorStopped(reply))
          ctx.stop(ref)
          Behaviors.same

        case ActorStopped(reply) =>
          reply ! Ack
          Behaviors.same
      }
      .receiveSignal {
        case (_, PreRestart) =>
          ctx.log.info("Potamoi ActorGuardian restarting...")
          Behaviors.same
        case (_, PostStop) =>
          ctx.log.info("Potamoi ActorGuardian stopped.")
          Behaviors.same
      }
      .onFailure[Exception](SupervisorStrategy.restart)
  }

}
