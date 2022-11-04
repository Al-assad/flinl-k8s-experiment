package potamoi.cluster

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed._
import potamoi.common.ActorExtension.BehaviorWrapper

/**
 * Potamoi actor system guardian behavior.
 */
object ActorGuardian {

  sealed trait Cmd
  final case class SpawnActor[T](name: String, behavior: Behavior[T], reply: ActorRef[ActorRef[T]], props: Props = Props.empty) extends Cmd
  final case class StopActor[T](ref: ActorRef[T], reply: ActorRef[Ack.type])                                                    extends Cmd
  final private case class ActorStopped(reply: ActorRef[Ack.type])                                                              extends Cmd

  case object Ack

  def apply(): Behavior[Cmd] = Behaviors.setup { ctx =>
    ctx.log.info("Potamoi ActorGuardian started.")

    Behaviors
      .receiveMessage[Cmd] {
        case SpawnActor(name, behavior, reply, props) =>
          reply ! ctx.spawn(behavior, name, props)
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
