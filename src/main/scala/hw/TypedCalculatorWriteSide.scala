package hw

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, RetentionCriteria}
import hw.Source.persId

object TypedCalculatorWriteSide {

  trait CborSerializable

  sealed trait Command
  case class Add(amount: Int)      extends Command
  case class Multiply(amount: Int) extends Command
  case class Divide(amount: Int)   extends Command

  sealed trait Event
  case class Added(id: Int, amount: Int)      extends Event
  case class Multiplied(id: Int, amount: Int) extends Event
  case class Divided(id: Int, amount: Int)    extends Event
  case class CalculationCompleted(id: Int, amount: Int)    extends Event

  final case class State(value: Int) extends CborSerializable {
    def add(amount: Int): State      = copy(value = value + amount)
    def multiply(amount: Int): State = copy(value = value * amount)
    def divide(amount: Int): State   = copy(value = value / amount)
  }

  object State {
    val empty = State(0)
  }

  def apply(persId: PersistenceId): Behavior[Command] =
    Behaviors.setup { ctx =>
      EventSourcedBehavior[Command, Event, State](
        persistenceId = persId,
        State.empty,
        (state, command) => handleCommand("001", state, command, ctx),
        (state, event) => handleEvent(state, event, ctx)
      )
        .snapshotWhen{
          case (state, CalculationCompleted(_, _), seqNumber) if seqNumber % 10 == 0 => true
          case (state, event, seqNumber) => true //todo оба выхода true в оригинале? Так точно должно быть?
        }
        .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 100, keepNSnapshots = 2))
    }

  def handleCommand(
                     persistenceId: String,
                     state: State,
                     command: Command,
                     ctx: ActorContext[Command]
                   ): Effect[Event, State] =
    command match {
      case Add(amount) =>
        ctx.log.info(s"Receive adding for number: $amount and state is ${state.value}")
        val added = Added(persistenceId.toInt, amount)
        Effect
          .persist(added)
          .thenRun { x =>
            ctx.log.info(s"The state result is ${x.value}")
          }
      case Multiply(amount) =>
        ctx.log.info(s"Receive multiplying for number: $amount and state is ${state.value}")
        Effect
          .persist(Multiplied(persistenceId.toInt, amount))
          .thenRun { newState =>
            ctx.log.info(s"The state result is ${newState.value}")
          }
      case Divide(amount) =>
        ctx.log.info(s"Receive dividing for number: $amount and state is ${state.value}")
        Effect
          .persist(Divided(persistenceId.toInt, amount))
          .thenRun { x =>
            ctx.log.info(s"The state result is ${x.value}")
          }
    }

  def handleEvent(state: State, event: Event, ctx: ActorContext[Command]): State =
    event match {
      case Added(_, amount) =>
        ctx.log.info(s"Handing event amount is $amount and state is ${state.value}")
        state.add(amount)
      case Multiplied(_, amount) =>
        ctx.log.info(s"Handing event amount is $amount and state is ${state.value}")
        state.multiply(amount)
      case Divided(_, amount) =>
        ctx.log.info(s"Handing event amount is $amount and state is ${state.value}")
        state.divide(amount)
    }
}
