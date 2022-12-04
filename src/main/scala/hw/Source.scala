package hw
import akka.NotUsed
import akka.actor.typed.{ActorSystem, Behavior, Props}
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.PersistenceId
import hw.TypedCalculatorWriteSide.{Add, Divide, Multiply}

object Source {
  val persId = PersistenceId.ofUniqueId("001")

  def apply(): Behavior[NotUsed] =
    Behaviors.setup { ctx =>
      val writeActorRef = ctx.spawn(TypedCalculatorWriteSide(persId), "Calculator", Props.empty)

      writeActorRef ! Add(10)
      writeActorRef ! Multiply(2)
      writeActorRef ! Divide(5)

      Behaviors.same
    }

  def main(args: Array[String]): Unit = {
    val value = Source()
    implicit val system: ActorSystem[NotUsed] = ActorSystem(value, "writeActorProcess")
  }

}