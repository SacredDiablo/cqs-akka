package hw

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.{Flow, Sink, Source}
import hw.CalculatorRepository.{getLatestOffsetAndResult, initDataBase, updateResultAndOfsset}
import hw.TypedCalculatorWriteSide.{Added, Divided, Multiplied}
import scalikejdbc.{ConnectionPool, DB, using}

object TypedCalculatorReadSide {

}

case class TypedCalculatorReadSide(system: ActorSystem[NotUsed],  persId: PersistenceId) {
  initDataBase

  implicit val materializer            = system.classicSystem
  var (offset, latestCalculatedResult) = getLatestOffsetAndResult
  val startOffset: Int                 = if (offset == 1) 1 else offset + 1

  //    val readJournal: LeveldbReadJournal =
  val readJournal: CassandraReadJournal =
    PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  val source: Source[EventEnvelope, NotUsed] = readJournal
    .eventsByPersistenceId("001", startOffset, Long.MaxValue)

  source
    .map{x =>
      println(x.toString())
      x
    }
    .runForeach { event =>
      event.event match {
        case Added(_, amount) =>
          //          println(s"!Before Log from Added: $latestCalculatedResult")
          latestCalculatedResult += amount
          updateResultAndOfsset(latestCalculatedResult, event.sequenceNr)
          println(s"! Log from Added: $latestCalculatedResult")
        case Multiplied(_, amount) =>
          //          println(s"!Before Log from Multiplied: $latestCalculatedResult")
          latestCalculatedResult *= amount
          updateResultAndOfsset(latestCalculatedResult, event.sequenceNr)
          println(s"! Log from Multiplied: $latestCalculatedResult")
        case Divided(_, amount) =>
          //          println(s"! Log from Divided before: $latestCalculatedResult")
          latestCalculatedResult /= amount
          updateResultAndOfsset(latestCalculatedResult, event.sequenceNr)
          println(s"! Log from Divided: $latestCalculatedResult")
      }
    }

  val update: Flow[EventEnvelope, EventEnvelope, NotUsed] = Flow[EventEnvelope].map { event =>
    event.event match {
      case Added(_, amount) => latestCalculatedResult += amount
      case Multiplied(_, amount) => latestCalculatedResult *= amount
      case Divided(_, amount) => latestCalculatedResult /= amount
    }
    event
  }

  val updateDB: Flow[EventEnvelope, Int, NotUsed] = Flow[EventEnvelope].map{ event =>
    using(DB(ConnectionPool.borrow())) { db =>
      db.autoClose(true)
      db.localTx {
        _.update(s"update public.result set " +
          s"calculated_value = ${event.event}, write_side_offset = ${event.sequenceNr} where id = ${event.persistenceId}")
      }
    }}

  source.async
    .via(update).async
    .via(updateDB).async
    .runWith(Sink.ignore)

}