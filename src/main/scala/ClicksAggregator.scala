import akka.actor.Actor
import akka.actor.Props
import scala.io.Source

import collection.mutable.Map

case class UserClick(user: Int, offer: Int)
case object Done

class ClicksAggregator extends Actor {
  val aggregatorCount = 4
  val inputFilePath = "/home/kazjote/projects/clicks_akka/clicks_10_000_000.txt"
    
  var aggregatedOffers = Map[(Int, Int), Int]()
  var responseCount = 0

  override def preStart(): Unit = {
    val aggregators = List.fill(aggregatorCount) {
      context.actorOf(Props[Aggregator])
    }

    for (line <- Source.fromFile(inputFilePath).getLines()) {
      val values = line.split(":")
      val userClick = UserClick(values(0).toInt, values(1).toInt)
      
      aggregators(userClick.user % aggregatorCount) ! userClick
    }
    
    for (aggregator <- aggregators) {
      aggregator ! Done
    }
  }

  def receive = {
    case aggregated: Map[(Int, Int), Int] => {      
      aggregated.foreach {
        case (key, counter) => {
          val oldCounter = aggregatedOffers.getOrElse(key, 0)

          aggregatedOffers.put(key, counter + oldCounter)
        }
      }

      responseCount += 1

      if (responseCount == aggregatorCount) {
        context.stop(self)
        
        
        
        aggregatedOffers.toList.sortBy( _._2 )foreach {
          case (offers, count) =>
            println(offers._1 + "-" + offers._2 + ":" + count)
        }
      }
    }
  }
}