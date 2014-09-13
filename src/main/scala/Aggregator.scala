import akka.actor.Actor
import akka.actor.Props

import collection.mutable.Map
import collection.mutable.Set

class Aggregator extends Actor {

  type UserOffers

  var clickedOffsers = Map[Int, Set[Int]]()
  var aggregatedOffers = Map[(Int, Int), Int]()

  def addOffer(user: Int, offer: Int) = {
    val set = clickedOffsers.getOrElseUpdate(user, Set[Int]())
    for (precedingOffer <- set) {
      val key = (precedingOffer, offer)
      val counter = aggregatedOffers.getOrElse(key, 0)

      aggregatedOffers.put(key, counter + 1)
    }

    set.add(offer)
  }

  def receive = {
    case UserClick(user, offer) => addOffer(user, offer)
    case Done => sender ! aggregatedOffers
  }
}
