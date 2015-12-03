package reactiveRoutersDemo      

import akka.routing._
import akka.actor._
import akka.event._
import java.util.{HashMap, Random}
import scala.concurrent.duration._


object MasterSearch {
	case class Terminated(a: ActorRef)
}

class MasterSearch(routeesCnt: Int) extends Actor {
	import MasterSearch._
	import AuctionSearch._

	val routees = Vector.fill(routeesCnt) {
		val r = context.actorOf(Props[AuctionSearch])
		context watch r
		ActorRefRoutee(r)
	}

	//var broadcastingRouter = Router(BroadcastRoutingLogic(), routees)

	//var roundRobinRouter = Router(RoundRobinRoutingLogic(), routees)

	val resizer = DefaultResizer(lowerBound = 2, upperBound = 30)
	var roundRobinRouter = context.actorOf(RoundRobinPool(routeesCnt, Some(resizer)).props(Props[AuctionSearch]), "roundRobinRouter")

	def receive = LoggingReceive {

		case q: Query =>
			roundRobinRouter.tell(q, sender())
			//roundRobinRouter.route(q, sender())

		case r: Register =>
			roundRobinRouter.tell(Broadcast(r), sender())
			//broadcastingRouter.route(r, sender())
	}
}

object Auction {
	case object Create
}

class Auction(val name: String) extends Actor with ActorLogging {
	import Auction._
	import AuctionSearch._

	def receive: Receive = LoggingReceive {
		case Create =>
			val lookup = context.system.actorSelection("user/masterSearch") 
			lookup ! Register(name, self)
	}
}

object Buyer {
	case object MakeBid
}

object TimeCounter {
	case object StatsRequest
	case class Stats(percent: Double, q75: Double, q90: Double)
	case class Store(time: Double)
}

class TimeCounter extends Actor with ActorLogging {
	import TimeCounter._

	var totalCount = 0
	var totalTime = 0.0
	var allTimes = new scala.collection.mutable.ListBuffer[Double]

	def receive = {
		case Store(time) =>
			totalCount += 1
			totalTime += time
			allTimes append time

		case StatsRequest =>
			sender ! Stats(average, getQuantil(75), getQuantil(90))
	}

	def average = totalTime / totalCount

	def getQuantil(percent: Int) = {
		allTimes = allTimes.sorted
		allTimes(allTimes.size * percent / 100)
	}
}

object CountdownPrinter {
	case object Increment
	case class DecrementAndMaybePrint(msg: String)
}

class CountdownPrinter extends Actor with ActorLogging {
	import CountdownPrinter._

	var counter = 0

	def receive = {
		case Increment =>
			counter += 1

		case DecrementAndMaybePrint(msg) =>
			counter -= 1
			if (counter == 0) {
				println(msg)
				System exit 0
			}
	}
}

class Buyer(val auctionName: String) extends Actor with ActorLogging {

	import AuctionSearch._
	import Buyer._
	import CountdownPrinter._
	import TimeCounter._

	implicit val executionContext = context.system.dispatcher

	val lookup = context.system.actorSelection("user/masterSearch")
	val timeCounter = context.system.actorSelection("user/timeCounter")
	val printer = context.system.actorSelection("user/printer")
	
	var itersLeft = 100

	var queryTime: Long = 0
	def getResponseTime = (System.nanoTime - queryTime) / 1e9
	def storeResponseTime() {
		timeCounter ! Store(getResponseTime)
	}

	printer ! Increment
	self ! MakeBid

	def receive = LoggingReceive {
		case MakeBid =>
			if (itersLeft > 0) {
				itersLeft -= 1
				lookup ! Query(auctionName)
				queryTime = System.nanoTime
			} else {
				timeCounter ! StatsRequest
			}
			//context.system.scheduler.scheduleOnce(1 seconds, self, MakeBid)

		case Found(_) =>
			storeResponseTime()
			self ! MakeBid

		case NotFound =>
			storeResponseTime()
			self ! MakeBid

		case Stats(time, q75, q90) =>
			def us(value: Double) = (value * 1e6).asInstanceOf[Int]
			val msg = "Average response time [us]: %d\nQ75 [us]: %d\nQ90 [us]: %d\n" format(us(time), us(q75), us(q90))
			printer ! DecrementAndMaybePrint(msg)
		//log.info("Average response time: " + String.format("%.3f", totalResponseTime / responsesReceived) + " seconds")
	}
}

class Backbone(auctionCount: Int, buyerCount: Int, searchCount: Int) extends Actor {
	import Auction._
	import Buyer._

	val random = new Random

	val masterSearch = context.system.actorOf(Props(classOf[MasterSearch], searchCount), "masterSearch")
	val timeCounter = context.system.actorOf(Props(classOf[TimeCounter]), "timeCounter")
	val printer = context.system.actorOf(Props(classOf[CountdownPrinter]), "printer")

	val auctions = for (i <- 0 until auctionCount) yield context.system.actorOf(
		Props(new Auction("Auction-" + i)), "auction-" + i)
	auctions.foreach(_ ! Create)

	val buyers = for (i <- 0 until buyerCount) yield context.system.actorOf(
		Props(new Buyer("Auction-" + random.nextInt(auctionCount))), "buyer-" + i)
	buyers.foreach(_ ! MakeBid)

	def receive = LoggingReceive {
		case _ =>
	}
}

object AuctionSearch {
	case class Register(name: String, ref: ActorRef)
	case class Query(name: String)
	case class Found(matching: ActorRef)
	case object NotFound
}

class AuctionSearch extends Actor with ActorLogging {
	import AuctionSearch._

	val reg = new HashMap[String, ActorRef]

	def action: Receive = LoggingReceive {
		case Register(name, ref) =>
			reg.put(name, ref)
			//log.info("Registering the auction " + name)

		case Query(name) =>
			//log.info("I got the query " + name + " and I am searching...")
			if (reg containsKey name)
				sender ! Found(reg get name)
			else
				sender ! NotFound
	}

	def receive = action
}

object ReactiveRouterApp {
	
	def main(args: Array[String]) {
		//println(args(0))
		val system = ActorSystem("ReactiveRouters")
		val auctionCount = 150
		val buyerCount = 50
		val searchCount = 5
		val backbone = system.actorOf(Props(classOf[Backbone], auctionCount, buyerCount, searchCount), "backbone")
	}
}

