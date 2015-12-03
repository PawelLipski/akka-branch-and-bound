import akka.actor._
import java.util.{HashMap, Random}
import scala.concurrent.duration._


object BranchAndBound {
	
	val deadline = 30
	val execTimes = Array(3, 5, 7, 9, 11, 13, 15, 17, 19)
	val taskCount = execTimes.length
	var bestResult = taskCount * deadline

	def getResult(consumedTime: Array[Int]) =
		consumedTime.reduceLeft(_ max _) * consumedTime.filter(_ > 0).length

	def run(index: Int, consumedTime: Array[Int], machinesBooted: Int) {
		if (index == taskCount) {
			val result = getResult(consumedTime)
			if (result < bestResult) {
				bestResult = result
				println(consumedTime.mkString(" ") + "; " + result)
			}
		} else {
			val time = execTimes(index)
			for (machine <- 0 to machinesBooted) {
				if (consumedTime(machine) + time <= deadline) {
					val newConsumedTime = consumedTime.clone()
					newConsumedTime(machine) += time
					if (getResult(newConsumedTime) < bestResult)
						run(index + 1, newConsumedTime, machinesBooted max (machine+1))
				}
			}
		}
	}

	def main(args: Array[String]) {
		//val system = ActorSystem("BranchAndBound")
		//val backbone = system.actorOf(Props(classOf[Backbone], auctionCount, buyerCount, searchCount), "backbone")
		//println(execTimes.reduceLeft(_ max _))
		run(0, new Array[Int](taskCount), 0)
	}
}

