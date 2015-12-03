
import akka.actor._
import java.util.{Comparator, PriorityQueue, LinkedList}
import scala.concurrent.duration._

case class Done(bestResultUpdate: Int)
case class Task(index: Int, consumedTime: Array[Int], machinesBooted: Int)
case class TaskAndBestResult(task: Task, bestResult: Int)
case class TryAssign

class TaskComparator extends Comparator[Task] {
	def compare(a: Task, b: Task) = b.index - a.index
}

object InputData {
	val deadline = 12
	val execTimes = Array(3, 5, 7, 9)
	val taskCount = execTimes.length

	def evaluate(consumedTime: Array[Int]) =
		consumedTime.reduceLeft(_ max _) * consumedTime.filter(_ > 0).length
}

class Worker extends Actor {
	import InputData._

	val recursionMaxDepth = 2
	var localBestResult = taskCount * deadline

	def solveTaskRecursive(index: Int, consumedTime: Array[Int], machinesBooted: Int, manager: ActorRef, rootIndex: Int) {
		println("solveTaskRecursive, index = " + rootIndex + " -> " + index + ", consumedTime = " + consumedTime.mkString)
		if (index == taskCount) {
			val result = evaluate(consumedTime)
			if (result < localBestResult) {
				localBestResult = result
				println(consumedTime.mkString(" ") + "; " + result)
			}
		} else if (index == rootIndex + recursionMaxDepth) {
			manager ! Task(index, consumedTime, machinesBooted)
		} else {
			val time = execTimes(index)
			for (machine <- 0 to machinesBooted) {
				if (consumedTime(machine) + time <= deadline) {
					val newConsumedTime = consumedTime.clone()
					newConsumedTime(machine) += time
					if (evaluate(newConsumedTime) < localBestResult) {
						solveTaskRecursive(index + 1, newConsumedTime, machinesBooted max (machine+1), manager, rootIndex)
					}
				}
			}
		}
	}

	def receive = {
		case TaskAndBestResult(Task(index, consumedTime, machinesBooted), bestResult: Int) =>
			println(self.path.name + ": " + index)
			localBestResult = bestResult
			solveTaskRecursive(index, consumedTime, machinesBooted, sender, index)
			sender ! Done(localBestResult)
	}
}

class Manager extends Actor {
	import InputData._

	var bestResult = taskCount * deadline
	val awaitingTasks = new PriorityQueue[Task](100, new TaskComparator)
	val freeWorkers = new LinkedList[ActorRef]
	for (i <- 1 to 2) {
		freeWorkers.add(context.system.actorOf(Props[Worker], "worker-" + i))
	}

	def enqueue(task: Task) {
		awaitingTasks.add(task)
	}

	def receive = {
		case task: Task =>
			enqueue(task)
			self ! TryAssign
		
		case TryAssign =>
			if (freeWorkers.size > 0 && awaitingTasks.size > 0) {
				val task = awaitingTasks.poll()
				println("TryAssign, task => " + evaluate(task.consumedTime) + ", best = " + bestResult)
				if (evaluate(task.consumedTime) < bestResult) {
					val worker = freeWorkers.poll()
					worker ! TaskAndBestResult(task, bestResult)
				}
			}
		
		case Done(bestResultUpdate) =>
			freeWorkers.add(sender)
			bestResult = bestResult min bestResultUpdate
			self ! TryAssign
	}
}

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
		val system = ActorSystem("BranchAndBound")
		val manager = system.actorOf(Props(classOf[Manager]), "manager")
		manager ! Task(0, new Array[Int](taskCount), 0)
		//run(0, new Array[Int](taskCount), 0)
		Thread sleep 5000
		system.shutdown()
	}
}

