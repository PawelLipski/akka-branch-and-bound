
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
	val deadline = 30
	val execTimes = Array(3, 5, 7, 9, 11, 13, 15, 17)
	val taskCount = execTimes.length

	def evaluate(consumedTime: Array[Int]) =
		consumedTime.reduceLeft(_ max _) * consumedTime.filter(_ > 0).length
}

class Worker extends Actor {
	import InputData._

	val recursionMaxDepth = 2
	var localBestResult = taskCount * deadline

	def solveTaskRecursive(index: Int, consumedTime: Array[Int], machinesBooted: Int, manager: ActorRef, rootIndex: Int) {
		//println("solveTaskRecursive, index = " + rootIndex + " -> " + index + ", consumedTime = " + consumedTime.mkString(" "))
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
			localBestResult = bestResult
			solveTaskRecursive(index, consumedTime, machinesBooted, sender, index)
			sender ! Done(localBestResult)
	}
}

class Manager(val overlord: ActorRef) extends Actor {
	import InputData._

	var bestResult = taskCount * deadline
	val awaitingTasks = new PriorityQueue[Task](100, new TaskComparator)
	val freeWorkers = new LinkedList[ActorRef]
	for (i <- 1 to 2) {
		freeWorkers.add(context.system.actorOf(Props[Worker], "worker-" + i))
	}
	var runningTaskCount = 0

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
				if (evaluate(task.consumedTime) < bestResult) {
					val worker = freeWorkers.poll()
					worker ! TaskAndBestResult(task, bestResult)
					runningTaskCount += 1
				} else {
					self ! TryAssign
				}
			} else if (runningTaskCount == 0 && awaitingTasks.size == 0) {
				overlord ! bestResult
			}
		
		case Done(bestResultUpdate) =>
			freeWorkers.add(sender)
			bestResult = bestResult min bestResultUpdate
			runningTaskCount -= 1
			self ! TryAssign
	}
}

class Overlord extends Actor {
	import InputData._

	val manager = context.system.actorOf(Props(classOf[Manager], self), "manager")
	manager ! Task(0, new Array[Int](taskCount), 0)

	def receive = {
		case result: Int =>
			println("Parallel result: " + result)
			context.system.shutdown()
			Sequential.run(0, new Array[Int](taskCount), 0)
			println("Sequential result: " + Sequential.bestResult)
	}
}

object Sequential {
	import InputData._	
	var bestResult = taskCount * deadline

	def run(index: Int, consumedTime: Array[Int], machinesBooted: Int) {
		if (index == taskCount) {
			val result = evaluate(consumedTime)
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
					if (evaluate(newConsumedTime) < bestResult)
						run(index + 1, newConsumedTime, machinesBooted max (machine+1))
				}
			}
		}
	}
}

object Main {
	def main(args: Array[String]) {
		ActorSystem("BranchAndBound").actorOf(Props(classOf[Overlord]), "overlord")		
	}
}

