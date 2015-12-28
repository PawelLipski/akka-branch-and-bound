
import akka.actor._
import java.util.{Comparator, PriorityQueue, LinkedList}
import scala.concurrent.duration._
import scala.util.Random

case class Done(bestResultUpdate: Int, workload: Int)
case class Task(index: Int, consumedTime: Array[Int], machinesBooted: Int)
case class TaskAndBestResult(task: Task, bestResult: Int)
case class TryAssign
case class AllDone(result: Int, workload: Int)

class TaskComparator extends Comparator[Task] {
	def compare(a: Task, b: Task) = b.index - a.index
}

object InputData {
	val rnd = new Random(123)

	def randomValues(n: Int, max: Int) = {
		for (i <- 1 to n) yield rnd.nextInt(max)
	}.toArray

	val deadline = 30

	var taskCount = 0
	var execTimes: Array[Int] = null
	var recursionMaxDepth = 1

	def initExecTimes(n: Int) {
		taskCount = n
		execTimes = randomValues(taskCount, deadline)
	}

	def evaluate(consumedTime: Array[Int]) =
		consumedTime.reduceLeft(_ max _) * consumedTime.filter(_ > 0).length
}

class Worker extends Actor {
	import InputData._

	var localBestResult = taskCount * deadline
	var localWorkload = 0

	def solveTaskRecursive(index: Int, consumedTime: Array[Int], machinesBooted: Int, manager: ActorRef, rootIndex: Int) {
		localWorkload += 1
		if (index == taskCount) {
			val result = evaluate(consumedTime)
			if (result < localBestResult) {
				localBestResult = result
				//println(consumedTime.filter(_ > 0).mkString(" ") + "; " + result)
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
			localWorkload = 0
			solveTaskRecursive(index, consumedTime, machinesBooted, sender, index)
			sender ! Done(localBestResult, localWorkload)
	}
}

class Manager(val overlord: ActorRef) extends Actor {
	import InputData._

	var bestResult = taskCount * deadline
	var workload = 0
	val awaitingTasks = new PriorityQueue[Task](100, new TaskComparator)
	val freeWorkers = new LinkedList[ActorRef]
	for (i <- 1 to 8) {
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
				overlord ! AllDone(bestResult, workload)
			}
		
		case Done(bestResultUpdate, workloadUpdate) =>
			freeWorkers.add(sender)
			bestResult = bestResult min bestResultUpdate
			workload += workloadUpdate
			runningTaskCount -= 1
			self ! TryAssign
	}
}

class Overlord extends Actor {
	import InputData._

	val manager = context.system.actorOf(Props(classOf[Manager], self), "manager")
	var start = System.nanoTime
	manager ! Task(0, new Array[Int](taskCount), 0)

	def receive = {
		case AllDone(result, workload) =>
			var elapsed = System.nanoTime - start
			//println("Parallel result: " + result + ", elapsed time: " + elapsed / 1e6 + " millisec")
			println(elapsed / 1e6)
			println(workload)
			context.system.shutdown()
			//start = System.nanoTime
			//Sequential.run(0, new Array[Int](taskCount), 0)
			//elapsed = System.nanoTime - start
			//println("Sequential result: " + Sequential.bestResult + ", elapsed time: " + elapsed / 1e6 + " millisec")
			//println(elapsed / 1e6)
			//println(Sequential.workload)
	}
}

object Sequential {
	import InputData._	
	var bestResult = taskCount * deadline
	var workload = 0

	def run(index: Int, consumedTime: Array[Int], machinesBooted: Int) {
		workload += 1
		if (index == taskCount) {
			val result = evaluate(consumedTime)
			if (result < bestResult) {
				bestResult = result
				//println(consumedTime.filter(_ > 0).mkString(" ") + "; " + result)
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
		InputData.initExecTimes(args(0).toInt)
		InputData.recursionMaxDepth = args(1).toInt
		ActorSystem("BranchAndBound").actorOf(Props(classOf[Overlord]), "overlord")		
	}
}

