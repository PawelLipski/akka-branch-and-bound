import akka.actor._
import java.util.{Comparator, PriorityQueue, LinkedList}
import scala.concurrent.duration._

case class Done
case class Task(priority: Int)
case class TryAssign

class TaskComparator extends Comparator[Task] {
	def compare(a: Task, b: Task) = a.priority - b.priority
}

class Worker extends Actor {
	def receive = {
		case task: Task =>
			println(self.path.name + ": " + task)
			sender ! Done
	}
}

class Manager extends Actor {
	
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
				//if (evaluate(task)  bestResult) {
					val worker = freeWorkers.poll()
					worker ! task
				//}
			}
		
		case Done /*(bestResultUpdate)*/ =>
			freeWorkers.add(sender)
			//bestResult ?= bestResultUpdate
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
		manager ! Task(10)
		manager ! Task(20)
		manager ! Task(30)
		manager ! TryAssign
		manager ! TryAssign
		manager ! TryAssign
		run(0, new Array[Int](taskCount), 0)
		system.shutdown()
	}
}

