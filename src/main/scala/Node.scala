package upmc.akka.leader

import akka.actor._
import Terminal._

import java.util
import java.util.Date
import java.util.Calendar

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.mutable.HashMap

case class Start ()

sealed trait SyncMessage
case class Sync (nodes:List[Int]) extends SyncMessage
case class SyncForOneNode (nodeId:Int, nodes:List[Int]) extends SyncMessage

// Beat
sealed trait AliveMessage
case class IsAlive (id:Int) extends AliveMessage
case class IsAliveLeader (id:Int) extends AliveMessage

sealed trait BeatMessage
case class BeatTick () extends BeatMessage

// Checker
abstract class Tick
case class CheckerTick () extends Tick

// Election
abstract class NodeStatus
case class Passive () extends NodeStatus
case class Candidate () extends NodeStatus
case class Dummy () extends NodeStatus
case class Waiting () extends NodeStatus
case class Leader () extends NodeStatus

abstract class LeaderAlgoMessage
case class Initiate (list:List[Int]) extends LeaderAlgoMessage
case class ALG (list:List[Int], nodeId:Int) extends LeaderAlgoMessage
case class AVS (list:List[Int], nodeId:Int) extends LeaderAlgoMessage
case class AVSRSP (list:List[Int], nodeId:Int) extends LeaderAlgoMessage

class Node (val id:Int, val terminaux:List[Terminal]) extends Actor {

	// Les differents acteurs du systeme
	val displayActor = context.actorOf(Props[DisplayActor], name = "displayActor")
	var allNodes:List[ActorSelection] = List()

	// paramètres
	val beatTime : Int = 50
	val checkerTime : Int = 200

	// variables du noeud
	var leaderId : Int = 0 // On estime que le premier Leader est 0
	var datesForChecking : HashMap[Int, Long] = HashMap.empty[Int, Long]

	var lastDate:Long = java.lang.System.currentTimeMillis()

	// variables pour election	
	var candSucc:Int = -1
	var candPred:Int = -1
	var status:NodeStatus = new Passive ()
	var isInElection = false // aide à ne pas relancer une élection en cours

	def nextNode (nodesAlive:List[Int]) : Int = {
		val nodesAliveSorted = nodesAlive.sorted
		val i = nodesAliveSorted.indexOf(id)
		val i_next = (i+1) % nodesAliveSorted.length
		return nodesAliveSorted(i_next)
	}

	def getActor (i:Int) : ActorSelection = {
		val n = terminaux(i)
		return context.actorSelection("akka.tcp://LeaderSystem" + n.id + "@" + n.ip + ":" + n.port + "/user/Node")
	}


	def receive = {

		// Initialisation
		case Start => {
			displayActor ! Message ("Node " + this.id + " is created")

			// Initilisation des autres remote, pour communiquer avec eux
			terminaux.foreach(n => {
				val remote = context.actorSelection("akka.tcp://LeaderSystem" + n.id + "@" + n.ip + ":" + n.port + "/user/Node")

				// Mise a jour de la liste des nodes
				this.allNodes = this.allNodes:::List(remote)
			})

			// Tick demandant d'envoyer un message indiquant que l'on est en vie
			context.system.scheduler.schedule(0 millis, beatTime millis, self, BeatTick)
			// Tick demandant de verifier qui est mort ou pas
			context.system.scheduler.schedule(0 millis, checkerTime millis, self, CheckerTick)
		}

		case BeatTick => {
			for (node <- allNodes) {
				val msg = if (id == leaderId) { IsAliveLeader(id) } else IsAlive (id)
				node ! msg
			}
		}

		// Messages venant des autres nodes : pour nous dire qui est encore en vie ou mort
		// on met a jour la liste des nodes
		case IsAlive (nodeId) => {
			//println("IsAlive " + nodeId)
			val curDate = java.lang.System.currentTimeMillis()
			datesForChecking.put(nodeId, curDate)
		}

		case IsAliveLeader (nodeId) => {
			//println("IsAliveLeader " + nodeId)
			isInElection = false // fin de l'élection
			// on est leader => le plus petit gagne
			if (leaderId == id) {
				if (nodeId<id) {
					leaderId = nodeId
					status = new Passive()
					println("STATUS CHANGE : New leader elected " + leaderId)
				}
			}
			// on n'est pas leader => on suit
			else if (leaderId != nodeId) {
				leaderId = nodeId
				status = new Passive()
				//println("The leader is : " + leaderId)
			}
			val curDate = java.lang.System.currentTimeMillis()
			lastDate = curDate
			datesForChecking.put(nodeId, curDate)
		}

		// A chaque fois qu'on recoit un CheckerTick : on verifie qui est mort ou pas
		// Objectif : lancer l'election si le leader est mort
		case CheckerTick => {
			var curDate = java.lang.System.currentTimeMillis()
			// leader supposé mort
			if (!isInElection && curDate - lastDate >= checkerTime) {
				println("Leader dead, starting election ")
				var nodesAlive : List[Int] = datesForChecking.collect {
					case (nodeId, lastNodeDate)
					if ((curDate - lastNodeDate) < checkerTime)
						=> nodeId
				}.toList

				leaderId = -1 // leader invalidé
				isInElection = true
				status = new Passive ()
				self ! Initiate (nodesAlive)
			}
		}

		// partie responsable de l'élection /////////////////////////////////////////:

		// lanncer l'élection avec les noeuds donnés
		case Initiate (nodesAlive) => {
			//println("Initiate " + status)
			if (status == Passive()) {
				candPred = -1
				candSucc = -1
				status = new Candidate ()
				getActor (nextNode(nodesAlive)) ! ALG(nodesAlive, id)
			}
		}

		case ALG (nodesAlive, init) => {
			println("ALG " + init + " " + status)
			if (status == Passive()) {
				status = new Dummy ()
				// println(id + " -> " + nextNode(nodesAlive) + " ALG " + init)
				getActor (nextNode(nodesAlive)) ! ALG(nodesAlive, init)
			}
			if (status == Candidate()) {
				candPred = init
				if (id > init) {
					if (candSucc == -1) {
						status = new Waiting ()
						println(id + " -> " + init + " AVS " + id)
						getActor (init) ! AVS(nodesAlive, id)
					}
					else {
						println(id + " -> " + candSucc + " AVSRSP " + candPred)
						getActor (candSucc) ! AVSRSP(nodesAlive, candPred)
						status = new Dummy()
					}
				}
				else if (id == init) {
					status = new Leader()
					leaderId = id
					println("STATUS CHANGE : leader now is " + id)

				}
			}
		}

		case AVS (nodesAlive, j) => {
			println("AVS " + j + " " + status)
			if (status == Candidate()) {
				if (candPred == -1)
					candSucc = j
				else {
					println(id + " -> " + j + " AVSRSP " + candPred)
					getActor (j) ! AVSRSP(nodesAlive, candPred)
					status = new Dummy()
				}
			}
			else if (status == Waiting())
				candSucc = j
		}

		case AVSRSP (nodesAlive, k) => {
			println("AVSRSP " + k + " " + status)
			if (status == Waiting()) {
				if (id == k) {
					status = Leader()
					leaderId = id
					println("STATUS CHANGE : leader now is " + id)
				}
				else {
					candPred = k
					if (candSucc == -1) {
						if (k < id) {
							status = new Waiting()
							println(id + " -> " + k + " AVS " + id)
							//getActor (k) ! AVS(nodesAlive, id)
						}
					}
					else {
						status = new Dummy ()
						println(id + " -> " + candSucc + " AVSRSP " + k)
						getActor (candSucc) ! AVSRSP(nodesAlive, k)
					}
				}
			}
		}
	}
}
