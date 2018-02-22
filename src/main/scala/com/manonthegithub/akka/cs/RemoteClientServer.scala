package com.manonthegithub.akka.cs


import akka.actor.{Actor, ActorRef, ActorRefFactory, Props, Terminated}
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.{DistributedData, ORSet, ORSetKey}
import akka.event.LoggingReceive
import com.manonthegithub.akka.cs.RemoteClientServer.Settings

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration
import scala.util.Random


class RemoteClientServer(key: String, val settings: Settings) {

  import RemoteClientServer._

  private val ServersKey: ServerSetKey = ORSetKey[ActorRef](key)

  def client(picker: ServerPicker = new DynamicRoundRobinPicker)(implicit fact: ActorRefFactory): ActorRef =
    fact.actorOf(Props(new RemoteClientActor(ServersKey, picker, settings.writeConsistency)))

  def server(logic: ActorRef)(implicit fact: ActorRefFactory): ActorRef =
    fact.actorOf(Props(new RemoteServerActor(ServersKey, logic, settings.writeConsistency)))

}


object RemoteClientServer {

  case class ForwardingEnvelope(message: Any, correlation: Option[Any] = None)

  case class NoRegisteredRecipients(correlation: Option[Any])

  trait ServerPicker {

    /**
     * @param availableServers must be nonempty
     */
    def pick(availableServers: Vector[ActorRef]): immutable.Set[ActorRef]

  }

  object RandomPicker extends ServerPicker {
    override def pick(availableServers: Vector[ActorRef]): immutable.Set[ActorRef] =
      Set(availableServers(Random.nextInt(availableServers.size)))
  }

  object AllServersPicker extends ServerPicker {
    override def pick(availableServers: Vector[ActorRef]): Set[ActorRef] = availableServers.toSet
  }

  object FirstSeverPicker extends ServerPicker {
    override def pick(availableServers: Vector[ActorRef]): Set[ActorRef] = Set(availableServers.head)
  }

  class DynamicRoundRobinPicker extends ServerPicker {

    private var index = 0

    override def pick(availableServers: Vector[ActorRef]): Set[ActorRef] = {
      if (index < availableServers.size) {
        val r = Set(availableServers(index))
        index += 1
        r
      } else {
        index = 0
        Set(availableServers(index))
      }
    }

  }

  private type ServersSet = ORSet[ActorRef]
  private type ServerSetKey = ORSetKey[ActorRef]

  private case object Register

  private case object Registered

  private case object Deregister

  private case object Deregistered

  private class RemoteServerActor(private val Key: ServerSetKey, private val recipient: ActorRef, writeConsistency: WriteConsistency) extends Actor {

    private implicit val cluster = Cluster(context.system)
    private val replicator = DistributedData(context.system).replicator

    private def tryRegister(ref: ActorRef) =
      replicator ! Update(Key, ORSet.empty[ActorRef], writeConsistency, Some(Register))(_ + ref)

    private def tryDeregister(ref: ActorRef) =
      replicator ! Update(Key, ORSet.empty[ActorRef], writeConsistency, Some(Deregister))(_ - ref)

    override def preStart(): Unit = {
      super.preStart()
      tryRegister(context.self)
      context.watch(recipient)
    }

    override def receive = LoggingReceive {
      case env: ForwardingEnvelope =>
        recipient forward env
      case UpdateSuccess(Key, Some(Register)) => self ! Registered
      case f: UpdateFailure[ServersSet] if f.request.contains(Register) =>
        tryRegister(context.self)
      case Deregister => tryDeregister(self)
      case UpdateSuccess(Key, Some(Deregister)) => self ! Deregistered
      case f: UpdateFailure[ServersSet] if f.request.contains(Deregister) =>
        tryDeregister(context.self)
      case Registered => //ignore
      case Deregistered =>
        context.stop(self)
      case t: Terminated if t.actor == recipient =>
        self ! Deregister
    }

    override def postStop(): Unit = {
      tryDeregister(context.self)
      super.postStop()
    }

  }

  private class RemoteClientActor(private val Key: ServerSetKey, private val serverPicker: ServerPicker, writeConsistency: WriteConsistency) extends Actor {

    private implicit val cluster = Cluster(context.system)

    private val replicator = DistributedData(context.system).replicator

    /**
     * Vector is needed to guarantee ordering and fast access by index
     */
    private var servers: Vector[ActorRef] = Vector.empty

    private def subscribe = DistributedData(context.system).replicator ! Subscribe(Key, self)

    private def tryRemove(ref: ActorRef) =
      replicator ! Update(Key, ORSet.empty[ActorRef], writeConsistency, Some(ref))(_ - ref)

    override def preStart(): Unit = {
      super.preStart()
      subscribe
    }

    override def receive = LoggingReceive {
      case m: ForwardingEnvelope =>
        if (servers.nonEmpty) serverPicker.pick(servers).foreach(_ forward m)
        else sender ! NoRegisteredRecipients(m.correlation)
      case c@Changed(Key) =>
        val newState = c.get(Key).elements.toVector.sorted
        val newToWatch = newState.diff(servers)
        servers = newState
        newToWatch.foreach(context.watch)
      case t: Terminated =>
        tryRemove(t.actor)
      case f: UpdateFailure[ServersSet] =>
        f.request match {
          case Some(ref: ActorRef) => tryRemove(ref)
          case _ => //ignore
        }
      case _: UpdateSuccess[ServersSet] => //ignore
    }

  }

  class Settings(writeTimeout: FiniteDuration) {
    val writeConsistency = WriteMajority(writeTimeout)
  }

}
