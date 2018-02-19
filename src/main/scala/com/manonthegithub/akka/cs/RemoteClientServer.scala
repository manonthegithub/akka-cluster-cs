package com.manonthegithub.akka.cs


import akka.actor.{Actor, ActorRef, ActorRefFactory, Props, Terminated}
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.{DistributedData, ORSet, ORSetKey}
import akka.event.LoggingReceive
import com.manonthegithub.akka.cs.RemoteClientServer.Settings

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration


class RemoteClientServer(val key: String, settings: Settings)(implicit val fact: ActorRefFactory) {

  import RemoteClientServer._
  import settings._

  private val ServersKey: ServerSetKey = ORSetKey[ActorRef](key)

  def client(pickServer: immutable.Set[ActorRef] => immutable.Set[ActorRef]): ActorRef =
    fact.actorOf(Props(new RemoteClientActor(ServersKey, pickServer, writeConsistency)))

  def server(logic: ActorRef): ActorRef =
    fact.actorOf(Props(new RemoteServerActor(ServersKey, logic, writeConsistency)))

}


object RemoteClientServer {

  case class ForwardingEnvelope(message: Any, correlationId: String)

  case class NoRegisteredRecipients(correlationId: String)

  type ServersSet = ORSet[ActorRef]
  type ServerSetKey = ORSetKey[ActorRef]


  private class RemoteServerActor(private val Key: ServerSetKey, private val recipient: ActorRef, writeConsistency: WriteConsistency) extends Actor {

    private implicit val cluster = Cluster(context.system)
    private val replicator = DistributedData(context.system).replicator

    private case object Register

    private case object Registered

    private case object Deregister

    private case object Deregistered

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

  private class RemoteClientActor(private val Key: ServerSetKey, private val pickRecipients: immutable.Set[ActorRef] => immutable.Set[ActorRef], writeConsistency: WriteConsistency) extends Actor {

    private implicit val cluster = Cluster(context.system)

    private val replicator = DistributedData(context.system).replicator

    private var servers: immutable.Set[ActorRef] = Set.empty

    private def subscribe = DistributedData(context.system).replicator ! Subscribe(Key, self)

    private def tryRemove(ref: ActorRef) =
      replicator ! Update(Key, ORSet.empty[ActorRef], writeConsistency, Some(ref))(_ - ref)

    override def preStart(): Unit = {
      super.preStart()
      subscribe
    }

    override def receive = LoggingReceive {
      case m: ForwardingEnvelope =>
        if (servers.nonEmpty) pickRecipients(servers).foreach(_ forward m)
        else sender ! NoRegisteredRecipients(m.correlationId)
      case c@Changed(Key) =>
        val newState = c.get(Key).elements
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
