package com.manonthegithub.akka.cs

import akka.actor.{Actor, Props}
import akka.cluster.Cluster
import akka.testkit.{ImplicitSender, TestKit}
import com.manonthegithub.akka.cs.RemoteClientServer.{ForwardingEnvelope, NoRegisteredRecipients, Settings}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.{Await, ExecutionContext, Promise}

class RemoteClientServerSpec extends TestKit(new MasterNode {

  override def port: Int = 2552

}.system) with WordSpecLike with Matchers with BeforeAndAfterAll with ImplicitSender {

  import scala.concurrent.duration._

  val allUpTimeout = 10.seconds

  val defaultSettings = new Settings(1.second)


  val slave = new SlaveNode {

    override def port: Int = 2553

    override def joinPort: Int = 2552

  }

  override def beforeAll(): Unit = {
    val masterUp = Promise[Unit]
    val slaveUp = Promise[Unit]

    Cluster(system).registerOnMemberUp(masterUp.success(()))
    Cluster(slave.system).registerOnMemberUp(slaveUp.success(()))
    implicit val ec = ExecutionContext.Implicits.global
    val allup = for {
      _ <- masterUp.future
      _ <- slaveUp.future
    } yield ()
    Await.result(allup, allUpTimeout)
  }


  "Client-Server" should {

    "stop server when server logic stops" in {

      val a = system.actorOf(Props(new Actor {
        override def receive: Receive = {
          case m => sender ! m
        }
      }))

      val serv = new RemoteClientServer("k", defaultSettings).server(a)

      system.stop(a)

      watch(a)
      expectTerminated(a)
      watch(serv)
      expectTerminated(serv, max = 30.seconds)


    }

    "respond with NoRegisteredRecipients" in {

      val cs = new RemoteClientServer("k", defaultSettings)

      cs.client()(system) ! ForwardingEnvelope("err", "cid")

      expectMsg(NoRegisteredRecipients("cid"))

    }

    "send message from client to server from different nodes" in {

      val cs = new RemoteClientServer("k", defaultSettings)

      val client = cs.client()(system)
      val server = cs.server(testActor)(slave.system)

      val message = ForwardingEnvelope("Hello", "1")

      awaitAssert {
        client ! message
        expectMsg(message)
      }

      slave.system.stop(server)


      awaitAssert {
        client ! message
        expectMsg(100.millis, NoRegisteredRecipients("1"))
      }

    }


  }

  override def afterAll(): Unit = {
    Cluster(system).leave(Cluster(slave.system).selfAddress)
    Cluster(system).leave(Cluster(system).selfAddress)
  }


}
