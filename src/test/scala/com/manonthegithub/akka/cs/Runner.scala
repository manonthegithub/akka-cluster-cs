package com.manonthegithub.akka.cs

import java.util.UUID

import akka.actor.{Actor, ActorSystem, Props, Timers}
import akka.cluster.Cluster
import akka.event.LoggingReceive
import com.manonthegithub.akka.cs.RemoteClientServer.{ForwardingEnvelope, Settings}
import com.typesafe.config.ConfigFactory

import scala.util.Properties

object Runner extends App {

  val isServer = Properties.propOrFalse("is.server")

  val port = if (isServer) 2552 else 2551

  val joinPort =
    Properties
      .propOrNone("port")
      .map(_.toInt)
      .getOrElse(2552)

  val sysName = "cluster"

  private val configString =
    s"""
       |akka {
       |
       |  loglevel = "DEBUG"
       |
       |  actor {
       |
       |    debug {
       |      # enable function of LoggingReceive, which is to log any received message at
       |      # DEBUG level
       |      receive = on
       |    }
       |
       |    provider = "cluster"
       |  }
       |  remote {
       |    log-remote-lifecycle-events = off
       |    enabled-transports = ["akka.remote.netty.tcp"]
       |    netty.tcp {
       |      hostname = "127.0.0.1"
       |      port = $port
       |    }
       |  }
       |
       |  cluster {
       |    seed-nodes = [
       |      "akka.tcp://$sysName@127.0.0.1:$joinPort"
       |      ]
       |
       |    # auto downing is NOT safe for production deployments.
       |    # you may want to use it during development, read more about it in the docs.
       |    #
       |    # auto-down-unreachable-after = 10s
       |  }
       |}
    """.stripMargin

  val sys = ActorSystem(
    sysName,
    config = ConfigFactory.parseString(configString).withFallback(ConfigFactory.load())
  )

  val clientServerPairKey = "someRandomKey"

  trait SomeProto

  case class Hello(reply: String, correlation: Option[UUID] = Some(UUID.randomUUID())) extends SomeProto

  case class SayHello(send: String, correlation: Option[UUID] = Some(UUID.randomUUID())) extends SomeProto

  import scala.concurrent.duration._

  val proto = new RemoteClientServer(clientServerPairKey, new Settings(10.seconds))

  if (isServer) {
    val echo = sys.actorOf(Props(new Actor {

      override def receive: Receive = LoggingReceive {
        case SayHello(greeting, correlation) => sender ! Hello(s"$greeting, $correlation, ${Cluster(context.system).selfAddress.port}")
      }

    }))

    proto.server(echo)(sys)

  } else {

    import scala.concurrent.duration._

    sys.actorOf(Props(new Actor with Timers {

      private val client = proto.client()

      case object Send

      override def preStart(): Unit = {
        timers.startSingleTimer(Send, Send, 1.second)
      }

      override def receive: Receive = {
        case Send =>
          client ! ForwardingEnvelope(SayHello("hi!"), "111")
          timers.startSingleTimer(Send, Send, 1.second)
        case other => println(other)
      }
    }))

  }


}
