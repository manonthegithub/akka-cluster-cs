package com.manonthegithub.akka.cs

import java.util.UUID

import akka.actor.{Actor, ActorSystem, Address, Props}
import akka.cluster.Cluster
import akka.testkit.TestKit
import com.manonthegithub.akka.cs.RemoteClientServer.Settings
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{Matchers, WordSpecLike}

object Conf {

  val sysName = "remotecs"

}

class RemoteClientServerSpec extends TestKit(new ClusterInitialization {
  override def systemName: String = Conf.sysName

  override def port: Int = 2552

  override def joinPort: Int = 0
}.ClusterSystem) with WordSpecLike with Matchers {

  import scala.concurrent.duration._


  val clu2 = new ClusterInitialization {

    override def role: String = "not-master"

    override def systemName: String = Conf.sysName

    override def port: Int = 2553

    override def joinPort: Int = 2552
  }


  "Client-Server" should {

    "stop server when server logic stops" in {

      val a = system.actorOf(Props(new Actor {
        override def receive: Receive = {
          case m => sender ! m
        }
      }))

      val serv = new RemoteClientServer("k", new Settings(1.second)).server(a)

      system.stop(a)

      watch(a)
      expectTerminated(a)
      watch(serv)
      expectTerminated(serv, max = 30.seconds)


    }


  }


  case class SayHi(correlation: Option[UUID] = Some(UUID.randomUUID()))

  case class Hi(correlation: Option[Any])


}


trait ClusterInitialization {

  def systemName: String

  def port: Int

  def joinPort: Int

  def role: String = "master"


  final lazy val ClusterSystem = {

    val s = ActorSystem(systemName, appConfig)

    val clu = Cluster(s)
    if (role == "master") {
      clu.join(clu.selfAddress)
    } else {
      clu.joinSeedNodes(List(Address("tcp", systemName, "127.0.0.1", joinPort)))
    }
    //after we left the cluster we want to stop the process, so System should be terminated
    clu.registerOnMemberRemoved {
      s.terminate()
    }

    s
  }

  private lazy val appConfig: Config = {

    val clusterConf = ConfigFactory.parseString(
      s"""
         |akka {
         |
         |  actor {
         |    provider = "cluster"
         |  }
         |
         |  remote {
         |    netty.tcp {
         |      hostname = "127.0.0.1"
         |      port = $port
         |    }
         |  }
         |}
        """.stripMargin
    )

    clusterConf.withFallback(ConfigFactory.defaultReference())
  }

}
