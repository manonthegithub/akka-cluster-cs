package com.manonthegithub.akka.cs

import akka.actor.{ActorSystem, Address}
import akka.cluster.Cluster
import com.typesafe.config.{Config, ConfigFactory}

object TestUtil {

}


trait MasterNode extends ClusterNode {

  override def role = "master"

  override lazy val system: ActorSystem = {
    val s = ActorSystem(systemName, appConfig)
    val clu = Cluster(s)
    clu.join(clu.selfAddress)
    //after we left the cluster we want to stop the process, so System should be terminated
    clu.registerOnMemberRemoved {
      s.terminate()
    }
    s
  }


}


trait SlaveNode extends ClusterNode {

  override def role = "slave"

  def joinPort: Int

  override lazy val system: ActorSystem = {
    val s = ActorSystem(systemName, appConfig)
    val clu = Cluster(s)
    clu.joinSeedNodes(List(Address("akka.tcp", systemName, "127.0.0.1", joinPort)))
    //after we left the cluster we want to stop the process, so System should be terminated
    clu.registerOnMemberRemoved {
      s.terminate()
    }
    s
  }


}


trait ClusterNode {

  def systemName: String = "test-cluster"

  def port: Int

  def role: String

  def system: ActorSystem

  protected lazy val appConfig: Config = {

    val debugConfig = ConfigFactory.parseString(
      """
        |akka {
        |
        |  loglevel = "DEBUG"
        |
        |  actor {
        |    debug {
        |      # enable function of LoggingReceive, which is to log any received message at
        |      # DEBUG level
        |      receive = on
        |    }
        |  }
        |}
      """.stripMargin
    )

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

    clusterConf
      .withFallback(debugConfig)
      .withFallback(ConfigFactory.defaultReference())
  }

}

