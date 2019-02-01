package my.cluster;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSystem;
import akka.actor.Props;

import akka.actor.Address;
import akka.cluster.*;

import java.util.LinkedList;
import java.util.List;

public class Main {

  public static void main(String[] args) {
    startup(args);
  }

  public static void startup(String[] init) {
    // init: role port

    // Override the configuration of the port
    String conf = "akka.remote.netty.tcp.port=" + init[1] + "\n" +
            "akka.remote.artery.canonical.port=" + init[1];
    Config config =
      ConfigFactory.parseString(
        conf)
        .withFallback(
            ConfigFactory.parseString("akka.cluster.roles = [compute]"))
        .withFallback(ConfigFactory.load("cluster"));

    ActorSystem system = ActorSystem.create("ClusterSystem", config);

    if (init[0].equals("leader")){
      String le = "akka://ClusterSystem@127.0.0.1:"+ init[1] +"/user/Service";
      system.actorOf(Props.create(Client.class,
              le),
              "client");
//      system.actorOf(Props.create(Client.class,
//              "akka://ClusterSystem@127.0.0.1:2552/user/Service"),
//              "client");
    }
    else if (init[0].equals("join"))
    {
      system.actorOf(Props.create(Service.class, init[1]), "Service");
    }
  }
}
