package no_config;

// In order to be more flexible, this package tests if akka can load config without external config file.

import static java.util.concurrent.TimeUnit.SECONDS;
import java.util.Random;

import akka.actor.*;
import com.typesafe.config.Config;
import scala.concurrent.duration.Duration;

import com.typesafe.config.ConfigFactory;
// for using google cloud, replace 127.0.0.1 with 10.138.0.3 to make
// frontend system running on some port, replace 127.0.0.1 with 10.138.0.2 to make
// backend system ruuning on some port
public class main {

  public static void main(String[] args) {
    if (args[0].equals("Worker"))
      startBackendSystem(args[1]);
    if (args[0].equals("Manager"))
      startFrontendSystem(args[1], args[2]);
  }

  public static void startBackendSystem(String port) {
    Config config =  ConfigFactory.parseString("akka {\n" +
            "  actor {\n" +
            "    provider = remote\n" +
            "  }\n" +
            "  remote {\n" +
            "    netty.tcp {\n" +
            "      hostname = \"127.0.0.1\"\n" +
            "    port = " + port + "}\n" +
            "  }\n" +
            "}");

    ActorSystem.create("WorkerSystem",
        ConfigFactory.load((config)));
    System.out.println("Started BackendSystem");
  }


  public static void startFrontendSystem(String port, String backend_port) {

    Config config =  ConfigFactory.parseString("akka {\n" +
            "  actor {\n" +
            "    provider = remote \n" +
            "    warn-about-java-serializer-usage = false \n" +
            "  deployment {\n" +
            "      \"/Manager/*\" {\n" +
            "        remote = \"akka.tcp://WorkerSystem@127.0.0.1:" + backend_port + "\"\n" +
            "      }\n" +
            "    }}\n" +
            "  remote {\n" +
            "    netty.tcp {\n" +
            "      hostname = \"127.0.0.1\"\n" +
            "    port = " + port + "}\n" +
            "  }\n" +
            "}");

    final ActorSystem system = ActorSystem.create("ManagerSystem",
            ConfigFactory.load((config)));

    final ActorRef actor = system.actorOf(Props.create(Manager.class),
            "Manager");

    System.out.println("Started ManagerSystem");
    final Random r = new Random();
    system.scheduler().schedule(Duration.create(1, SECONDS),
        Duration.create(1, SECONDS), new Runnable() {
          @Override
          public void run() {
            actor.tell(new Message.Greeting("hello"),null);
          }
        }, system.dispatcher());
  }
}
