package pause;

// this package test if akka can pause and resume with simple workflow

import akka.actor.*;
import com.typesafe.config.Config;

import com.typesafe.config.ConfigFactory;

import java.util.Scanner;


// for using google cloud, replace 127.0.0.1 with 10.138.0.3 to make
// frontend system running on some port, replace 127.0.0.1 with 10.138.0.2 to make
// worker system ruuning on some port

// To run:
// mvn compile exec:java -Dexec.mainClass="pause.main" -Dexec.args="Manager 2551"
// mvn compile exec:java -Dexec.mainClass="pause.main" -Dexec.args="Worker 2552"

public class main {

  public static void main(String[] args) {
    if (args[0].equals("Worker"))
      startWorkerSystem(args[1]);
    if (args[0].equals("Manager"))
      startManagerSystem(args[1]);
  }

  public static void startWorkerSystem(String port) {
    Config config =  ConfigFactory.parseString("akka {\n" +
            "  actor {\n" +
            "    provider = remote\n" +
            "    warn-about-java-serializer-usage = false \n" +
            "  }\n" +
            "  remote {\n" +
            "    netty.tcp {\n" +
            "      hostname = \"127.0.0.1\"\n" +
            "   port = " + port + "}\n" +
            "  }\n" +
            "}");

    final ActorSystem system = ActorSystem.create("WorkerSystem",
        ConfigFactory.load((config)));
    ActorSelection managerRef = system.actorSelection(
            "akka.tcp://ManagerSystem@127.0.0.1:2551/user/manager");
    managerRef.tell(new Join(port),null);
    System.out.println("Started WorkerSystem");
  }


  public static void startManagerSystem(String port) {

    Config config =  ConfigFactory.parseString("akka {\n" +
            "  actor {\n" +
            "    provider = remote \n" +
            "    warn-about-java-serializer-usage = false \n" +
            "}\n" +
            "  remote {\n" +
            "    netty.tcp {\n" +
            "      hostname = \"127.0.0.1\"\n" +
            "    port = " + port + "}\n" +
            "  }\n" +
            "}");

    final ActorSystem system = ActorSystem.create("ManagerSystem",
            ConfigFactory.load((config)));
    ActorRef manager = system.actorOf(Props.create(Manager.class),
            "manager");
    System.out.println("Started ManagerSystem");

    Scanner scanner = new Scanner( System.in );

    while (true){
      String op = scanner.nextLine();
      System.out.println(op);
      manager.tell(op,null);
    }

  }
}
