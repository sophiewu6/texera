package streaming;

// this pa

import akka.actor.*;
import com.typesafe.config.Config;

import com.typesafe.config.ConfigFactory;

import java.util.Scanner;


// for using google cloud, replace 127.0.0.1 with 10.138.0.3 to make
// frontend system running on some port, replace 127.0.0.1 with 10.138.0.2 to make
// worker system ruuning on some port
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
            "}"+"\nblocking-dispatcher {                  \n" +
            "  executor = thread-pool-executor      \n" +
            "  thread-pool-executor {               \n" +
            "    core-pool-size-min = 10            \n" +
            "    core-pool-size-max = 10            \n" +
            "  }                                    \n" +
            "}                                      \n" +
            "akka.actor.default-mailbox.mailbox-type = akka.dispatch.UnboundedMailbox\n");

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
