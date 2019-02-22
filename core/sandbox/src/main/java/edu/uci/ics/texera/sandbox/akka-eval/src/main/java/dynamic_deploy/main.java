package dynamic_deploy;

// This package try to see if akka can deploy an actor from another actor and communicate with each others in run time.

import static java.util.concurrent.TimeUnit.SECONDS;
import java.util.Random;

import akka.actor.*;
import akka.remote.RemoteScope;
import scala.concurrent.duration.Duration;

import com.typesafe.config.ConfigFactory;
// To run:
// mvn compile exec:java -Dexec.mainClass="dynamic_deploy.main" -Dexec.args="Worker"
// mvn compile exec:java -Dexec.mainClass="dynamic_deploy.main" -Dexec.args="Manager"

public class main {

  public static void main(String[] args) {
    if (args.length == 0 || args[0].equals("Worker"))
      startBackendSystem();
    if (args.length == 0 || args[0].equals("Manager"))
      startFrontendSystem();
  }

  public static void startBackendSystem() {
    ActorSystem.create("BackendWorkerSystem",
        ConfigFactory.load(("backend_load")));
    System.out.println("Started BackendWorkerSystem");
  }

  public static void startFrontendSystem() {
    final ActorSystem system = ActorSystem.create("FrontendSystem",
        ConfigFactory.load("no_deploy"));
    Address addr = AddressFromURIString.parse("akka.tcp://BackendWorkerSystem@127.0.0.1:2552");
    final ActorRef actor = system.actorOf(Props.create(Frontend.class).withDeploy(
            new Deploy(new RemoteScope(addr))), "frontend");

    System.out.println("Started FrontendSystem");
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
