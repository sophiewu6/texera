package old_pause;

import akka.actor.*;
import akka.remote.RemoteScope;

import java.util.ArrayList;
import java.util.Scanner;

import static old_pause.Operator.BACKEND_REGISTRATION;


public class Manager extends AbstractActor {
    private static ArrayList<ActorRef> workers = new ArrayList<>();
    private static ArrayList<ActorRef> idles = new ArrayList<>();
    Scanner scanner = new Scanner( System.in );
    int batch = 500;
    private ArrayList<String[]> sourceObj = new ArrayList<>();

    public Receive createReceive() {
    return receiveBuilder()
            .match(Operator.Join.class, obj -> {
                Address addr = new Address("akka.tcp", "WorkerSystem",
                        "127.0.0.1", obj.port);
                getContext().actorOf(Props.create(Worker.class).withDeploy(
                        new Deploy(new RemoteScope(addr))));
            })
            .matchEquals(BACKEND_REGISTRATION, message -> {
                System.out.print("adding worker?(y/n): ");

                if (scanner.nextLine().equals("y"))
                {
                    workers.add(sender());
                    idles.add(sender());
                    System.out.println(workers.size() + " workers in hold");
                }
                System.out.print("begin search?(y/n): ");
                if ( scanner.nextLine().equals("y"))
                {
                    for (int i = 0; i < sourceObj.size();)
                    {
                        for (ActorRef worker: workers)
                        {
                            if ((i+batch) > sourceObj.size())
                            {
                                worker.tell(new Operator.Matcher(
                                        sourceObj.subList(i,sourceObj.size())), self());
                                System.out.println("bbbbb");
                                i+=batch;
                                break;
                            }
                            else {
                                worker.tell(new Operator.Matcher(
                                        sourceObj.subList(i,i+=batch)), self());
                                System.out.println("aaaaaaa");
                                if (i>=sourceObj.size())
                                    break;
                            }
                        }
                    }
                }
            })
            .matchEquals("s", message -> {
//                System.out.print("enter input path: ");
                Scanner scanner = new Scanner( System.in );
                String path = scanner.nextLine();
                sourceObj = new Operator.Scan("input/small_input.csv").getSourceObj();

            })
            .matchEquals("p", message -> {
                System.out.print("pausing all worker");
            })
            .match(Operator.Sink.class, Obj -> {
                System.out.println("sinking");
                sender().tell(Obj, self());
            })
            .build();
  }
}
