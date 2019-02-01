package pause;



import akka.actor.*;
import akka.remote.RemoteScope;
import java.util.ArrayList;
import static pause.Operator.BACKEND_REGISTRATION;

// Operator:
// s -> Scan    m -> Match
// p -> Pause   r -> Resume
// d -> getText

public class Manager extends AbstractActor {
    private static ArrayList<ActorRef> workers = new ArrayList<>();
    private static ArrayList<ActorRef> idles = new ArrayList<>();
    private int batch = 500;
    private ArrayList<String[]> source = new ArrayList<>();
    private int counter = 0;
    public ArrayList<String[]> text = new ArrayList<>();
    private Scan scan;


    public Receive createReceive() {
    return receiveBuilder()

            // register
            .match(Join.class, obj -> {
                Address addr = new Address("akka.tcp", "WorkerSystem",
                        "127.0.0.1", obj.port);
                getContext().actorOf(Props.create(Worker.class).withDeploy(
                        new Deploy(new RemoteScope(addr))));
            })
            .matchEquals(BACKEND_REGISTRATION, message -> {
                workers.add(sender());
                idles.add(sender());
                System.out.println(workers.size() + " workers in hold");
            })

            // scan
            .matchEquals("s", message -> {
                Scan.eof = false;
                scan = new Scan("input/median_input.csv");
                source.addAll(scan.getSourceObj(batch));
                self().tell("forward", self());
            })
            .matchEquals("forward", message -> {
                if (!Operator.PAUSE){
                    if (!Scan.eof){
                        source.addAll(scan.getSourceObj(batch)) ;
                        self().tell("forward", self());
                    }
                }
            })

            // match
            .matchEquals("m", message -> {
                System.out.println("start matching...");
                counter = 0;
                for (ActorRef worker: workers)
                {
                   worker.tell(new Matcher(
                           source.subList(counter, counter ++)), self());
               }
            })

            // sink
            .matchEquals("d", message -> {
                counter = 0;
                for (ActorRef worker: workers)
                        {
                            if (counter>=text.size())
                                break;
                            worker.tell(
                                    new Sink(text.subList(counter, counter +=batch)), self());
                        }
            })

            // pause & resume
            .matchEquals("p", message -> {
                System.out.println("pausing all worker");
                Operator.turnTrue();
                for (ActorRef worker: workers)
                    worker.tell("p",self());
            })
            .matchEquals("r", message -> {
                if (Operator.PAUSE){
                    if (!Scan.eof){
                        Operator.turnFalse();
                        self().tell("forward", self());
                    }else {
                        System.out.println("resuming all worker");
                        for (ActorRef worker: workers)
                            worker.tell("r",self());
                    }
                }
            })

            // respond
            .match(Operator.class, obj -> {
                int size = text.size();
                if (obj instanceof Matcher)
                {
                    text.addAll(obj.text);
                    size = source.size();

                }
                if (counter <= size) {
                    if ((counter + batch) > size) {
                        if (obj instanceof Matcher) {
                            sender().tell(new Matcher(
                                    source.subList(counter, size)), self());
                        }
                        else if (obj instanceof Sink) {
                            sender().tell(new Sink(
                                    text.subList(counter, size)), self());
                            System.out.println("operator finished");
                        }

                        counter +=batch;
                    }
                    else {
                        if (obj instanceof Matcher) {
                            sender().tell(new Matcher(
                                    source.subList(counter, counter +=batch)), self());
                        }
                        else if (obj instanceof Sink) {
                            sender().tell(new Sink(
                                    text.subList(counter, counter +=batch)), self());
                        }
                    }
                }
            })
            .build();
  }

}


