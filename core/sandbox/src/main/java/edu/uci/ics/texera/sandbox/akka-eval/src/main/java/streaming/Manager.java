package streaming;

import akka.actor.*;

import akka.remote.RemoteScope;

import java.io.Serializable;
import java.util.ArrayList;

import static akka.pattern.PatternsCS.ask;
import static pause.Operator.BACKEND_REGISTRATION;


//#stream-imports
import akka.stream.*;
import akka.stream.javadsl.*;
//#stream-imports

//#other-imports
import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.util.ByteString;
import akka.util.Timeout;

import java.nio.file.Paths;
import java.math.BigInteger;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import akka.remote.*;


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
    //#create-materializer
    final Materializer materializer = ActorMaterializer.create(getContext());

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
                scan = new Scan("input/small_input.csv");
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
//            .matchEquals("r", message -> {
//                if (Operator.PAUSE){
//                    if (!Scan.eof){
//                        Operator.turnFalse();
//                        self().tell("forward", self());
//                    }else {
//                        System.out.println("resuming all worker");
//                        for (ActorRef worker: workers)
//                            worker.tell("r",self());
//                    }
//                }
//            })
//
//            // respond
//            .match(Operator.class, obj -> {
//                int size = text.size();
//                if (obj instanceof Matcher)
//                {
//                    text.addAll(obj.text);
//                    size = source.size();
//
//                }
//                if (counter <= size) {
//                    if ((counter + batch) > size) {
//                        if (obj instanceof Matcher) {
//                            sender().tell(new Matcher(
//                                    source.subList(counter, size)), self());
//                        }
//                        else if (obj instanceof Sink) {
//                            sender().tell(new Sink(
//                                    text.subList(counter, size)), self());
//                            System.out.println("operator finished");
//                        }
//
//                        counter +=batch;
//                    }
//                    else {
//                        if (obj instanceof Matcher) {
//                            sender().tell(new Matcher(
//                                    source.subList(counter, counter +=batch)), self());
//                        }
//                        else if (obj instanceof Sink) {
//                            sender().tell(new Sink(
//                                    text.subList(counter, counter +=batch)), self());
//                        }
//                    }
//                }
//            })

            .matchEquals("test", message -> {
                System.out.println("streaming send");
                Source<String, NotUsed> words =
                        Source.from(Arrays.asList("hello", "hi"));


                ActorRef receiver =
                        workers.get(0);

                akka.stream.javadsl.Sink<String, NotUsed> sink =
                        akka.stream.javadsl.Sink.<String>actorRefWithAck(receiver,
                        new StreamInitialized(),
                        Ack.INSTANCE,
                        new StreamCompleted(),
                        ex -> new StreamFailure(ex)
                );

                words
                        .map(el -> el.toLowerCase())
                        .runWith(sink, materializer);
            })
            .match(StreamInitialized.class, init -> {
                System.out.println("Stream initialized");
                workers.get(0).tell("Stream initialized", getSelf());
                sender().tell(Ack.INSTANCE, self());
            })
            .match(String.class, element -> {
                System.out.println("Received element: {}"+ element);
                workers.get(0).tell(element, getSelf());
                sender().tell(Ack.INSTANCE, self());
            })
            .match(StreamCompleted.class, completed -> {
                System.out.println("Stream completed");
                workers.get(0).tell("Stream completed", getSelf());
            })
            .match(StreamFailure.class, failed -> {
                System.out.println("Stream failed!");
                workers.get(0).tell("Stream failed!", getSelf());
            })
            .build();
  }

    enum Ack {
        INSTANCE;
    }
    interface abstr extends Serializable { }
    static class StreamInitialized implements abstr {}
    static class StreamCompleted {}
    static class StreamFailure {
        private final Throwable cause;
        public StreamFailure(Throwable cause) { this.cause = cause; }

        public Throwable getCause() { return cause; }
    }


}


