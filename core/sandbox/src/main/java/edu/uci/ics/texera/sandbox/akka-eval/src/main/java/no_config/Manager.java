package no_config;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.AbstractActor;

public class Manager extends AbstractActor {
    // backend at port 2551 frontend at 2552
// backend actor full address:
// akka.tcp://BackendSystem@127.0.0.1:2551/remote/akka.tcp/FrontendSystem@127.0.0.1:2552/user/frontend/$e  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(Message.Greeting.class, message -> {
        ActorRef backend = getContext().actorOf(
          Props.create(Worker.class));
        backend.tell(message, self());
//        System.out.println(backend.path());
      })
      .build();
  }
}
