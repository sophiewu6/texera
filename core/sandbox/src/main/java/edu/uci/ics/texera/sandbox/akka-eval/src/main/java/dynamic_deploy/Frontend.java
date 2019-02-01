package dynamic_deploy;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.AbstractActor;

public class Frontend extends AbstractActor {

  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(Message.Greeting.class, message -> {
        ActorRef backend = getContext().actorOf(
          Props.create(Backend.class));
        System.out.println(backend.path()+"!\n");
        backend.tell(message, self());
      })
      .build();
  }
}
