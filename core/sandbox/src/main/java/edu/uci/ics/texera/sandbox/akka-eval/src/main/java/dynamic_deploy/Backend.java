package dynamic_deploy;

import akka.actor.AbstractActor;

public class Backend extends AbstractActor {
  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(Message.Greeting.class, Message -> {
        System.out.println(Message.getText());
      })
      .build();
  }
}
