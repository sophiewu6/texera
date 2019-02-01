package no_config;

import akka.actor.AbstractActor;

public class Worker extends AbstractActor {
  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(Message.Greeting.class, Message -> {
        System.out.println(Message.getText());
      })
      .build();
  }
}
