package my.cluster;

import akka.event.Logging;
import akka.event.LoggingAdapter;

import akka.actor.AbstractActor;

public class Worker extends AbstractActor {

  LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  @Override
  public Receive createReceive() {
    return receiveBuilder()
            .match(String.class, word -> {
              log.info("\nWorker received info");
              System.out.println(word);
            })
            .match(Messages.TextJob.class, job -> !job.getText().isEmpty(), job -> {
                log.info("\nWorker now receive info");
                String words = job.getText();
                System.out.println(words);
            })
            .build();
  }
}
