package my.cluster;

import akka.actor.AbstractActor;
import akka.actor.Address;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.LinkedList;
import java.util.List;

public class Service extends AbstractActor {
    String port;
    LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    final Cluster cluster = Cluster.get(getContext().system());

    public Service (String port){this.port = port;}

    @Override
    public void preStart() {
        System.out.println("Programatically joining to seed nodes with" + port);

        List<Address> list = new LinkedList<>();
        Address addr = new Address(
                "akka", "ClusterSystem", "127.0.0.1", 2551);
        list.add(addr);
        cluster.joinSeedNodes(list);
    }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(Messages.TextJob.class, job -> !job.getText().isEmpty(), job -> {
        String words = job.getText();
        log.info("\nService received info\n" + words);
      })

      .build();
  }
}
