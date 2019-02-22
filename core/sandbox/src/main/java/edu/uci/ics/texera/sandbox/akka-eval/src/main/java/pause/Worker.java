package pause;

import akka.actor.AbstractActor;
import akka.actor.ActorSelection;

import static pause.Operator.BACKEND_REGISTRATION;


public class Worker extends AbstractActor {
  private Operator buff = new Operator();

  @Override
  public void preStart() {
    ActorSelection manager = getContext().actorSelection(
            "akka.tcp://ManagerSystem@127.0.0.1:2551/user/manager");
    manager.tell(BACKEND_REGISTRATION, self());
  }
  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(Matcher.class, matcher -> {
        System.out.println("Match request receive");
        if (!Operator.PAUSE)
        {
          matcher.getText();
          sender().tell(matcher, self());
          System.out.println("searching batch finished \n");
        }
        else {
          System.out.println("paused");
          matcher.getText();
          buff = matcher;
//          buff.text = matcher.getText();
        }
      })
      .match(Sink.class, sink -> {
        System.out.println("Sink request receive");
        if (!Operator.PAUSE)
        {
          sink.getText();
          sender().tell(sink, self());
          System.out.println("sinking batch finished \n");
        }
        else
        {
          System.out.println("paused");
          buff = sink;
        }

      })

      .matchEquals("p", message -> {
        System.out.println("Pause request receive");
        System.out.println("pausing");
        Operator.turnTrue();
      })
      .matchEquals("r", message -> {
        System.out.println("Resume request receive");
        System.out.println("resumed");
        sender().tell(buff, self());
        buff = null;
        Operator.turnFalse();
      })

//            .match(Operator.class, op -> {
//              op.getText();
//              if (!Operator.PAUSE)
//              {
//                sender().tell(op, self());
//                System.out.println("Operation  finished \n");
//              }
//              else {
//                System.out.println("paused");
//                buff = op;
//              }
//            })
      .build();
  }
}
