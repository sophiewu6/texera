## A Simple Cluster: two machine hello world (package new_cluster)
This program illustrate different Akka cluster features.

- Subscribe to cluster membership events
- Sending messages to actors running on nodes in the cluster
- Cluster aware routers
- Cluster metrics


### File description

Open application.conf.

To enable cluster capabilities in Akka project, adding the remote settings, and use `cluster` for `akka.actor.provider`. The `akka.cluster.seed-nodes` should normally also be added to application.conf file.

The seed nodes are configured contact points which newly started nodes will try to connect with in order to join the cluster.

In order to start the nodes on different machines, specifying the two ip-addresses of the machines in `application.conf`.

Open Main.java.

The small program together with its configuration starts an ActorSystem with the Cluster enabled. It joins the cluster and starts an actor that logs some membership events.
Concepts about the cluster  in the [documentation](http://doc.akka.io/docs/akka/2.5/java/cluster-usage.html).

Specifying two ports:

In the first machine, 2551 corresponds to the port of the first seed-nodes element in the configuration. In the log output you see that the cluster node has been started and changed status to 'Up'.

In the second machine, 2552 corresponds to the port of the second seed-nodes element in the configuration. In the log output you see that the cluster node has been started and joins the other seed node and becomes a member of the cluster. Its status changed to 'Up'.

Switch over to the first  window and see in the log output that the member joined.

Open Messages.java. 
It defines the messages that are sent between the actors. In this case, hard-code message is "Hello !!!!"".


The service will receives a text object from this class requested by client and display the text. Note that Routee is the service in this case.


Open cluster.conf. The router is configured with `routees.paths`. This means that user requests can be sent to `Service` node.

### run
     
     mvn compile exec:java -Dexec.mainClass="package_name.main" -Dexec.args="role port"
    
Argument 2551 will start client actor on akka://ClusterSystem@10.138.0.3:2551. 

Argument 2552 will start service actor on akka://ClusterSystem@10.138.0.2:2552.

Two actors is under same actor system called ClusterSystem with same configuration.

Shut down one of the nodes by pressing 'ctrl-c' in one of the machine. The other nodes will detect the failure after a while, which you can see in the log output in the other machine.

the actor registers itself as subscriber of certain cluster events. It gets notified with an snapshot event, `CurrentClusterState` that holds full state information of the cluster. After that it receives events for changes that happen in the cluster.



## Cluster Aware Routers

All routers can be made aware of member nodes in the cluster, i.e. deploying new routees or looking up routees on nodes in the cluster. When a node becomes unreachable or leaves the cluster the routees of that node are automatically unregistered from the router. When new nodes join the cluster additional routees are added to the router, according to the configuration. Routees are also added when a node becomes reachable again, after having been unreachable.

## Remote deploy

Loading config:

    Config config =  ConfigFactory.parseString("akka {
                                                  actor {
                                                    provider = "remote"
                                                    warn-about-java-serializer-usage = false
                                                  }
                                                  remote {
                                                    netty.tcp {
                                                      hostname = "127.0.0.1"
                                                      port = custom_port
                                                    }
                                                  }");
                
This enables the remoting by installing the RemoteActorRefProvider and chooses the default remote transport. Be sure to replace the default IP 127.0.0.1 with the real address the system is reachable if you deploy onto multiple machines!

For actor system that will deploy remotely another actor, loading a extra section:

    Config config =  ConfigFactory.parseString("akka {
                                                      actor {
                                                        provider = "remote"
                                                        warn-about-java-serializer-usage = false
                                                        deployment {
                                                                    /frontend/* {
                                                                        remote = "akka.tcp://frontendSystem@127.0.0.1:2552"
                                                                           }
                                                                    }
                                                               }
                                                      remote {
                                                        netty.tcp {
                                                          hostname = "127.0.0.1"
                                                          port = custom_port
                                                            }
                                                      }");

The configuration contains a deployment section that matches these child actors and defines that the actors are to be deployed at the remote system. The wildcard (*) is needed because the child actors are created with unique anonymous names.

Creating system:

    final ActorSystem system = ActorSystem.create("WorkerSystem",
                ConfigFactory.load((config)));

    // loading extra section
    final ActorSystem system = ActorSystem.create("ManagerSystem",
                ConfigFactory.load((config)));
                
Creating Manager actor under main class
    
    final ActorRef actor = system.actorOf(Props.create(Manager.class),
                "manager");
                
Creating remote actor(worker) under class of Manager actor 

    ActorRef backend = getContext().actorOf(
              Props.create(Worker.class));
         
         
## Simple work flow (psudocode)

After Manager and workers estabilish connection and manager is able to receive request.
    
    Manager.matchEqual("request", 
                self().tell(operator to workers))
                
    Worker.match(Operator,
            run operator,
            sender.tell(result to manager),
            request next operation))
    
    Manager.match(result,
            store result,
            self().tell(next operation if availlble))
            
The pattern will go as 
1) The manager receives request from front-end.
2) The manager identifies the request type.
3) The manager acts and send to workers, correspongding to the request type.
4) Available workers receives the task, identify task's type, and start to process.
5) the workers send back the result and request a next task from the manager.
            
##   Pause and Resume work flow

 
     
     Manager.matchEqual("request", 
                     self().tell(operator to workers or 
                     Pause or Resume request to workers))
                     
     Worker.match(Operator,
             run operator,
             check operator's pause flag
                0)store current result into buffer
                no request for next operation
             
                1)sender.tell(result to manager),
                request for next operation))
             
     Worker.matchEqual("pause",
            Operator.turnTrue())
            
     Worker.matchEqual("resume",
                 Operator.turnFalse())
                 sender.tell(buffer)
                 request for next operation
                 clear buffer
                 
     
     Manager.match(result,
             store result,
             self().tell(next operation if availlble))
             
             
Based simple work flow, the pattern will go as  <br />
2.b  The manager identifies that the request is "Pause" or "Resume".   <br />
4.b  Available workers receive the task, identify "Pause" or "Resume" request, and turn static boolean accordingly.   <br />
5.b  After processing source text, store the result into buffer, and do not request next task, When "Pause" active; 
     When "Resume" request receives,Workers send back the result and request a next task.  <br />
      

request type specifer: 
s -> Scan    m -> Match
d -> Sink
p -> Pause   r -> Resume    


## Message Delivery

1. at-most-once delivery, i.e. no guaranteed delivery
2. message ordering FIFO maintain by sender–receiver pair
   Actor A1 sends messages M1, M2, M3 to A2
   Actor A3 sends messages M4, M5, M6 to A2 
   The order of message for from A1 to A2 follows M1, M2, M3.
   The order of message for from A3 to A2 follows M4, M5, M6.
   A2 can see messages from A1 interleaved with messages from A3. (M1 may received by A2 after A2 receives M4; M5 may receive before M1 by A2)
Note that mailbox implementing PriorityMailBox does not respect FIFO.