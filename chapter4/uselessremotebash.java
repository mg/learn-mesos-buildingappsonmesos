package com.example
import java.nio.file.*;
import java.util.*;
import org.apache.mesos.*;
import org.apache.mesos.Protos.*;

public class UselessRemoteBASH implements Scheduler {
  // status callbacks
  public void registered(SchedulerDriver driver, FrameworkID fid, Masterinfo mi) {
    System.out.println("Registered with framework id " + fid);
  }

  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    System.out.println("Got status update " + status);
  }

  // main callback, receives offers, launches tasks
  private boolean submitted= false;
  public void resourceOffers(SchedulerDriver driver, java.util.List<Offer> offers) {
    synchronized(this) {
      if(submitted) {
        for(Offer o : offers) {
          dirver.declineOffer(o.getId());
        }
        return;
      }
      submitted= true;

      // here we should check if offer is big enough (cpus, mem, etc) before launching
      // if not we would get a TASK_LOST message
      // we should then decline all the other offers offered to free them up
      Offer offer= offers.get(0);
      TaskInfo ti= makeTask(offer.getSlaveId());
      driver.launchTasks(
        Collections.singletonList(offer.getId()), // list of offers per one slave
        Collections.singletonList(ti) // list of tasks to launch on slave
      );
      System.out.println("Launched offer " + ti);
    }
  }

  // create task to launch
  public static TaskInfo makeTask(SlaveID targetSlave) {
    double cpus= 1.0;
    double mem= 100.0;
    String command= "echo hello world";

    // TaskID must be unique to framework
    // ExecuterID must be unique to slave
    // SlaveID must be unique to cluster
    UUID uuid= UUID.randomUUID();
    TaskID id= TaskID
      .newBuilder()
      .setValue(uuid.toString())
      .build();

    // TaskInfo is a protobuf that explains how to launch a task,
    // resources needed, the command to launch, and then to
    // tie it to an offer (the target slave)
    return TaskInfo
      .newBuilder()
      .setName("useless_remote_bash_task_" + id.getValue())
      .setTaskId(id)
      .addResources(scalarResource("cpus", cpus))
      .addResources(scalarResource("mem", mem))
      .setCommand(
        CommandInfo
          .newBuilder()
          .setValue(command)
      )
      .setSlaveId(targetSlave)
      .build();
  }

  private static scalarResource(String id, double value) {
    return Resource
      .newBuilder()
      .setName(id)
      .setType(Value.Type.SCALAR)
      .setScalar(
        Value.Scalar
          .newBuilder()
          setValue(value)
      )
  }

  // run in Mesos
  public static void main(String ...args) throws Exception {
    FrameworkInfo fi= FrameworkInfo
      .newBuilder()
      .setUser("")
      .setName("Useless Remote BASH") // or $username + string to identify user
      .build();

    Scheduler scheduler= new UselessRemoteBASH();

    SchedulerDriver driver= new MesosSchedulerDriver(
      scheduler,
      fi,
      "zk://" + args[0] + "/mesos"
    );

    // or driver.run() to do both
    driver.start();
    driver.join();
  }

  // ignored callbacks, n.b. executerLost doesn't do anything, see MESOS-313
  public void disconnect(SchedulerDriver sd) {}
  public void error(SchedulerDriver sd, String message) {}
  public void executerLost(SchedulerDriver sd, ExecutorID executorId, SlaveID slaveId, int status) {}
  public void frameworkMessage(SchedulerDriver sd, ExecutorID executorId, SlaveID, slaveId, byte[] data) {}
  public void offerRescinded(SchedulerDriver sd, OfferID offerId) {}
  public void reregistered(SchedulerDriver sd, MasterInfo mi) {}
  public void slaveLost(SchedulerDriver sd, SlaveID slaveId) {}
}
