package com.example
import java.nio.file.*;
import java.util.*;
import org.apache.mesos.*;
import org.apache.mesos.Protos.*;

public class Job {
  private double cpus;
  private double mem;
  private String command;
  private boolean submitted= false;

  public boolean isSubmitted() {
    return submitted;
  }

  public void setSubmitted(boolean submitted) {
    this.submitted= submitted;
  }

  public static TaskInfo makeTask(SlaveID targetSlave) {
    UUID uuid= UUID.randomUUID();
    TaskID id= TaskID
      .newBuilder()
      .setValue(uuid.toString())
      .build();

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

  public static Job fromJSON(JSONObject obj) throws JSONException {
    Job job= new Job();
    job.cpus= obj.getDouble("cpus");
    job.mem= obj.getDouble("mem");
    job.command= obj.getString("command");
    return job;
  }
};

public class UselessRemoteBASH implements Scheduler {
  // keep track of jobs, pending and running
  private List<Job> jobs;

  UselessRemoteBASH(List<Job> jobs) {
    this.jobs= jobs;
  }

  /// .. callbacks

  public void resourceOffers(SchedulerDriver sd, java.util.List<Offer> offers) {
    synchronized(jobs) {
      List<Job> pendingJobs= new ArrayList();
      for(Job j : jobs) {
        if(!j.isSubmitted()) {
          pendingJobs.add(j);
        }
      }

      for(Offer o : offers) {
        if(pendingJobs.isEmpty()) {
          // decline offers not needed
          driver.declineOffer(o.getId());
          continue;
        }
        Job j= pendingJobs.remove(0);
        TaskInfo ti= j.makeTask(o.getSlaveId());
        sd.launchTasks(
          Collections.singletonList(o.getId()),
          Collections.singletonList(ti)
        );
        j.setSubmitted(true);
      }
    }
  }

  public static void main(String ...args) throws Exception {
    byte[] data= Files.readAllBytes(Paths.get(args[1]));
    JSONObject config= new JSONObject(new String(data, "UTF-8"));
    JSONArray jobsArray= config.getJSONArray("jobs");

    List<Job> jobs= new ArrayList<>();
    for(int i= 0; i < jobsArray.length(); i++) {
      jobs.add(Job.fromJSON(jobsArray.getJSONObject()));
    }

    FrameworkInfo fi= FrameworkInfo
      .newBuilder()
      .setUser("")
      .setName("Useless Remote BASH")
      .build();

    Scheduler scheduler= new UselessRemoteBASH(jobs);

    SchedulerDriver driver= new MesosSchedulerDriver(
      scheduler,
      fi,
      "zk://" + args[0] + "/mesos"
    );

    driver.run();
  }
}
