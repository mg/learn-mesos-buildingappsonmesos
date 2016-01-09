package com.example
import java.nio.file.*;
import java.util.*;
import org.apache.mesos.*;
import org.apache.mesos.Protos.*;

public class Job {
  // ...
  public double getCpus() {
    return cpus;
  }

  public double getMem() {
    return mem;
  }

  // keep track of job progress
  enum JobState {
    PENDING,
    STAGING,
    RUNNING,
    SUCCESSFUL,
    FAILED
  }
  private JobState status;
  private int retries;

  private Job() {
    status= JobState.PENDING;
    retries= 3;
  }

  public void launch() {
    status= JobState.STAGING;
  }

  public void started() {
    status= JobState.RUNNING;
  }

  public void suceed() {
    status= JobState.SUCCESSFUL;
  }

  public void fail() {
    if(retries == 0) {
      status= JobState.FAILED;
    } else {
      retries--;
      status= JobState.PENDING;
    }
  }

  // new statusUpdate handler
  public void statusUpdate(SchedulerDriver sd, TaskStatus status) {
    synchronized(jobs) {
      for(Job j : jobs) { // store a status.taskId => job.id map
        if(j.getId().equals(status.getTaskId().getValue())) {
          switch(status.getState()) {
            case TASK_RUNNING:
              j.started();
              break;
            case TASK_FINISHED:
              j.succeed();
              break;
            case TASK_FAILED: // task reported failure
            case TASK_KILLED: // scheduler stopped task
            case TASK_LOST: // unexpted error occurred
            case TASK_ERROR: // indicates a problem with the task descriptor
              j.fail();
              break;
            default:
              break;
          }
        }
      }
    }
  }
}

public class UselessRemoteBASH implements Scheduler {
  // ...

  public void resourceOffers(SchedulerDriver sd, java.util.List<Offer> offers) {
    // ...
    sd.launchTasks(
      Collections.singletonList(o.getId()),
      doFirstFit(o, pendingJobs)
    );
  }

  public List<TaskInfo> doFirstFit(Offer offer, List<Job> jobs) {
    List<TaskInfo> toLaunch= new ArrayList<>();
    List<Job> launchedJobs= new ArrayList<>();

    // sum resourcers on offer
    double offerCpus= 0;
    double offerMem= 0;
    for(Resource r : offer.getResourcesList()) {
      if(r.getName().equals("cpus")) {
        offerCpus += r.getScalar().getValue();
      } else if(r.getName().equals("mem")) {
        offerMem += r.getScalar().getValue();
      }
    }

    // iterate jobs, launch jobs that fit
    // better algorithms exists, but this is
    // ulitmately a NP-complete problem (the
    // knapsack problem) if we want to always
    // find best fit give offers and jobs
    // a strategy is to pack the solution along
    // the most constraint dimension (e.g. cpu's)
    for(Job j : jobs) {
      double jobCpus= j.getCpus();
      double jobMem= j.getMem();
      if(jobCpus <= offerCpus && jobMem <= offerMem) {
        offerCpus -= jobCups;
        offerMem -= jobMem;
        toLaunch.add(j.makeTask(offer.getSlaveId()));
        j.setSubmitted(true)
        j.launch();
        launchedJobs.add(j);
      }
    }

    jobs.removeAll(launchedJobs);
    return toLaunch;
  }
}
