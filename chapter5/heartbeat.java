// Executor

public class MyExecutor implements Executor, Runnable {
  public static main(String ...args) throws Exception {
    // ..skipped code
    Timer timer= new Timer();
    timer.schedule(new TimerTask() {
      public void run() {
        ed.sendFrameworkMessage(new byte[1]); // place to send more interesting data
      }
    }, 0, 5000);
  }
}

// Scheduler
public class Job {
  // the timer could be shared between all jobs for more efficiency
  private Timer timer= new Timer(); // when latest heartbeat expires
  private TimerTask missedHearbeatTask;

  public void heartbeat() {
    if(missedHearbeatTask != null) {
      missedHearbeatTask.cancel();
    }
    missedHearbeatTask= new HeartbeatTask();
    timer.schedule(missedHearbeatTask, 20000);
  }

  public static Job fromJSON(JSONObject obj) {
    Job job= new Job();
    // ... skipped code
    job.heartbeat();
    return job;
  }

  public void succeed() {
    // ... skipped code
    missedHearbeatTask.cancel();
  }

  public void fail() {
    // ... skipped code
    missedHearbeatTask.cancel();
  }

  private class HeartbeatTask extends TimerTask {
    public void run() {
      System.out.println("Heartbeat missed; failing");
      fail(); // fail job
      // need to kill the failing executors using the killTask API, not just log the death
      // also need to log the failed exeutor id per slave id so we don't try to relaunch same eid on sid
    }
  }
}

public class UselessRemoteBASH implements Scheduler {
  public void frameworkMessage(SchedulerDriver sd, ExecutorID eid, SlaveID sid, byte[] data) {
    String id= eid.getValue();
    synchronized(jobs) {
      for(Job j : jobs) {
        if(j.getId().equals(id)) {
          j.heartbeat();
        }
      }
    }
  }
}
