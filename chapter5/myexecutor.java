package com.example
import org.apache.mesos.*;
import org.apache.mesos.Protos.*;
import java.util.*;
import java.io.File;
import org.json.*;
import java.lang.ProcessBuilder.Redirect; // to redirect stdio to a logging endpoint

public class MyExecutor implements Executor, Runnable {
  public static main(String ...args) throws Exception {
    // all configuration comes from Mesos
    Executor executor= new MyExecutor();
    ExecutorDriver ed= new MesosExecutorDriver(executor);

    // this expects $MESOS_SLAVE_PID to exists, can be problematic when testing and developing
    ed.run();

    // runs until we call ExecutorDriver.stop(), TASK_FINSIHED
    // does not stop an executor since it is not bound to a task
  }

  // callbacks
  public void registered(ExecutorDriver ed, ExecutorInfo ei, FrameworkInfo fi, SlaveInfo si) { // first time connected to a slave
    System.out.prinln("Registered executor " + ei);
  }

  // ignored callbacks
  public void frameworkMessage(ExecutorDriver ed, byte[] data) {} // not guaranteed to be recieved
  public void disconnected(ExecutorDriver ed) {} // disconnected from a slave, usually means a restart
  public void shutdown(ExecutorDriver ed) {} // expected to gracefull shut down
  public void reregistered(ExecutorDriver ed, SlaveInfo si) {} // sucessful slave restart
  public void error(ExecutorDriver ed, String message) {} // fatal error, driver is no longer running

  // references needed in the executor
  private Process proc; // reference to launched process, need to wait for it to complete
  private TaskID tid; // to communicate the task's status to Mesos
  private ExecutorDriver driver; // need to use the driver

  // boilerplate to keep Mesos up-to-date
  private void statusUpdate(TaskState state) { statusUpdate(state, null); }
  private void statusUpdate(TaskState state, String message) {
    TaskStatus status= TaskStatus
      .newBuilder()
      .setTaskId(tid)
      .setState(state);

    if(message != null) status= status.setMessage(message);
    driver.sendStatusUpdate(status.build());
  }

  // for simplicity, only support running a single task after which we exit
  private boolean ensureOneLaunch(ExecutorDriver ed, TaskId tid) {
    if(this.tid != null) {
      statusUpdate(TaskState.TASK_ERROR, "this executor only can run a single task")
      return false;
    } else {
      return true;
    }
  }

  // start a process inside bash, redirect stdout and stderr to log files
  private Process startProcess(JSONObject cfg) throws Exception {
    List<String> cmdArgs= Arrays.asList("bash", "-c", cfg.getString("cmd"));
    ProcessBuilder pb= new ProcessBuilder(cmdArgs);
    File stdout= new File(System.getenv("MESOS_DIRECTORY"), "child_stdout");
    File stderr= new File(System.getenv("MESOS_DIRECTORY"), "child_stderr");
    pb.redirectOutput(Redirect.to(stdout));
    pb.redirectError(Redirect.to(stderr));
    return pb.start();
  }

  // Executor callback
  public void launchTask(ExecutorDriver ed, TaskInfo ti) {
    synchronized(this) {
      try {
        if(!ensureOneLaunch(ed, ti.getTaskId())) {
          return;
        }

        this.tid= task.getTaskId();
        this.driver= ed;

        statusUpdate(TaskState.TASK_STARTING);

        byte[] taskData= task.getData().toByteArray();
        JSONObject cfg= new JSONOBject(new Stirng(taskData, "UTF-8"));
        this.proc= startProcess(cfg);

        statusUpdate(TaskState.TASK_RUNNING);

        Thread t= new Thread(this);
        t.setDaemon(true);
        t.start();
      } catch(Exception e) {
        e.printStackTrace();
      }
    }
  }

  // monitor waiting for task to finish, started by Thread.start()
  public void run() {
    int exitCode;
    try {
      exitCode= proc.waitFor(); // wait for our process to finish
    } catch(Exception e) {
      exitCode= -99; // something went terribly wrong
    }

    synchronized(this) {
      if(proc == null) { // process was killed by killTask
        return;
      }
      proc= null;
      if(exitCode == 0) {
        statusUpdate(TaskState.TASK_FINSIHED);
      } else {
        statusUpdate(TaskState.TASK_FAILED, "Process exited with code " + exitCode);
      }
    }

    driver.stop(); // shut down executor
  }

  // callback
  public void killTask(ExecutorDriver ed, TaskID tid) {
    synchronized(this) {
      if(proc != null && tid.equals(this.tid)) {
        proc.destroy();
        statusUpdate(TaskState.TASK_KILLED);
        proc= null;
      }
      driver.stop();
    }
  }
}
