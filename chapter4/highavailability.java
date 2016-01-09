public class Job {
  private String id;

  private void saveState() {
    JSONObject ojb= new JSONObject();
    obj.put("id", id);
    obj.put("status", (status == JobState.STAGING ? JobState.RUNNING : status).toString());
    // store other fields
    byte[] data= obj.toString().getBytes("UTF-8");
    try {
      curator.setData().forPath("/sampleframework/jobs/" + id, data);
    } catch(KeeperException.NoNodeException e) {
      curator.create().creatingParentsIfNeeded().forPath("/sampleframework/jobs/" + id, data);
    }
  }

  // modify started(), suceed() and failed() same way
  public void launch() {
    // previous code
    saveState();
  }
}

public class UselessRemoteBASH implements Scheduler {
  // add simple leader election
  public static void main(String ...args) throws Exception {
    CuratorFamework curator= CuratorFamework.newClient(
      args[0], // address of the ZooKeeper cluster
      new RetryOneTime(1000) // ExponentialBackoffRetry, BoundedExponentialBackoffRetry
    );
    curator.start();

    // LeaderLatch is naive and does not handle being desposed. Use LeaderSelector insted.
    LeaderLatch leaderLatch= new LeaderLatch(curator, "/sampleframework/leader"); // path on zk
    leaderLatch.start();
    leaderLatch.await(); // when this returns, this process is the leader

    FrameworkInfo.Builder fiBuilder= FrameworkInfo
      .newBuilder()
      .setUser("")
      .setName("Useless Remote BASH");

    try {
      // fetch stored fi id from zk, this is the leader id
      byte[] curatorData= curator.getData().forPath("/sampleframework/id");
      fiBuilder.setId(new String(curatorData, "UTF-8"));
    } catch(KeeperException.NoNodeException e) {
      // no id stored on zk, allow Mesos to assign it
    }

    FrameworkInfo fi= fiBuilder
      .setFailoverTimeout(60*60*24*7) // if scheduler fails, we have a week to spin up a new one
      .build();

    List<Job> jobs= new ArrayList<>();
    if(args.length > 1) {
      // load jobs from file
      byte[] data= Files.readAllBytes(Paths.get(args[1]));
      JSONObject config= new JSONObject(new String(data, "UTF-8"));
      JSONArray jobsArray= config.getJSONArray("jobs");
      for(int i= 0; i < jobsArray.length(); i++) {
        jobs.add(Job.fromJSON(jobsArray.getJSONObject()));
      }
    }

    try {
      // load jobs from zk
      for(String id : curator.getChildren().forPath("/sampleframework/jobs")) {
        byte[] data= curator.getData().forPath("/sampleframework/jobs/" + id);
        JSONObject config= new JSONObject(new String(data, "UTF-8"));
        jobs.add(Job.fromJSON(jobsArray.getJSONObject()));
      }
    } catch(Exception e) {
      // handle error
    }

    // .. launch scheduler
  }

  // callback
  public void registered(SchedulerDriver sd, FrameworkID fId, MasterInfo mi) {
    System.out.println("Registered with framework id" + fId);
    try {
      // store framework id in zk
      curator.create().creatingParentsIfNeeded().forPath(
        "/sampleframework/id",
        frameworkId.getBytes()
      );
    } catch(KeeperException.NodeExistsException e) {
      // do nothing
    }
  }
}
