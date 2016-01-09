public class UselessRemoteBASH implements Scheduler {
  public TaskInfo makeTask(SlaveID sid, FrameworkID fid) {
    TaskID tid= TaskID
      .newBuilder()
      .setValue(this.id) // command executor uses same id for task and executor
      .build();

    ExecutorID eid= ExecutorID
      .newBuilder()
      .setValue(this.id) // see above
      .build();

    CommandInfo ci= CommandInfo
      .newBuilder()
      .setValue("java -jar /path/to/custom/executor.jar") // needs to be distributed to slaves
      .build();

    ExecutorInfo ei= ExecutorInfo
      .newBuilder()
      .setExecutorId(eid)
      .setFrameworkId(fid)
      .setCommand(ci)
      .build();

    JSONObject cfg= new JSONObject();
    try {
      cfg.put("cmd", this.command)
      return TaskInfo
        .newBuilder()
        // .. unchanged code, minus setCommand()
        .setExecutor(ei) // task info must have exactly one of either CommandInfo or ExecutorInfo
        .setData(ByteString.copyFrom(cfg.toString().getBytes("UTF-8")))
        .build();
    } catch(Exception e) {
      e.printStackTrace();
      throw new RuntimeException();
    }
  }
}
