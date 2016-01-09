List<TaskStatus> runningTasks= new ArrayList<>();
for(Job j : jobs) {
  if(j.getStatus() == JobState.RUNNING) {
    TaskID tid= TaskID
      .newBuilder()
      .setValue(j.getId())
      .build();

    SlaveID sid= SlaveID
      .newBuilder()
      .setValue(j.getSlaveId())
      .build();

    System.out.println("Reconciling the task " + j.getId());
    runningTasks.add(TaskStatus
      .newBuilder()
      .setTaskId(tid)
      .setSlaveId(sid)
      .setState(TaskState.TASK_RUNNING)
      .build()
    );
  }
}

driver.reconcileTasks(runningTasks);
