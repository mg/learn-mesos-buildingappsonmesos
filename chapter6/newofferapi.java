// old api
driver.launchTasks(
  Collections.singletonList(offer.getId()),
  Collections.singletonList(ti)
);

// new api
Offer.Operation.Launch launch= Offer.Operation.Launch
  .newBuilder()
  .addTaskInfos(ti)
  .build();

Offer.Operation.Operation launchOp= Offer.Operation
  .newBuilder()
  .setType(Offer.Operation.Type.LAUNCH)
  .setLaunch(launch)
  .build();

Offer.Operation.Reserve reserve= Offer.Operation.Reserve
  .newBuilder()
  .addAllResources(ti.getREsourcesList())
  .build();

Offer.Operation.Reserve reserveOp= Offer.Operation
  .newBuilder()
  .setType(Offer.Operation.Type.RESERVE)
  .setReserve(reserve)
  .build();

driver.acceptOffers(
  Collections.singletonList(offer.getId()),
  Array.asList([reserveOp, launchOp]),
  Fillters.newBuilder().build()
);

// querying resources for the reservation status
if(resource.hasReservation()) {
  resource.getReservation().getPrincipal();
}

// unreserving resources
List<Resource> resourcees= new ArrayList<>();
for(Resource r : offer.getREsourcesList()) {
  if(r.hasReservation()) {
    resources.add(r);
  }
}

Offer.Operation.Unreserve unreserve= Offer.Operation.Unreserve
  .newBuilder()
  .addAllResources(resources)
  .build();

Offer.Operation unreserveOp= Offer.Operation
  .newBuilder()
  .setType(Offer.Operation.Type.UNRESERVE)
  .setUnreserve(unreserve)
  .build();

driver.acceptOffers(
  Collections.singletonList(offer.getId()),
  Collections.singletonList(unreserveOp),
  Fillters.newBuilder().build()
);
