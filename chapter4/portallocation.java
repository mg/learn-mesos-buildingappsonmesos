public static findNPorts(Offer offer, int n) {
  int numStillNeeded= n;
  Value.Ranges.Builder ranges= Value.Ranges.newBuilder();
  List<Value.Range> availablePorts= null;
  for(Resource r : offer.getResourcesList()) {
    if(r.getName().equals("port")) {
      availablePorts= r.getRanges().getRangeList();
    }
  }

  while(numStillNeeded > 0 && availablePorts != null && !availablePorts.isEmpty()) {
    Value.Range portRange= availablePorts.remove(0);
    long numAvail= portRange.getEnd() - portRange.getBegin() + 1; // continuous subranges are inclusive
    long numWillUse= numAvail >= numStillNeeded ? numStillNeeded : numAvail;
    ranges.addRange(
      Value.Range.NewBuilder()
        .setBegin(portRange.getBegin())
        .setEnd(portRange.getEnd() + numWillUse - 1) // inclusive range
        .build()
    );
    numStillNeeded -= numWillUse;
  }

  if(numStillNeeded > 0) {
    throw new RuntimeException("Couldn't satisfy " + n + " ports");
  }

  return Resource.newBuilder()
    .setName("ports")
    .setType(Value.Type.RANGES)
    .setRanges(ranges.build())
    .build();
}
