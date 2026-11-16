Akka Cluster Shard Allocation Benchmark During Rolling Deploy

Goal: Measure the impact on shard resolution/allocation of various prospective changes to Akka Cluster Sharding and Akka Distributed Data.

Outline:
* CLI arg (required): number in range [0-14], taken as the number of the node for this JVM
* main:
  - initialize `appVersion` to zero
  - 100 times in a loop:
    * construct a Typesafe Config string which:
      - sets the artery host to `127.0.0.1` and the port to `30000 + nodeNumber + appVersion * 15` (so that port lingering doesn't disrupt the shutdown/restart dance)
      - sets the app version `akka.cluster.app-version` to `appVersion`
      - sets `akka.cluster.sharding.telemetry.instrumentations` to a list containing the FQCN of the telemetry implementation (see below)
      - explicitly configures the actor system to not exit the JVM when terminating
      - enables SBR with the default keep-majority strategy
    * start a typed actor system (see below for details on guardian behavior etc.)
    * Await.result on the `whenTerminated` future of the actor system
    * sleep for between 1500 and 3000 ms
    * increment `appVersion`
  - before exiting, retrieves and prints relevant percentiles in milliseconds (at least p99) from the telemetry HDRHistogram

* Telemetry implementation:
  - main state is an HDRHistogram which must retain measurements across actor system stop/starts
    * intent is to measure latency of shard resolution (which sometimes triggers allocation)
    * histogram is aggregated across versions running in this JVM. The app-version is intended solely to loosely coordinate restarts
    * histogram should be capable of recording intervals from 1us to 10s
  - ephemeral state is a concurrent hashmap of String to Long
  - `regionRequestedShardHome(typeName, shardId)`: if ephemeral state does not contain an entry for `shardId`, save `nanoTime` into the ephemeral state for `shardId`
  - `receivedShardHome(typeName, shardId)`: if ephemeral state contains an entry for `shardId`, subtract the value from `nanoTime` and record the difference in the histogram, remove entry for `shardId`

* Guardian behavior:
  - if node 0 version 0, perform a cluster self-join
  - otherwise join using `127.0.0.1` and immediately preceding ports (using `thisPort` as the configured remoting port, from max(`thisPort` - 29, 30000) to `thisPort` + 14)
  - when member is `Up` and the cluster has at least 10 `Up` members, start sharding for a single entity type (see below)
  - after sharding has started, start 1000 child actors (see below)
    * the sharding extension can either be passed to the child actors or they can load it themselves in their setup block... since these actors don't start until sharding has started, the result is equivalent
  - subscribe to cluster events
    * if there are 15 Up members of the cluster, the set of app versions for those members is of size at least 2, and this node is one of the two youngest (greatest up-number) among the nodes with the lowest app version in the cluster, schedule actor system termination for between 6000ms and 8000ms from now
      - example: 14 nodes of app-version 0, 1 node with app-version 1: highest 2 up numbers of app-version 0 schedule themselves
    * if node 14 and a cluster with 15 Up members has all with the same version, schedule actor system termination in 10 seconds
    * once a node with the last app-version (e.g. 99 if main loops 100 times) has left the cluster (node 14 from the above rule) and this node is one of the two youngest overall, regardless of cluster size, schedule termination for between 6000ms and 8000ms
    * if two minutes pass with no change in cluster membership, exit the JVM (effectively aborting the test)
    * shutdown need only be scheduled once (when shutdown is scheduled, can unsubscribe from cluster events)

* Entity behavior:
  - has a single command type with `entityId: String` and `replyTo: ActorRef[Done]` fields
  - when receiving a command, replies immediately with `Done`
  - entity's message extractor extracts entityId from the command and uses that as the shard (i.e. shard is exactly the entityId)

* Child actor:
  - parameterized with 1 to 1000 (used as a string)
  - at random intervals between 1000ms and 2500ms sends a command through sharding to the entityId it was parameterized with.  It doesn't necessarily wait for a reply (can set replyTo to the ignoreRef), nor does it check for timeout
    * note: the child must wait such a random interval before sending the first message
  - note that this is open loop: the child is allowed to have multiple in flight messages to its entity

* Build setup:
  - set the version for the akka dependencies as a variable for easy overriding.  This can be initially developed using `2.10.24`; I'll be using locally published artifacts for some of the runs, so this will make it easier to update
  - include the sbt-assembly plugin, configured to concatenate `reference.conf` into one file
