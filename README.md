Minishard — lightweigth sharding for distributed Erlang applications
=======

Goal
-----
Sometimes you need to store large amount of temporary data available from any node of Erlang cluster.
Storing all the data on single node may cause a memory problem and makes this node a single point of failure.
Replication is even worse sometimes — in case of unreliable network (e.g. multiple datacenters) you get
inconsistencies and merge conflicts.

Minishard keeps configured number of unique shards allocated on nodes of your cluster, restores cluster connectivity,
resolves possible conflicts after netsplit, notifies your application when cluster degrades.

How minishard is supposed to work
-----------
Minishard node is started with two arguments: ```ClusterName :: atom()``` and ```CallbackMod :: module()```.

Before joining the cluster we first need to know the list of nodes it is constructed of. This is done by calling ```CallbackMod:nodes(ClusterName) -> [node()]```.

Also minishard gets the number of required shards by calling ```CallbackMod:shard_count(ClusterName) -> integer()```.

Then minishard tries to connect to all these nodes, synchronizes ```global``` with them and starts a ```minishard_{{ClusterName}}_watcher``` process which tracks the cluster health.
There are three possible health values:
  * ```degraded``` — number of available nodes is less than shard count, so it is impossible to allocate all shards
  * ```available``` — all shards are allocated, the cluster is fully available
  * ```transition``` — status is temporary unclear due to recent changes in node visibility

When cluster is not degraded, a shard manager ```minishard_{{ClusterName}}_manager``` should be running. There are two valid states for it:
  * ```allocated``` — one of required shards is allocated to this node
  * ```standby``` — all shards are allocated on other nodes, their managers are monitored by this one

After manager has allocated a shard, ```CallbackMod:allocated(ClusterName, ShardId) -> {ok, State}``` is called.
Any actions needed to initialize a shard should be performed in this function.
```State``` is any term you want, it will be passed to other callbacks.

When conflict occurs minishard decides which shard instance should be shut down. To do that each instance is queried for its score by calling ```CallbackMod:score(State) -> integer()```.
Instance with highest score is a winner and keeps its allocation. Loser is deallocated. Corresponding callbacks are caled:
  * ```CallbackMod:prolonged(LoserPid, State) -> {ok, NewState}```
  * ```CallbackMod:deallocated(WinnerPid, State) -> any()``` — final cleanup, return value is ignored. If deallocation is done due to cluster degradation or shutdown, ```WinnerPid``` is ```undefined```.

If you need a complex migration algorithm, implement it yourself by calling e.g. ```gen_server:enter_loop``` on deallocation.
