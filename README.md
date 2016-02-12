Minishard — lightweight sharding for distributed Erlang applications
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

Before joining the cluster we first need to know the list of nodes it is constructed of. This is done by calling ```CallbackMod:cluster_nodes(ClusterName) -> [node()]```.

Also minishard gets the number of required shards by calling ```CallbackMod:shard_count(ClusterName) -> integer()```.

Minishard uses modified version of ```gen_leader``` for leader election.
Leader is responsible for all shard allocations/deallocations.

After manager has been selected as a shard owner, ```CallbackMod:allocated(ClusterName, ShardId) -> {ok, State}``` is called.
Any actions needed to initialize a shard should be performed in this function.
```State``` is any term you want, it will be passed to other callbacks.

When conflict occurs minishard decides which shard instance should be shut down. To do that each instance is queried for its score by calling ```CallbackMod:score(State) -> integer()```.
Instance with the highest score is a winner and remains allocated. Loser is deallocated. Corresponding callbacks are called:
  * ```CallbackMod:prolonged(LoserPid, State) -> {ok, NewState}```
  * ```CallbackMod:deallocated(WinnerPid, State) -> any()``` — final cleanup, return value is ignored. If deallocation is done due to cluster degradation or shutdown, ```WinnerPid``` is ```undefined```.

If you need a complex migration algorithm, implement it yourself by calling e.g. ```gen_server:enter_loop``` on deallocation.

