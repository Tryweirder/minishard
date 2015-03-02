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

Before joining the cluster we first need to know the list of nodes it is constructed of. This is done by calling ```CallbackMod:nodes(ClusterName) -> [node()]```.

Also minishard gets the number of required shards by calling ```CallbackMod:shard_count(ClusterName) -> integer()```.

Then minishard tries to connect to all these nodes, synchronizes ```global``` with them and starts a ```minishard_{{ClusterName}}_watcher``` process which tracks the cluster health.
There are three possible health values:
  * ```degraded``` — number of available nodes is less than shard count, so it is impossible to allocate all shards
  * ```available``` — there are enough nodes to allocate all shards
  * ```transition``` — status is temporary unclear due to recent changes in node visibility

When cluster is not degraded, a shard manager ```minishard_{{ClusterName}}_manager``` should be running. There are two valid states for it:
  * ```allocated``` — one of required shards is allocated to this node
  * ```standby``` — all shards are allocated on other nodes, their managers are monitored by this one

After manager has allocated a shard, ```CallbackMod:allocated(ClusterName, ShardId) -> {ok, State}``` is called.
Any actions needed to initialize a shard should be performed in this function.
```State``` is any term you want, it will be passed to other callbacks.

When conflict occurs minishard decides which shard instance should be shut down. To do that each instance is queried for its score by calling ```CallbackMod:score(State) -> integer()```.
Instance with the highest score is a winner and remains allocated. Loser is deallocated. Corresponding callbacks are called:
  * ```CallbackMod:prolonged(LoserPid, State) -> {ok, NewState}```
  * ```CallbackMod:deallocated(WinnerPid, State) -> any()``` — final cleanup, return value is ignored. If deallocation is done due to cluster degradation or shutdown, ```WinnerPid``` is ```undefined```.

If you need a complex migration algorithm, implement it yourself by calling e.g. ```gen_server:enter_loop``` on deallocation.

Watcher details
-----------
For cluster monitoring we need every node to be continiously checked. For each node a pinger is started.
A running pinger keeps node status in its process dictionary (for quick lock-less querying).
When started, pinger sets status to ```unavailable``` and pings the target node.
If ping is successful, the status changes to ```node_up```, and pinger sets a monitor for this node.
Now it's time to check if node is part of the same minishard cluster by checking if corresponging processes exist. After this check status becomes one of ```not_my_cluster``` or ```available```.
For available node a monitor on reverse pinger is set.

After that the pinger starts simple periodic check of remote process. Depending on a check result or incoming monitor messages the status may degrade to ```unavailable``` or ```node_up```.

Pinger notifies his watcher about every status change.

Watcher handles status notifications and polls pinger statuses to keep its internal node status table up-to-date.
Watcher starts with ```degraded``` status, switches to ```available``` when number of available nodes is not less than shard count.
When available node count is less than shard count, watcher switches to ```transition``` state.
After some time (e.g. 1 minute) in ```transition``` state (if it does not switch back to ```available```) the status is set to ```degraded```.
