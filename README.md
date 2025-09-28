How is Raft working?


We have a log which is shared between everyone, it must be saved to the stable storage and it must be the same for every node.
The base idea about log and the most important one is: IF THE LOG IS SET AND DONE, THEN IT'S CORRECT UP UNTO THIS POINT.
This allows us to reason about it. If the value at some index has the same term and value, then prefix is the same. 

Since our log can grow infinitely we need a solution to making it compact. For this we will use snapshotting. We can use snapshotting
indepentently because of log - if we agreed to the log, then everyone has the same log. Hence whatever we do with the log, we can 
be assured, that it's correct. Because of that we can get the state of our state machine, save it to file and then restore
our machine from this. We do not need to talk to other servers, because we already agreed that our log is the same. In that case,
we can cut the log, save it to snapshot and then proceed with minimal log - empty or just some entries that came up while we were snapshotting




What was before snapshot? \
We were loooking at pure len(r.log). Now len(r.log) = len(r.log) + lastIndexSnapshot
