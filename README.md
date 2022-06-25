lab1 ---- 2022/6/6
```
☁  main [main] ⚡  ./test-mr.sh
*** Starting wc test.
2022/06/26 01:26:27 rpc.Register: method "CrashDetector" has 1 input parameters; needs exactly three
2022/06/26 01:26:27 rpc.Register: method "Done" has 1 input parameters; needs exactly three
All tasks are finished, the coordinator is going to exit.
Task execution completed, the worker is going to exit.
--- wc test: PASS
2022/06/26 01:26:41 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
2022/06/26 01:26:42 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
*** Starting indexer test.
2022/06/26 01:26:44 rpc.Register: method "CrashDetector" has 1 input parameters; needs exactly three
2022/06/26 01:26:44 rpc.Register: method "Done" has 1 input parameters; needs exactly three
All tasks are finished, the coordinator is going to exit.
2022/06/26 01:26:51 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
--- indexer test: PASS
2022/06/26 01:26:51 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
*** Starting map parallelism test.
2022/06/26 01:26:51 rpc.Register: method "CrashDetector" has 1 input parameters; needs exactly three
2022/06/26 01:26:51 rpc.Register: method "Done" has 1 input parameters; needs exactly three
All tasks are finished, the coordinator is going to exit.
2022/06/26 01:27:01 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
2022/06/26 01:27:01 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
--- map parallelism test: PASS
*** Starting reduce parallelism test.
2022/06/26 01:27:01 rpc.Register: method "CrashDetector" has 1 input parameters; needs exactly three
2022/06/26 01:27:01 rpc.Register: method "Done" has 1 input parameters; needs exactly three
All tasks are finished, the coordinator is going to exit.
2022/06/26 01:27:10 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
--- reduce parallelism test: PASS
2022/06/26 01:27:10 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
*** Starting job count test.
2022/06/26 01:27:10 rpc.Register: method "CrashDetector" has 1 input parameters; needs exactly three
2022/06/26 01:27:10 rpc.Register: method "Done" has 1 input parameters; needs exactly three
All tasks are finished, the coordinator is going to exit.
Task execution completed, the worker is going to exit.
2022/06/26 01:27:27 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
2022/06/26 01:27:28 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
2022/06/26 01:27:28 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
--- job count test: PASS
*** Starting early exit test.
2022/06/26 01:27:28 rpc.Register: method "CrashDetector" has 1 input parameters; needs exactly three
2022/06/26 01:27:28 rpc.Register: method "Done" has 1 input parameters; needs exactly three
All tasks are finished, the coordinator is going to exit.
Task execution completed, the worker is going to exit.
2022/06/26 01:27:36 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
2022/06/26 01:27:36 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
--- early exit test: PASS
*** Starting crash test.
2022/06/26 01:27:36 rpc.Register: method "CrashDetector" has 1 input parameters; needs exactly three
2022/06/26 01:27:36 rpc.Register: method "Done" has 1 input parameters; needs exactly three
The task[2] may has crashed, and has taken 10 seconds.
The task[3] may has crashed, and has taken 10 seconds.
The task[0] may has crashed, and has taken 10 seconds.
The task[2] may has crashed, and has taken 11 seconds.
The task[8] may has crashed, and has taken 11 seconds.
The task[10] may has crashed, and has taken 10 seconds.
The task[17] may has crashed, and has taken 11 seconds.
All tasks are finished, the coordinator is going to exit.
Task execution completed, the worker is going to exit.
Task execution completed, the worker is going to exit.
2022/06/26 01:28:16 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
--- crash test: PASS
*** PASSED ALL TESTS
```
lab2 ---- 2022/6/19
```
☁  raft [main] ⚡  go test -race
Test (2A): initial election ...
  ... Passed --   3.1  3   50   13664    0
Test (2A): election after network failure ...
  ... Passed --   4.5  3  118   22044    0
Test (2A): multiple elections ...
  ... Passed --   5.7  7  576  104611    0
Test (2B): basic agreement ...
  ... Passed --   0.7  3   16    4366    3
Test (2B): RPC byte count ...
  ... Passed --   1.9  3   48  113754   11
Test (2B): agreement after follower reconnects ...
  ... Passed --   5.8  3  115   29510    8
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.5  5  216   40254    3
Test (2B): concurrent Start()s ...
  ... Passed --   0.7  3   12    3246    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   6.3  3  153   36392    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  21.3  5 1948 1241814  103
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.2  3   36   10262   12
Test (2C): basic persistence ...
  ... Passed --   3.7  3   74   18887    6
Test (2C): more persistence ...
  ... Passed --  15.2  5  958  183238   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   1.5  3   33    8244    4
Test (2C): Figure 8 ...
  ... Passed --  27.6  5 1151  229963   43
Test (2C): unreliable agreement ...
  ... Passed --   2.4  5  242   84113  246
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  39.5  5 7501 15490661  105
Test (2C): churn ...
  ... Passed --  16.5  5 2515 2825429  855
Test (2C): unreliable churn ...
  ... Passed --  16.5  5 1686 1089356  356
Test (2D): snapshots basic ...
  ... Passed --   5.3  3  138   47118  201
Test (2D): install snapshots (disconnect) ...
  ... Passed --  66.1  3 1339  529242  312
Test (2D): install snapshots (disconnect+unreliable) ...
  ... Passed --  70.8  3 1526  634671  335
Test (2D): install snapshots (crash) ...
  ... Passed --  32.5  3  646  349306  335
Test (2D): install snapshots (unreliable+crash) ...
  ... Passed --  43.1  3  818  373066  321
Test (2D): crash and restart all servers ...
  ... Passed --  10.4  3  272   78232   62
PASS
ok      6.824/raft      406.666s
```
lab3 ---- 2022/6/26
```
☁  kvraft [main] ⚡  go test
Test: one client (3A) ...
  ... Passed --  15.1  5  5481  957
Test: ops complete fast enough (3A) ...
  ... Passed --  15.9  3  3844    0
Test: many clients (3A) ...
  ... Passed --  15.3  5 11195 4122
Test: unreliable net, many clients (3A) ...
  ... Passed --  16.2  5  6350 1376
Test: concurrent append to same key, unreliable (3A) ...
  ... Passed --   0.8  3   182   52
Test: progress in majority (3A) ...
  ... Passed --   0.3  5    33    2
Test: no progress in minority (3A) ...
  ... Passed --   1.0  5   137    3
Test: completion after heal (3A) ...
  ... Passed --   1.0  5    46    3
Test: partitions, one client (3A) ...
  ... Passed --  22.5  5  9756  973
Test: partitions, many clients (3A) ...
  ... Passed --  22.9  5 48853 3899
Test: restarts, one client (3A) ...
  ... Passed --  18.7  5  8420  951
Test: restarts, many clients (3A) ...
  ... Passed --  19.0  5 21195 4087
Test: unreliable net, restarts, many clients (3A) ...
  ... Passed --  19.8  5  6814 1376
Test: restarts, partitions, many clients (3A) ...
  ... Passed --  26.1  5 42686 4097
Test: unreliable net, restarts, partitions, many clients (3A) ...
  ... Passed --  27.0  5  6955  923
Test: unreliable net, restarts, partitions, random keys, many clients (3A) ...
  ... Passed --  30.4  7 17625 2794
Test: InstallSnapshot RPC (3B) ...
  ... Passed --   2.7  3  1017   63
Test: snapshot size is reasonable (3B) ...
  ... Passed --  12.7  3  3266  800
Test: ops complete fast enough (3B) ...
  ... Passed --  16.0  3  4076    0
Test: restarts, snapshots, one client (3B) ...
  ... Passed --  18.6  5  7413  947
Test: restarts, snapshots, many clients (3B) ...
  ... Passed --  19.5  5 37885 16542
Test: unreliable net, snapshots, many clients (3B) ...
  ... Passed --  16.2  5  6094 1318
Test: unreliable net, restarts, snapshots, many clients (3B) ...
  ... Passed --  20.2  5  6976 1394
Test: unreliable net, restarts, partitions, snapshots, many clients (3B) ...
  ... Passed --  26.5  5  7275 1092
Test: unreliable net, restarts, partitions, snapshots, random keys, many clients (3B) ...
  ... Passed --  29.2  7 18696 2159
PASS
ok      6.824/kvraft    414.032s
```