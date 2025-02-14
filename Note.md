# to get this to recreate the figures in the paper, i modified the following:
- commented out the sleep in the execute function for both src/mdlin/mdlin.go and src/paxos/paxos.go
- made the Sleep value in src/clientnew/main.go 1e6 (instead of 100 * 1e6). the randSleep is always 1, since it's not a flag passed into any configs
- made the configs put 9 shard leaders on 9 physical machines
- the length of the experiment doesn't really matter, i used 16 wth 0 rampup and rampdown
- clients are [1, 2, 4, 8, 16, 32, 6, 128, 256, 512] 
