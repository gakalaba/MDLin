lsof -ti:7087 | xargs kill -9
lsof -ti:7070 | xargs kill -9
lsof -ti:7071 | xargs kill -9
lsof -ti:7072 | xargs kill -9
lsof -ti:7073 | xargs kill -9
lsof -ti:7074 | xargs kill -9
echo "Starting master"
bin/master -N 3 > master.log 2>&1 &
sleep 2
echo "Starting replica-1"
GODEBUG=gctrace=1 bin/server $1 -proxy -debug -port 7070 -rpcport 8070 -exec -dreply -thrifty -statsFile server-0.stats > replica-0.log 2>&1 &
sleep 2
echo "Starting replica-2"
bin/server $1 -proxy -port 7071 -rpcport 8071 -exec -dreply -thrifty -statsFile server-1.stats > replica-1.log 2>&1 &
sleep 2
echo "Starting replica-3"
bin/server $1 -proxy -port 7072 -rpcport 8072 -exec -dreply -thrifty -statsFile server-2.stats > replica-2.log 2>&1 &
sleep 3
#echo "Starting replica-4"
#bin/server $1 -debug -t -port 7073 -rpcport 8073 -exec -dreply -thrifty -statsFile server-3.stats > replica-3.log 2>&1 &
#sleep 2
#echo "Starting replica-5"
#bin/server $1 -debug -t -port 7074 -rpcport 8074 -exec -dreply -thrifty -statsFile server-4.stats > replica-4.log 2>&1 &
#sleep 3
echo "Starting client"
#for i in `seq 1024`; do
#bin/clientnew -replProtocol multi_paxos -clientId $i > /dev/null 2>&1 &
#done
#bin/clientnew -replProtocol multi_paxos -clientId 1024
#for i in `seq 50`; do
#bin/clientnew -rmws 100 -writes 0 -reads 0 -conflicts 100 -txpLength 60 -debug -replProtocol tupaq -clientId $i -debug > client-$i.log 2>&1 &
#done
#bin/clientnew -rmws 333 -writes 333 -reads 334 -conflicts 33 -expLength 60 -debug -replProtocol tupaq -clientId 51 -debug -forceLeader 0 -tailAtScale 2 > /dev/null 2>&1 &



#for i in `seq 7`; do
#bin/clientnew $2 -debug -proxy -rmws 0 -writes 1000 -reads 0 -conflicts 100 -expLength 30 -replProtocol tupaq -clientId $i -forceLeader 0 -cpuProfile cpu.prof -statsFile client-${i}.stats 2> client-${i}-stderr.log &
#done
bin/clientnew $2 -proxy -rmws 0 -writes 1000 -reads 0 -conflicts 100 -expLength 30 -replProtocol multi_paxos -clientId 52 -forceLeader 0 -statsFile client.stats 2> client-stderr.log

lsof -ti:7087 | xargs kill -15
lsof -ti:7070 | xargs kill -15
lsof -ti:7071 | xargs kill -15
lsof -ti:7072 | xargs kill -15
lsof -ti:7073 | xargs kill -15
lsof -ti:7074 | xargs kill -15
