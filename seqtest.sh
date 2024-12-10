GOPATH=`pwd` GOBIN=`pwd`/bin go install lintest
lsof -ti:7087 | xargs kill -9
lsof -ti:7070 | xargs kill -9
lsof -ti:7071 | xargs kill -9
lsof -ti:7072 | xargs kill -9
echo "Starting master"
bin/master > master.log 2>&1 &
sleep 2
echo "Starting replica-1"
bin/server -t -port 7070 -rpcport 8070 -exec -dreply > replica-1.log 2>&1 &
sleep 2
echo "Starting replica-2"
bin/server -t -port 7071 -rpcport 8071 -exec -dreply > replica-2.log 2>&1 &
sleep 2
echo "Starting replica-3"
bin/server -t -port 7072 -rpcport 8072 -exec -dreply > replica-3.log 2>&1 &
sleep 5
echo "Starting client"
bin/seqtest
lsof -ti:7087 | xargs kill -9
lsof -ti:7070 | xargs kill -9
lsof -ti:7071 | xargs kill -9
lsof -ti:7072 | xargs kill -9
