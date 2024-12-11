pwd

# make sure software is freshly built.
(cd /home/jyjays/MIT6.5840/6.5840/src/mrapps && go clean)
(cd /home/jyjays/MIT6.5840/6.5840/src/main && go clean)
(cd /home/jyjays/MIT6.5840/6.5840/src/mrapps && go build $RACE -buildmode=plugin usercount.go) || exit 1
(cd /home/jyjays/MIT6.5840/6.5840/src/mrapps && go build $RACE -buildmode=plugin inverted.go) || exit 1
(cd /home/jyjays/MIT6.5840/6.5840/src/mrapps && go build $RACE -buildmode=plugin usercf.go) || exit 1
(cd /home/jyjays/MIT6.5840/6.5840/src/mrapps && go build $RACE -buildmode=plugin movierecommend.go) || exit 1
(cd /home/jyjays/MIT6.5840/6.5840/src/mrapps && go build $RACE -buildmode=plugin eval1.go) || exit 1
(cd /home/jyjays/MIT6.5840/6.5840/src/main && go build $RACE mrcoordinator.go) || exit 1
(cd /home/jyjays/MIT6.5840/6.5840/src/main && go build $RACE mrworker.go) || exit 1

failed_any=0

# start usercount job
echo '***' usercount

/home/jyjays/MIT6.5840/6.5840/src/main/mrcoordinator /home/jyjays/MIT6.5840/6.5840/src/main/input_files/trainSet_*txt &
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
(/home/jyjays/MIT6.5840/6.5840/src/main/mrworker /home/jyjays/MIT6.5840/6.5840/src/mrapps/usercount.so) &
( /home/jyjays/MIT6.5840/6.5840/src/main/mrworker /home/jyjays/MIT6.5840/6.5840/src/mrapps/usercount.so) &
( /home/jyjays/MIT6.5840/6.5840/src/main/mrworker /home/jyjays/MIT6.5840/6.5840/src/mrapps/usercount.so) &

# wait for the coordinator to exit.
wait $pid

python3 merge.py -n user_rating_count.txt

# start inverted job
echo '***' inverted

 /home/jyjays/MIT6.5840/6.5840/src/main/mrcoordinator /home/jyjays/MIT6.5840/6.5840/src/main/input_files/trainSet_*txt &
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
( /home/jyjays/MIT6.5840/6.5840/src/main/mrworker /home/jyjays/MIT6.5840/6.5840/src/mrapps/inverted.so) &
( /home/jyjays/MIT6.5840/6.5840/src/main/mrworker /home/jyjays/MIT6.5840/6.5840/src/mrapps/inverted.so) &
( /home/jyjays/MIT6.5840/6.5840/src/main/mrworker /home/jyjays/MIT6.5840/6.5840/src/mrapps/inverted.so) &

# wait for the coordinator to exit.
wait $pid

python3 merge.py -n daopai.txt -s daopai

# start usercf job
echo '***' usercf

 /home/jyjays/MIT6.5840/6.5840/src/main/mrcoordinator /home/jyjays/MIT6.5840/6.5840/src/main/input_files/daopai_*txt &
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
( /home/jyjays/MIT6.5840/6.5840/src/main/mrworker /home/jyjays/MIT6.5840/6.5840/src/mrapps/usercf.so) &
( /home/jyjays/MIT6.5840/6.5840/src/main/mrworker /home/jyjays/MIT6.5840/6.5840/src/mrapps/usercf.so) &
( /home/jyjays/MIT6.5840/6.5840/src/main/mrworker /home/jyjays/MIT6.5840/6.5840/src/mrapps/usercf.so) &

# wait for the coordinator to exit.
wait $pid

python3 merge.py -n user_matrix.txt -s sim

# start movierecommend job

echo '***' movierecommend

 /home/jyjays/MIT6.5840/6.5840/src/main/mrcoordinator /home/jyjays/MIT6.5840/6.5840/src/main/input_files/sim_*txt &
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
( /home/jyjays/MIT6.5840/6.5840/src/main/mrworker /home/jyjays/MIT6.5840/6.5840/src/mrapps/movierecommend.so) &
( /home/jyjays/MIT6.5840/6.5840/src/main/mrworker /home/jyjays/MIT6.5840/6.5840/src/mrapps/movierecommend.so) &
( /home/jyjays/MIT6.5840/6.5840/src/main/mrworker /home/jyjays/MIT6.5840/6.5840/src/mrapps/movierecommend.so) &

# wait for the coordinator to exit.
wait $pid

python3 merge.py -n movie_recommend.txt



# cleanup
rm -rf mr-out-*
rm -rf mr-inter-*

python3 eval.py