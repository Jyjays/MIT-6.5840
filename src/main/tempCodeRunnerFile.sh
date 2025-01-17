 /home/jyjays/MIT6.5840/6.5840/src/main/mrcoordinator /home/jyjays/MIT6.5840/6.5840/src/main/input_files/daopai_*txt &
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
( /home/jyjays/MIT6.5840/6.5840/src/main/mrworker /home/jyjays/MIT6.5840/6.5840/src/mrapps/usercf.so) &
( /home/jyjays/MIT6.5840/6.5840/src/main/mrworker /home/jyjays/MIT6.5840/6.5840/src/mrapps/usercf.so) &
( /home/jyjays/MIT6.5840/6.5840/src/main/mrworker /home/jyjays/MIT6.5840/6.5840/src/mrapps/usercf.so) &