#!/bin/bash

run_asio() {
    ./asio_fibers --logtostderr --vmodule=rpc_connection=1,connection_handler=1 &
    SRV_PID=$!

    ./asio_fibers --connect=localhost --count 100000 --num_connections=1 &
    CL_PID=$!
    echo "Run client/server, iteration $1"

    sleep 0.5

    kill $SRV_PID
    wait $CL_PID
    wait $SRV_PID
}

COUNTER=0
NUM_ITERS=${1:-10}
echo $NUM_ITERS

while [  $COUNTER -lt $NUM_ITERS ]; do
 run_asio $COUNTER
 let COUNTER=COUNTER+1
done
