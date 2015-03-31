#!/bin/zsh

MACHINE="client-asia-1.asia-east1-c.robotic-branch-89320"
NR_ENTRIES=5000000
MAX_THREADS=16

DIR="concurrent"
mkdir $DIR
cd $DIR

echo "Starting concurrency tests on $MACHINE" 
echo "Nr entries: $NR_ENTRIES"
echo "Mx threads: $MAX_THREADS"

ssh $MACHINE "java -Xmx16G -cp benchmarks.jar ch.ethz.globis.distindex.benchmark.InsertionBenchmark $NR_ENTRIES $MAX_THREADS > client.log"

echo "Waiting for test to finish ..."

while [ $(ssh $MACHINE "ps -ef | grep java -c") != 2 ]
do
    sleep 1   
done

echo "Retrieving test data ..."
ssh $MACHINE "cat client.log" > concurrent.data

echo "Done..."
cd ..
