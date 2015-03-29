#!/bin/zsh

THREADS=8
POINTS=50000

ZK_HOST="zk-us-1.us-central1-a.robotic-branch-89320"
ZK_HOST_IP=$(ssh $ZK_HOST 'curl ipecho.net/plain')

SERVERS=("server-us-1.us-central1-a.robotic-branch-89320" "server-us-2.us-central1-a.robotic-branch-89320" "server-us-3.us-central1-a.robotic-branch-89320" "server-us-4.us-central1-a.robotic-branch-89320")
CLIENTS=("client-us-1.us-central1-a.robotic-branch-89320" "client-us-2.us-central1-a.robotic-branch-89320" "client-us-3.us-central1-a.robotic-branch-89320" "client-us-4.us-central1-a.robotic-branch-89320" "client-us-5.us-central1-a.robotic-branch-89320" "client-us-6.us-central1-a.robotic-branch-89320" "client-us-7.us-central1-a.robotic-branch-89320")

if [ "$#" -ne 4 ]; then 
    echo "Usage: <nr_servers> <nr_clients> <experiment_type> <operation_grep>"
    exit 1
fi

NR_SERVERS=$1
NR_CLIENTS=$2

SERVERS=("${SERVERS[@]:0:$NR_SERVERS}")
CLIENTS=("${CLIENTS[@]:0:$NR_CLIENTS}")
EXPERIMENT=$3
OPERATION=$4

# store the experiment data here
mkdir $EXPERIMENT
cd $EXPERIMENT 

echo "Threads: " $THREADS
echo "Points: " $POINTS
echo "Servers: " $NR_SERVERS $SERVERS
echo "Clients:" $NR_CLIENTS $CLIENTS

for client in $CLIENTS
do
    ssh $client "killall java"
done

for server in $SERVERS
do
    ssh $server "killall java"
done 

echo "Starting ZooKeeper"
ssh $ZK_HOST "sh zkStop.sh"
ssh $ZK_HOST 'sh zkStart.sh'

echo "Starting servers"
for server in $SERVERS
do
    SERVER_IP=$(ssh $server 'curl -s ipecho.net/plain')
    ssh $server "java -XX:+UseConcMarkSweepGC -Xmx4G -jar middleware-1.0-SNAPSHOT.jar $SERVER_IP:5050 $ZK_HOST_IP:2181 > server.log &"
done

echo "Starting clients"
for client in $CLIENTS
do
    ssh $client "java -XX:+UseConcMarkSweepGC -Xmx4G -cp benchmarks.jar ch.ethz.globis.distindex.cluster.Cluster$EXPERIMENT $ZK_HOST_IP 2181 $POINTS $THREADS > client.log &"
done 

echo "" > client.log

echo "Waiting for the clients to finish executing"
sleep 1
for client in $CLIENTS
do    
    while [ $(ssh $client "ps -ef | grep java -c") != 2 ]
    do
        sleep 1    
    done
    ssh $client "cat client.log" > $client.log             
    cat $client.log >> client.log
    cat $client.log | grep $OPERATION | cut -d"," -f1 > $client.dat
    gnuplot -e "imagefile='./${OPERATION}-tp-${client}.png'; raw_file='./${OPERATION}-raw-${client}.csv'; inputfile='${client}.dat'" ../process_logs.gp       
    rm $client.log
    rm $client.dat
    rm ${OPERATION}-raw-$client.csv
done

echo "Stopping servers..."
for server in $SERVERS
do
    for pid in $(ssh $server "ps -ef | grep java" | awk '{print $2}')
    do
        ssh $server "kill -9 $pid";
    done
done

echo "Stopping ZooKeeper"
ssh $ZK_HOST 'sh zkStop.sh';

REPORT_FILE=experiment_$NR_SERVERS\_$NR_CLIENTS\_$THREADS\_$POINTS.txt
echo "Computing stats and storing them into "$REPORT_FILE

echo "# Throughput stats #" >> $REPORT_FILE  
q -d "," "select count(c1) from client.log group by c1" | tail -n +15 | head -n -15 | histogram.py >> $REPORT_FILE
echo "# Response time stats #" >> $REPORT_FILE
q -d "," "select avg(c4) from client.log group by c1" | tail -n +15 | head -n -15 | histogram.py >> $REPORT_FILE

echo "Done!"
cd ..
