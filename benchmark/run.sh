#!/bin/zsh

THREADS=8
POINTS=50000

ZK_HOST="zk-us-1.us-central1-a.robotic-branch-89320"
ZK_HOST_IP=$(ssh $ZK_HOST 'curl ipecho.net/plain')

SERVERS=("server-us-1.us-central1-a.robotic-branch-89320" "server-us-3.us-central1-a.robotic-branch-89320")
CLIENTS=("client-us-1.us-central1-a.robotic-branch-89320" "client-us-2.us-central1-a.robotic-branch-89320" "client-us-3.us-central1-a.robotic-branch-89320")

NR_SERVERS=${#SERVERS[@]}
NR_CLIENTS=${#CLIENTS[@]}

echo "Threads: " $THREADS
echo "Points: " $POINTS
echo "Servers: " $NR_SERVERS
echo "Clients:" $NR_CLIENTS

ssh $ZK_HOST 'sh zkStart.sh';

for server in $SERVERS
do
    SERVER_IP=$(ssh $server 'curl ipecho.net/plain')
    ssh $server "java -jar middleware-1.0-SNAPSHOT.jar $SERVER_IP:5050 $ZK_HOST_IP:2181 > server.log &"
done

for client in $CLIENTS
do
    ssh $client "java -cp benchmarks.jar ch.ethz.globis.distindex.cluster.ClusterInsertionBenchmark $ZK_HOST_IP 2181 $POINTS $THREADS > client.log &"
done 

echo "" > client.log

sleep 1
for client in $CLIENTS
do    
    while [ $(ssh $client "ps -ef | grep java -c") != 2 ]
    do
        sleep 1    
    done
    ssh $client "cat client.log" > $client.log             
    cat $client.log >> client.log
    cat $client.log | grep "insert" | cut -d"," -f1 > $client.dat
    gnuplot -e "imagefile='./insert-tp-${client}.png'; raw_file='./insert-raw-${client}.csv'; inputfile='${client}.dat'" process_logs.gp       
    rm $client.dat
done

for server in $SERVERS
do
    for pid in $(ssh $server "ps -ef | grep java" | awk '{print $2}')
    do
        echo $pid
        ssh $server "kill -9 $pid";
    done
done

ssh $ZK_HOST 'sh zkStop.sh';

REPORT_FILE=experiment_$NR_SERVERS\_$NR_CLIENTS\_$THREADS\_$POINTS.txt

q -d "," "select count(c1) from client.log group by c1" | tail -n +15 | head -n -15 | histogram.py > $REPORT_FILE
