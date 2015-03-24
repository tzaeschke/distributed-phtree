#!/bin/bash

ZK_HOST="zookeerper-1.europe-west1-b.robotic-branch-89320"
ZK_HOST_IP=$(ssh $ZK_HOST 'curl ipecho.net/plain')

SERVERS=("server-2.europe-west1-b.robotic-branch-89320")
CLIENTS=("client-1.europe-west1-b.robotic-branch-89320")

ssh $ZK_HOST 'sh zkStart.sh';

for server in $SERVERS
do
    scp "../middleware/target/middleware-1.0-SNAPSHOT.jar" $server:
    SERVER_IP=$(ssh $server 'curl ipecho.net/plain')
    ssh $server "java -jar middleware-1.0-SNAPSHOT.jar $SERVER_IP:5050 $ZK_HOST_IP:2181 > server.log &"
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
