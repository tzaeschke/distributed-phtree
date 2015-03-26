#!/bin/zsh
SERVERS=("server-us-1.us-central1-a.robotic-branch-89320" "server-us-2.us-central1-a.robotic-branch-89320" "server-us-3.us-central1-a.robotic-branch-89320" "server-us-4.us-central1-a.robotic-branch-89320")
CLIENTS=("client-us-1.us-central1-a.robotic-branch-89320" "client-us-2.us-central1-a.robotic-branch-89320" "client-us-3.us-central1-a.robotic-branch-89320" "client-us-4.us-central1-a.robotic-branch-89320" "client-us-5.us-central1-a.robotic-branch-89320" "client-us-6.us-central1-a.robotic-branch-89320")

for server in $SERVERS
do
    scp "../middleware/target/middleware-1.0-SNAPSHOT.jar" $server:
done

for client in $CLIENTS
do
    scp "target/benchmarks.jar" $client:
done
