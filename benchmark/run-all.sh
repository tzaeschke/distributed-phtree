#!/bin/zsh

# Run all of the tests with repetitions

NR_REPS=1;

COUNTER=0;
while [[ $COUNTER -lt $NR_REPS ]]; do
    for i in `seq 1 4`;
    do 
        zsh run_experiment.sh $i 7 "RangeBenchmark" "range"
    done

    let COUNTER+=1
done 

COUNTER=0;
while [[ $COUNTER -lt $NR_REPS ]]; do
    for i in `seq 1 4`;
    do 
        zsh run_experiment.sh $i 7 "InsertionBenchmark" "insert"
    done

    let COUNTER+=1
done

COUNTER=0;
while [[ $COUNTER -lt $NR_REPS ]]; do
    for i in `seq 1 4`;
    do 
        zsh run_experiment.sh $i 7 "KNNBenchmark" "knn"
    done

    let COUNTER+=1
done

COUNTER=0;
while [[ $COUNTER -lt $NR_REPS ]]; do
    for i in `seq 1 4`;
    do 
        zsh run_experiment.sh $i 7 "ReadBenchmark" "get"
    done

    let COUNTER+=1
done



