#!/bin/bash

cat client.log | grep "insert" | cut -d"," -f1 > insert.dat
# Run the Gnuplot script to create a graph of client throughput and populate the throughput file
gnuplot -e "imagefile='./insert-tp.png'; raw_file='./insert-raw.csv'; inputfile='insert.dat'" process_logs.gp
# Process the raw gnuplow throughputs and transform them to something useful
cat insert_raw.csv | grep \"g\" | cut -d" " -f3 > insert.csv

nohup gnuplot -e "stats 'insert.csv'"

