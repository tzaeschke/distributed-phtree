
set datafile separator ","
set xdata time
set timefmt x "%Y-%m-%d %H:%M:%S"
set terminal pngcairo  
set output imagefile

plot inputfile using 1:(1) smooth frequency

set table raw_file
plot inputfile using 1:(1) smooth frequency
unset table
