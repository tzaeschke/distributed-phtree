#argument="distributed"
filename="./".argument.".data"

set style line 12 lc rgb '#808080' lt 0 lw 1
set grid back ls 12

set style line 1 lc rgb '#8b1a0e' pt 5 ps 1 lt 1 lw 4 # --- red
set style line 2 lc rgb '#5e9c36' pt 7 ps 1 lt 1 lw 4 # --- green
set style line 3 lc rgb '#0000FF' pt 9 ps 1 lt 1 lw 4 # --- blue

set terminal pdfcairo enhanced font 'Verdana,16'
set output argument."-tp.pdf"

set title "Throughput vs number of servers"
set xlabel "Number of servers"
set ylabel "Operations per s"

plot filename using 1:2:xtic(1) w lp ls 1 title "Throughput"

set output argument."-rt.pdf"

set title "Response time vs number of servers"
set xlabel "Number of servers"
set ylabel "Response time (ms)"
plot filename using 1:3:xtic(1) w lp ls 1 title "Response time"
