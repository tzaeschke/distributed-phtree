#argument="concurrent"
filename="./".argument.".data"

set style line 12 lc rgb '#808080' lt 0 lw 1
set grid back ls 12

set style line 1 lc rgb '#8b1a0e' pt 1 ps 1 lt 1 lw 2 # --- red
set style line 2 lc rgb '#5e9c36' pt 6 ps 1 lt 1 lw 2 # --- green
set style line 3 lc rgb '#0000FF' pt 9 ps 1 lt 1 lw 2 # --- blue

set terminal pdfcairo enhanced
set output argument."-tp.pdf"

set title "Throughput vs number of threads"
set xlabel "Number of threads"
set ylabel "1e6 operations per s"

set yrange [0:2]
set ytics 0.5

plot    filename using 1:($2/1e6):xtic(1) title "COW" w lp ls 1, \
        filename using 1:($4/1e6):xtic(1) title "Hand over Hand locking" w lp ls 2, \
        filename using 1:($6/1e6):xtic(1) title "Optimistic locking" w lp ls 3

reset
set output argument."-rt.pdf"

set title "Response time vs number of threads"
set xlabel "Number of threads"
set ylabel "Response time ({/Symbol m}s)"
set style line 1 lc rgb '#8b1a0e' pt 1 ps 1 lt 1 lw 2 # --- red
set style line 2 lc rgb '#5e9c36' pt 6 ps 1 lt 1 lw 2 # --- green
set style line 3 lc rgb '#0000FF' pt 9 ps 1 lt 1 lw 2 # --- blue
set autoscale
set yrange [0:50]

plot    filename using 1:($3 * 1000.0):xtic(1) title "COW" w lp ls 1, \
        filename using 1:($5 * 1000.0):xtic(1) title "Hand over Hand locking" w lp ls 2, \
        filename using 1:($7 * 1000.0):xtic(1) title "Optimistic locking" w lp ls 3

