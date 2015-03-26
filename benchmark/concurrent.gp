set style line 12 lc rgb '#808080' lt 0 lw 1
set grid back ls 12

set style line 1 lc rgb '#8b1a0e' pt 1 ps 1 lt 1 lw 2 # --- red
set style line 2 lc rgb '#5e9c36' pt 6 ps 1 lt 1 lw 2 # --- green
set style line 3 lc rgb '#0000FF' pt 9 ps 1 lt 1 lw 2 # --- blue

set terminal pngcairo
set output "concurrency-tp.png"

set title "Evolution of throughput in relation with number of threads"
set xlabel "Number of threads"
set ylabel "Operations per second"

set yrange [0:1500000]
set ytics 250000
set logscale x

plot    "./concurrent.data" using 1:2:xtic(1) title "COW" w lp ls 1, \
        "./concurrent.data" using 1:4:xtic(1) title "Hand over Hand locking" w lp ls 2, \
         "./concurrent.data" using 1:6:xtic(1) title "Optimistic locking" w lp ls 3
