filename="./concurrent-comp.data"

set terminal pdfcairo
set output "concurrent-comp.pdf"

set style data histograms 
set style fill solid 1.00 border -1

set title "Insertion of 1e6 elements into the PH-tree"
set ylabel "Duration (s)"
set xlabel "Concurrency strategies"

set yrange [0:5]
set boxwidth 0.8 relative
unset xtics
plot filename using 1 title "No concurrency", \
    filename using 2 title "Copy-on-Write", \
    filename using 3 title "Hand-over-Hand locking", \
    filename using 4 title "Optimistic locking"
