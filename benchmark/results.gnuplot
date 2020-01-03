
set terminal pngcairo size 600,300 enhanced font 'Verdana,8';
set output '/home/vy/Projects/reactor-pubsub/benchmark/results.png';
set datafile separator ',';
set grid;
set xtics;
set ytics;
set xlabel 'concurrency';
set ylabel 'time (sec)';
set origin 0,0;
set xrange [0:7];
set yrange [1.083:9.813];
set multiplot layout 1,4;

set title '1 KiB';
plot 'results.csv' using ($2==1024?$3:1/0):($4/1000.0):($1/4000.0) with points linestyle 6 notitle

set xlabel ' ';
unset ylabel;
set title '4 KiB';
plot 'results.csv' using ($2==4096?$3:1/0):($4/1000.0):($1/4000.0) with points linestyle 6 notitle

set xlabel ' ';
unset ylabel;
set title '8 KiB';
plot 'results.csv' using ($2==8192?$3:1/0):($4/1000.0):($1/4000.0) with points linestyle 6 notitle

set xlabel ' ';
unset ylabel;
set title '16 KiB';
plot 'results.csv' using ($2==16384?$3:1/0):($4/1000.0):($1/4000.0) with points linestyle 6 notitle
