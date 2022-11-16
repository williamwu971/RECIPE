#!/usr/bin/env bash

for var in "$@"; do
  if [ "$var" = "build" ]; then

    #    apt-get update
    #    apt-get install -y build-essential cmake libboost-all-dev libpapi-dev default-jdk
    #    apt-get install -y libtbb-dev libjemalloc-dev libpmem-dev

    # build P-Masstree
    cd /mnt/sdb/xiaoxiang/RECIPE/P-Masstree/ || exit
    git pull || exit
    #    rm -rf build && mkdir build
    rm -rf build/example
    cd build || exit
    cmake .. && make -j
    if [ ! -f example ]; then
      echo "Failed to build P-Masstree!"
      exit
    fi

    echo "" && echo "############" && echo "masstree build OK" && echo "############" && echo ""
    exit
  fi

  if [ "$var" = "ralloc" ]; then
    # build ralloc
    cd /home/xiaoxiang/ralloc/test/ || exit
    git pull
    make clean
    make -j libralloc.a
    if [ ! -f libralloc.a ]; then
      echo "Failed to build ralloc!"
      exit
    fi
    echo "" && echo "############" && echo "ralloc build OK" && echo "############" && echo ""
    exit
  fi

  if [ "$var" = "pmdk" ]; then
    # build ralloc
    cd /mnt/sdb/xiaoxiang/pmdk/ || exit
    git pull
    make -j || exit
    make -j install || exit
    echo "" && echo "############" && echo "pmdk build OK" && echo "############" && echo ""
    exit
  fi

  if [ "$var" = "graph" ]; then
    cd build || exit
    for filename in *.rdtsc; do
      if [ ! -f graph-"$filename".png ]; then
        python3 ../simple_graph.py --r "$filename" --fn graph-"$filename" --y "ops/ms" --x "time(ms)" --xlim 300 --ylim 1900 &
      fi
    done

    while pgrep -i -f simple_graph >/dev/null; do
      sleep 1
    done

    exit
  fi

done

cd build || exit
#rm -f ./*.rdtsc ./*.png ./*.csv ./max_latencies.txt

use_perf="yes"
record_latency="yes"


workload=430000000 # todo: do not change this
#workload=43000000

key_order="random"
#key_order="seq"

extra_sizes=(0)
#extra_sizes=(256)
#extra_sizes=($(seq 0 8 240)) # the size of the value impact performance a lot
#extra_sizes=($(seq 0 32 240))

#total_sizes=(0)
#total_sizes=(64)
total_sizes=(256)
#total_sizes=($(seq 40 8 256))
#total_sizes=($(seq 40 24 256))
#total_sizes=(1024)
#total_sizes=($(seq 40 24 1024))
#total_sizes=($(seq 2048 -24 40))

#index_location=("dram" "ralloc" "obj")
#index_location=("dram")
#index_location=("ralloc")
index_location=("obj")
#index_location=("dram" "ralloc")

#value_location=("ralloc" "log" "obj")
#value_location=("obj" "log" "ralloc")
#value_location=("ralloc")
#value_location=("log")
value_location=("obj")
#value_location=("ralloc" "log" "log-best")

#num_threads=(1 3 5 7 9 11 13 15)
#num_threads=(24)
num_threads=(19) # todo: do not change this
#num_threads=(1)

#num_of_gc=(8 0)
num_of_gc=(8)

#pmdk_no_flush=("0" "1")
pmdk_no_flush=("0")
#pmdk_no_flush=("1")

#ycsbs=("N")
ycsbs=("au" "bu" "cu" "eu" "az" "bz" "cz" "ez")

#persist=("flush" "non-temporal")
persist=("flush")
#persist=("non-temporal")

file_prefixes=("perf")

for fp in "${file_prefixes[@]}"; do

  {
    echo ""
    "date"
  } >>"$fp".csv

  echo "$fp,workload=$workload,key_order=$key_order" >>"$fp".csv

  # the header of csv file

  if [ "${#ycsbs[@]}" -eq "1" ]; then
    {
      printf "index,value,threads,gc,pmdk_no_flush,extra_sizes,total_sizes,ycsb,persist,"

      printf "insert_log(gb),insert_gc_log(gb),"
      printf "insert_Pr(gb),insert_Prb(gb/s),insert_Pw(gb),insert_Pwb(gb/s),"
      printf "insert_Dr(gb),insert_Drb(gb/s),insert_Dw(gb),insert_Dwb(gb/s),"
      printf "insert_TP(ops/us),insert_gc_TP(ops/us),"

      printf "update_log(gb),update_gc_log(gb),"
      printf "update_Pr(gb),update_Prb(gb/s),update_Pw(gb),update_Pwb(gb/s),"
      printf "update_Dr(gb),update_Drb(gb/s),update_Dw(gb),update_Dwb(gb/s),"
      printf "update_TP(ops/us),update_gc_TP(ops/us),"

      printf "lookup_log(gb),lookup_gc_log(gb),"
      printf "lookup_Pr(gb),lookup_Prb(gb/s),lookup_Pw(gb),lookup_Pwb(gb/s),"
      printf "lookup_Dr(gb),lookup_Drb(gb/s),lookup_Dw(gb),lookup_Dwb(gb/s),"
      printf "lookup_TP(ops/us),lookup_gc_TP(ops/us),"

      printf "delete_log(gb),delete_gc_log(gb),"
      printf "delete_Pr(gb),delete_Prb(gb/s),delete_Pw(gb),delete_Pwb(gb/s),"
      printf "delete_Dr(gb),delete_Drb(gb/s),delete_Dw(gb),delete_Dwb(gb/s),"
      printf "delete_TP(ops/us),delete_gc_TP(ops/us),"

    } >>"$fp".csv
  else
    {
      printf "index,value,threads,gc,pmdk_no_flush,extra_sizes,total_sizes,ycsb,persist,"

      printf "load_log(gb),load_gc_log(gb),"
      printf "load_Pr(gb),load_Prb(gb/s),load_Pw(gb),load_Pwb(gb/s),"
      printf "load_Dr(gb),load_Drb(gb/s),load_Dw(gb),load_Dwb(gb/s),"
      printf "load_TP(ops/us),load_gc_TP(ops/us),"

      printf "run_log(gb),run_gc_log(gb),"
      printf "run_Pr(gb),run_Prb(gb/s),run_Pw(gb),run_Pwb(gb/s),"
      printf "run_Dr(gb),run_Drb(gb/s),run_Dw(gb),run_Dwb(gb/s),"
      printf "run_TP(ops/us),run_gc_TP(ops/us),"
    } >>"$fp".csv
  fi

  echo "" >>"$fp".csv
done

echo 0 >/proc/sys/kernel/nmi_watchdog

for s in "${extra_sizes[@]}"; do
  for t in "${total_sizes[@]}"; do
    for i in "${index_location[@]}"; do
      for v in "${value_location[@]}"; do
        for n in "${num_threads[@]}"; do
          for g in "${num_of_gc[@]}"; do
            for f in "${pmdk_no_flush[@]}"; do
              for y in "${ycsbs[@]}"; do
                for p in "${persist[@]}"; do

                  # backup perf files
                  #        cd .. || exit
                  #          for pfn in *.perf; do
                  #            [ -f "$pfn" ] || break
                  #            echo "backing up $pfn"
                  #            mv "$pfn" "$pfn".old
                  #          done
                  #        cd - || exit

                  # the first three columns
                  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,' "$i" "$v" "$n" "$g" "$f" "$s" "$t" "$y" "$p" >>perf.csv

                  # drop system cache and clear pmem device
                  echo 1 >/proc/sys/vm/drop_caches
                  rm -rf /pmem0/masstree*
                  killall -w perf >/dev/null 2>&1
                  pkill -f pcm-memory >/dev/null 2>&1
                  #      /home/blepers/linux/tools/perf/perf record -g ./example "$workload" "$n" index="$i" value="$v" key="$key_order"
                  PMEM_NO_FLUSH="$f" ./example "$workload" "$n" extra_size="$s" total_size="$t" \
                    index="$i" value="$v" key="$key_order" perf="$use_perf" \
                    gc="$g" ycsb="$y" latency="$record_latency" persist="$p" \
                    prefix="$i"-"$v"-"$n"-"$g"-NF"$f"-"$s"b-"$t"B-"$y"-"$p" || exit

                  #      mv out.png out_"$i"_"$v".png
                  #      ./example 100 "$n" index="$i" value="$v"

                  # this should result in two csv files insert.csv and lookup.csv
                  # just append a new line to it
                  for fp in "${file_prefixes[@]}"; do
                    echo "" >>"$fp".csv
                  done

                done
              done
            done
          done
        done
      done
    done
  done
done

if [ "$record_latency" = "yes" ]; then
  for filename in *.rdtsc; do
    #              python3 ../simple_graph.py --r "$filename" --fn graph-"$i"-"$v"-"$n"-"$g"-NF"$f"-"$filename" --ylim 100000000 --xlim "$workload" || exit
    if [ ! -f graph-"$filename".png ]; then
      python3 ../simple_graph.py --r "$filename" --fn graph-"$filename" --y "ops/ms" --x "time(ms)" --xlim 300 --ylim 1900 &
    fi
    #              python3 ../simple_graph.py --r "$filename" --fn graph-"$i"-"$v"-"$n"-"$g"-NF"$f"-"$filename"|| exit
  done

  while pgrep -i -f simple_graph >/dev/null; do
    sleep 1
  done
fi

echo 1 >/proc/sys/kernel/nmi_watchdog

# move perf files
#for pfn in *.perf; do
#  [ -f "$pfn" ] || break
#  mv "$pfn" ../
#done
