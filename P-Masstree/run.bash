#!/usr/bin/env bash

for var in "$@"; do
  if [ "$var" = "build" ]; then

    # build ralloc
    cd /home/xiaoxiang/ralloc/test/ || exit
    git pull
    make clean
    make libralloc.a
    if [ ! -f libralloc.a ]; then
      echo "Failed to build ralloc!"
      exit
    fi

    # build P-Masstree
    cd /home/xiaoxiang/RECIPE/P-Masstree/ || exit
    git pull
    rm -rf build && mkdir build
    cd build || exit
    cmake .. && make -j
    if [ ! -f example ]; then
      echo "Failed to build P-Masstree!"
      exit
    fi

    echo "" && echo "############" && echo "OK" && echo "############" && echo ""
    exit
  fi
done

cd /home/xiaoxiang/RECIPE/P-Masstree/build/ || exit

index_location=("dram" "pmem")
value_location=("dram" "pmem")
num_threads=(16)
workload=100000000
key_order="random"

echo "insert,workload=$workload,unit=ops/us,key_order=$key_order" >insert.csv
echo "lookup,workload=$workload,unit=ops/us,key_order=$key_order" >lookup.csv

# the header of csv file
printf "index,value," >>insert.csv
printf "index,value," >>lookup.csv
for n in "${num_threads[@]}"; do
  printf "T=$n," >>insert.csv
  printf "T=$n," >>lookup.csv
done
echo "" >>insert.csv
echo "" >>lookup.csv

for i in "${index_location[@]}"; do
  for v in "${value_location[@]}"; do

    # the first two columns
    printf "$i,$v," >>insert.csv
    printf "$i,$v," >>lookup.csv

    for n in "${num_threads[@]}"; do
      rm -rf /pmem0/*
      /home/blepers/linux/tools/perf/perf record ./example "$workload" "$n" index="$i" value="$v" key="$key_order"
      #      ./example 100 "$n" index="$i" value="$v"
    done

    # this should result in two csv files insert.csv and lookup.csv
    # just append a new line to it
    echo "" >>insert.csv
    echo "" >>lookup.csv

  done
done
