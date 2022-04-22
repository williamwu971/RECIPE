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

  fi
done

cd /home/xiaoxiang/RECIPE/P-Masstree/build/ || exit
rm -rf /pmem0/*
/home/blepers/linux/tools/perf/perf record ./example "$1" "$2"
