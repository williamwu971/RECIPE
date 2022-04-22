#!/usr/bin/env bash

for var in "$@"; do
  if [ "$var" = "build" ]; then

    # build ralloc
    cd /home/xiaoxiang/ralloc/test/
    git pull
    make clean
    make libralloc.a

    # build P-Masstree
    cd /home/xiaoxiang/RECIPE/P-Masstree/
    git pull
    rm -rf build && mkdir build
    cd build
    cmake .. && make -j

    echo "" && echo "############" && echo "rebuilt" && echo "############" && echo ""
  fi
done

cd /home/xiaoxiang/RECIPE/P-Masstree/
/home/blepers/linux/tools/perf/perf record ./build/example "$1" "$2"
