cp latex.txt template.txt

python3 parse.py \
  dram-ralloc-19-8-NF0-0b-64B-N-flush-10000000n-update.rdtimes \
  dram-ralloc-19-8-NF0-0b-128B-N-flush-10000000n-update.rdtimes \
  dram-ralloc-19-8-NF0-0b-256B-N-flush-10000000n-update.rdtimes \
  dram-ralloc-19-8-NF0-0b-512B-N-flush-10000000n-update.rdtimes \
  dram-ralloc-19-8-NF0-0b-1024B-N-flush-10000000n-update.rdtimes >nf0.csv

python3 parse.py \
  dram-ralloc-19-8-NF1-0b-64B-N-flush-10000000n-update.rdtimes \
  dram-ralloc-19-8-NF1-0b-128B-N-flush-10000000n-update.rdtimes \
  dram-ralloc-19-8-NF1-0b-256B-N-flush-10000000n-update.rdtimes \
  dram-ralloc-19-8-NF1-0b-512B-N-flush-10000000n-update.rdtimes \
  dram-ralloc-19-8-NF1-0b-1024B-N-flush-10000000n-update.rdtimes >nf1.csv
