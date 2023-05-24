#cp latex.txt template.txt
cp latex_colour.txt template.txt
#
python3 parse.py \
  dram-log_best-19-0-NF1-0b-64B-N-flush-10000000n-update.rdtimes \
  dram-log_best-19-0-NF1-0b-128B-N-flush-10000000n-update.rdtimes \
  dram-log_best-19-0-NF1-0b-256B-N-flush-10000000n-update.rdtimes \
  dram-log_best-19-0-NF1-0b-512B-N-flush-10000000n-update.rdtimes \
  dram-log_best-19-0-NF1-0b-1024B-N-flush-10000000n-update.rdtimes >nf1.csv

python3 parse.py \
  dram-log_best-19-0-NF0-0b-64B-N-flush-10000000n-update.rdtimes \
  dram-log_best-19-0-NF0-0b-128B-N-flush-10000000n-update.rdtimes \
  dram-log_best-19-0-NF0-0b-256B-N-flush-10000000n-update.rdtimes \
  dram-log_best-19-0-NF0-0b-512B-N-flush-10000000n-update.rdtimes \
  dram-log_best-19-0-NF0-0b-1024B-N-flush-10000000n-update.rdtimes >nf0.csv

#python3 parse.py \
#  dram-ralloc-19-0-NF0-0b-64B-N-flush-10000000n-update.rdtimes \
#  dram-ralloc-19-0-NF0-0b-128B-N-flush-10000000n-update.rdtimes \
#  dram-ralloc-19-0-NF0-0b-256B-N-flush-10000000n-update.rdtimes \
#  dram-ralloc-19-0-NF0-0b-512B-N-flush-10000000n-update.rdtimes \
#  dram-ralloc-19-0-NF0-0b-1024B-N-flush-10000000n-update.rdtimes
#
#python3 parse.py \
#  dram-log_best-19-0-NF0-0b-64B-N-flush-10000000n-update.rdtimes \
#  dram-log_best-19-0-NF0-0b-128B-N-flush-10000000n-update.rdtimes \
#  dram-log_best-19-0-NF0-0b-256B-N-flush-10000000n-update.rdtimes \
#  dram-log_best-19-0-NF0-0b-512B-N-flush-10000000n-update.rdtimes \
#  dram-log_best-19-0-NF0-0b-1024B-N-flush-10000000n-update.rdtimes
