.PHONY: rdtimes stat png push

push:
	git commit -am "-AUTO- `date`" || true
	git push || true

download:
#	mkdir pngs || true
#	mkdir stats || true
	#scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sda/xiaoxiang/RECIPE/P-Masstree/build/log_debug_print_*.txt' .
	#scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sdb/xiaoxiang/RECIPE/P-Masstree/build/*.png' .
#	scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sdb/xiaoxiang/RECIPE/P-Masstree/*.perf' .
#	scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sdb/xiaoxiang/RECIPE/P-Masstree/*.old' .
	scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sdb/xiaoxiang/RECIPE/P-Masstree/build/*.csv' .
#	scp xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sda/xiaoxiang/RECIPE/P-Masstree/build/lookup.csv .

	#scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sdb/xiaoxiang/RECIPE/P-Masstree/build/*.stat' .
	#scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sdb/xiaoxiang/RECIPE/P-Masstree/build/max_latencies.txt' .

rdtimes:
	rm -rf rdtimes
	mkdir rdtimes || true
	mkdir rdtimes/insert || true
	mkdir rdtimes/update || true
	mkdir rdtimes/lookup || true
	mkdir rdtimes/delete || true
	scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sdb/xiaoxiang/RECIPE/P-Masstree/build/*-64B-*10000000n*.rdtimes' .
	mv *insert.rdtimes rdtimes/insert
	mv *update.rdtimes rdtimes/update
	mv *lookup.rdtimes rdtimes/lookup
	mv *delete.rdtimes rdtimes/delete

update_rdtimes:
	#rm -rf rdtimes
	#mkdir rdtimes || true
	#mkdir rdtimes/update || true
	scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sdb/xiaoxiang/RECIPE/P-Masstree/build/*update*.rdtimes' ./rdtimes/update/


stat:
	mkdir stat || true
	mkdir stat/insert || true
	mkdir stat/update || true
	mkdir stat/lookup || true
	mkdir stat/delete || true
	scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sdb/xiaoxiang/RECIPE/P-Masstree/build/*.stat' .
	mv *insert.perf.stat stat/insert
	mv *update.perf.stat stat/update
	mv *lookup.perf.stat stat/lookup
	mv *delete.perf.stat stat/delete

png:
	scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sdb/xiaoxiang/RECIPE/P-Masstree/build/*.png' pngs/

clean:
	#scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sdb/xiaoxiang/RECIPE/P-Masstree/build/*.{png,csv,out,stat,txt}' pngs/
	rm -f *.csv *.png

