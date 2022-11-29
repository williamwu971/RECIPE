.PHONY: rdtimes

push:
	git commit -am "-AUTO- `date`" || true
	git push || true

download:
	mkdir pngs || true
	mkdir stats || true
	#scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sda/xiaoxiang/RECIPE/P-Masstree/build/log_debug_print_*.txt' .
	#scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sdb/xiaoxiang/RECIPE/P-Masstree/build/*.png' .
#	scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sdb/xiaoxiang/RECIPE/P-Masstree/*.perf' .
#	scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sdb/xiaoxiang/RECIPE/P-Masstree/*.old' .
	scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sdb/xiaoxiang/RECIPE/P-Masstree/build/*.csv' .
#	scp xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sda/xiaoxiang/RECIPE/P-Masstree/build/lookup.csv .

	#scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sdb/xiaoxiang/RECIPE/P-Masstree/build/*.stat' .
	#scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sdb/xiaoxiang/RECIPE/P-Masstree/build/max_latencies.txt' .

rdtimes:
	mkdir rdtimes || true
	mkdir rdtimes/insert || true
	mkdir rdtimes/update || true
	mkdir rdtimes/lookup || true
	mkdir rdtimes/delete || true
	scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sdb/xiaoxiang/RECIPE/P-Masstree/build/*.rdtimes' .
	mv *insert.rdtimes rdtimes/insert
	mv *update.rdtimes rdtimes/update
	mv *lookup.rdtimes rdtimes/lookup
	mv *delete.rdtimes rdtimes/delete

everything:
	scp 'xiaoxiang@labos2.cs.usyd.edu.au:/mnt/sdb/xiaoxiang/RECIPE/P-Masstree/build/*.{png,csv,out,stat,txt}' .
	mv *.png pngs/
	mv *.stat stats/

clean:
	rm -f *.csv *.png

