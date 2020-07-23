# *-Makefile-*

run: clean
	Rscript example.R

clean:
	rm tmp/* model/*