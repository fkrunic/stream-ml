# Stream Scoring in R

The motivation behind `example.R` is to show to score potentially large datasets
in a memory-efficient way. It utilizes parallel computatation to score "chunks"
of data, and then save them to disk so they can be uploaded back to whatever
data store is needed. 

The parallel computation is used for speed, but could be removed if sequential
scoring is fast-enough. For large data, this is unlikely to be the case. 

To run the script, do `make run` on the command-line while in the directory 
containing the code. 