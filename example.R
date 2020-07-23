# ------------------------------------------------------------------------------
# Libraries for parallelization
# ------------------------------------------------------------------------------

library("foreach")
library("doMC")

# This library is for parallelization on Windows, in case that is ever needed. 
# library(doSnow)

# ------------------------------------------------------------------------------
# Other libraries related to the script
# ------------------------------------------------------------------------------

library("caret")
library("DBI")
library("RSQLite")

# ------------------------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------------------------

generate_query <- function(index, total_chunks) {

    # The `index` and `total_chunks` don't do anything special here, except
    # acting like a random record filter. Note the `%d` within the `sprintf`
    # template string which is what you will use when parametrizing your own 
    # queries.

    # In case your string template has a natural '%' within it, it must be
    # escaped by putting another '%' in front of it as you see in this example.

    # For a list of templating parameters for `sprintf` strings, see: 
    # https://www.rdocumentation.org/packages/base/versions/3.6.2/topics/sprintf

    sprintf(
        "SELECT * FROM mtcars where seq %% %d = %d - 1", 
        total_chunks, 
        index
    )
}

generate_ingress_path <- function(index, total_chunks) {

    # We use `file.path` instead of a larger `sprintf` template string to 
    # create the file-path because `file.path` will correctly escape illegal
    # path characters and works across Windows, Mac and Linux. 

    # You don't have to use `file.path`, but it's kind of standard so I would
    # use it. 

    file.path("tmp", sprintf("ingress_%d_%d.csv", total_chunks, index))
}

generate_egress_path <- function(index, total_chunks) {

    # Similar to `generate_egress_path` except we change the file prefix. 

    file.path("tmp", sprintf("egress_%d_%d.csv", total_chunks, index))
}

# ------------------------------------------------------------------------------
# Script constants
# ------------------------------------------------------------------------------

# Path where the model is stored on disk so it can be loaded. 
MODEL_PATH <- file.path("model", "model.rds")

# The number of "chunks" that the master data should be split up into. This 
# should be a large number to keep each "chunk" relatively small in memory. In
# this script, it is kept small for convenience purposes. This could be set to
# a large number safely (e.g. 10000)
INGRESS_CHUNKS <- 3

# This determines how many threads will execute will execute simultaneously. 
# All the threads will compete greedily for work available, but the end-user
# doesn't have to manage that. 
#
# For example, suppose I need to score 200GB worth of data. I likely cannot fit
# this all into memory, so I need to chunk the results into smaller pieces. 
# If my machine has 16GB of memory, I want to utilize about 14GB of memory for
# the scoring and leave the rest to system processes. 
#
# Suppose your Linux machine has four cores, and thus four threads to execute
# instructions in parallel. Since I want to keep the _overall_ memory limitation
# to 14GB, this means each thread should use about 3.5GB of memory. As a result,
# the `INGRESS_CHUNKS` should be set so that each chunk of data is smaller than
# 3.5GB in memory. 
#
# Each parallel iteration consumes only one chunk at a time, and the work is
# distributed evenly across the threads on a first-come first-served basis,
# meaning once a thread has finished its work, it pulls the next chunk 
# "from the queue" and begins to score it. This ensures that across all the 
# threads, no more than 14GB is being used at a time, thus ensuring you have 
# "fixed" memory usage. Similarly, all 200GB will eventually be scored, provided
# they fit on disk. 
NUM_THREADS <- 4
registerDoMC(NUM_THREADS)

# ------------------------------------------------------------------------------
# Step -1: Train a simple regression model to predict `mpg` from the classic
#   `mtcars` dataset and save it offline to reflect a realistic scoring
#   workflow. 
# ------------------------------------------------------------------------------

model <- lm(mpg ~., data = mtcars)
saveRDS(model, file = MODEL_PATH)

# ------------------------------------------------------------------------------
# Step 0: Load data into a SQLite database sitting in memory.
# ------------------------------------------------------------------------------

con <- dbConnect(RSQLite::SQLite(), ":memory:")
mtcars$seq = 1:nrow(mtcars)
dbWriteTable(con, "mtcars", mtcars)

# ------------------------------------------------------------------------------
# Step 1: Query the data and save into into a temporary location. This step
#   is not done in parallel since some databases do not support parallel
#   access within the same client. 

#   Your mileage may vary - it's worth trying the approach from Step 3 to speed 
#   up data ingestion, but be aware all threads pull from the same system 
#   memory, so if each loop iteration requires 5GB of memory to store the 
#   data-pull, across four different `INGRESS_CHUNKS` your total memory 
#   usage will be at least 20GB. 
# ------------------------------------------------------------------------------

for (i in 1:INGRESS_CHUNKS) {

    # This represents querying the data from some expensive source; a 
    # parametrized query is constructed, and the output file is saved according
    # to how the data was parametrized.

    chunk_query <- generate_query(i, INGRESS_CHUNKS)
    data_chunk <- dbGetQuery(con, chunk_query)
    chunk_save_path <- generate_ingress_path(i, INGRESS_CHUNKS)

    write.csv(x = data_chunk, file = chunk_save_path, row.names = FALSE)

}

# ------------------------------------------------------------------------------
# Step 2: Load the model into memory from disk. This should be done once and 
#   before the scoring happens in parallel. 
# ------------------------------------------------------------------------------

model <- readRDS(MODEL_PATH)

# ------------------------------------------------------------------------------
# Step 3. Execute the scoring in parallel. The reason we create separate
#   ingress chunks was to avoid locking when multiple threads attempt to access
#   the same file to pull in the data. 
#
#   The same is true when we output the data as a result of each scoring
#   iteration, which you will see below. 
# ------------------------------------------------------------------------------

# We throw-away the results from the parallel execution since we care about
# the side-effect and not the return value. The variable name for storing the
# result is arbitrary and not significant. 

throw_away <- foreach(i = 1:INGRESS_CHUNKS) %dopar% {

    # Load the ingress data for each thread. 
    chunk_save_path <- generate_ingress_path(i, INGRESS_CHUNKS)
    data_chunk <- read.csv(file = chunk_save_path)

    # Generate predictions for each chunk; if there are fields you want to
    # preserve for "egress", this is the place to add or drop columns.
    predictions <- predict(model, data_chunk)
    data_chunk$scores <- predictions
    
    # Save the results to disk; all the files are different to prevent access
    # locks from multiple threads writing to the same file. 
    output_save_path <- generate_egress_path(i, INGRESS_CHUNKS)
    write.csv(x = data_chunk, file = output_save_path, row.names = FALSE)
}

# ------------------------------------------------------------------------------
# Step 4: Upload the scores to the database. Like Step 0, this is not done in 
#   parallel since some databases do not support simultaneous, parallel access
#   (note, this is different from concurrent access). 
#
#   However, try the approach from Step 3 and see if it works; if it does not
#   you will see an error and can use the original for-loop method.
# ------------------------------------------------------------------------------

for (i in 1:INGRESS_CHUNKS) {
    output_save_path <- generate_egress_path(i, INGRESS_CHUNKS)
    egress_chunk <- read.csv(file = output_save_path)

    # We use the first egress chunk to create a new table, and the rest will 
    # insert into that table instead. 

    if (i == 1) {
        dbWriteTable(con, "mtcars_scores", egress_chunk)
    }
    else {
        dbAppendTable(con, "mtcars_scores", egress_chunk)
    }
}

# ------------------------------------------------------------------------------
# Step 5: Optional clean-up step to remove ingress/egress files in temporary
#   folder. Alternatively, see `Makefile` for another solution.
# ------------------------------------------------------------------------------

# Be careful with this - once you delete it, you cannot get it back. It is not
# like the "recycle bin" on Windows / MacOS.
# unlink("tmp", recursive = TRUE, force = TRUE)