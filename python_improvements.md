# Part 2: How you would you make your script more robust?

## Timestamps

Adding timestamps to each output file would help with versioning and identifying if the data has not been updated recently.

## Appending data

Instead of overwriting the files, the script could append new data to the bottom of the file, with timestamp column acting as a versioning system. This way the evolution of the playlists, etc. can be analyzed.

## File format

Parquet file format could be used as an alternative to store the data. This would eliminate the need of the extra zipping step (last part of the script), as well as help with versioning and partitioning the data.

## Orchestration and scheduling

The running of the script can be scheduled with tools like Airflow, which would also allow us to chain the execution of this pipeline to other pipelines as well as monitor the results of every run easily.
