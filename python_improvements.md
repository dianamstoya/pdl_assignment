# Part 2: How you would you make your script more robust?

## Multiple categories

The variable category_id can be passed to the script as a command-line argument and read using argparse. This would allow the same script to be reused for any category. Changes required: the file category_playlists_records.csv needs to be expanded with one column to add the category name/id. Also if the app.py is going to be executed multiple times, a different output method needs to be used (see point Appending data below).

## Timestamps

Adding timestamps to each output file would help with versioning and identifying if the data has not been updated recently.

## Appending data

Instead of overwriting the files, the script could append new data to the bottom of the file, with timestamp column acting as a versioning system. This way the evolution of the playlists, etc. can be analyzed.

## File format

Parquet file format could be used as an alternative to store the data. This would eliminate the need of the extra zipping step (last part of the script), as well as help with versioning and partitioning the data.

## Orchestration and scheduling

The running of the script can be scheduled with tools like Airflow, which would also allow us to chain the execution of this pipeline to other pipelines as well as monitor the results of every run easily.
