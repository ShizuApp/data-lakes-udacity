# Data Lake with Spark

## Overview

Extracts data from AWS S3(Cloud Storage) collected by a music streaming app, Sparkify, to process the data into analytics tables using a Spark running in AWS, and then loads them back into S3. Models the data into a star schema for running optimaze queries to meet the goals of the analytics team of the app.

## Files

- **dl.cfg**: Configuration file containing AWS IAM credentials
- **etl.py**: Read and write data to S3 and process it using a Spark cluster

## Schema Design
This project implements a star schema. Using `songplays` as the fact table and `songs`, `artists`, `users` and `time` as dimensional tables.

### Fact Table

- **songplays** (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

### Dimension Tables

- **users** (user_id, first_name, last_name, gender, level)
- **songs** (song_id, title, artist_id, year, duration)
- **artists** (artist_id, name, location, latitude, longitude)
- **time** timestamps of records in songplays broken down into specific units (start_time, hour, day, week, month, year, weekday)


## How to Run
1. Add appropriate AWS IAM Credentials and Input/Output path to `dl.cfg`
2. Run `etl.py`