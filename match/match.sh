#! /bin/bash

psql -U john earthbase < prematch.sql

python match_parallel.py

echo "DONESKI"
