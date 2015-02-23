#! /bin/bash

psql -U john geomacro < prematch.sql

python match_parallel.py
python no_space_match_parallel.py

echo "DONESKI"
