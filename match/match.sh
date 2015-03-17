#! /bin/bash

psql -U john geomacro < prematch.sql

python match_parallel.py
python no_space_match_parallel.py
python miss_parallel.py
python find_polys.py
python inherit_bests.py
psql -U john geomacro < remove_duplicates.sql

echo "DONESKI"
