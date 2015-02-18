# Map processing
Various scripts for working between and among GMUS/GMNA and Macrostrat

## credentials.py
Update this first. Everything else depends on it.

## mysql-to-pg.py
1. Delete all CSVs in current folder
2. Dump ````unit_strat_names````, ````strat_names````, ````units_sections````, ````intervals````, ````lookup_unit_intervals````, ````units````, and ````lookup_strat_names```` from MySQL to CSVs
3. Set permissions to ````777```` on all dumped CSVs
4. Drop and recreate the schema ````new_macrostrat```` from Postgres
5. Import all of the above tables into Postgres
6. ````VACUUM ANALYZE```` all imported tables
7. Delete all CSVs

## match
First pass at matching GMUS polygons to Macrostrat units

## match-sans-spatial
Second pass at matching GMUS polygons to Macrostrat. Ignores all GMUS polygons already matched to Macrostrat polygons on the first pass, and attempts to match only on strings

## unit-link-strat-name
1. Given a list of matched ````unit_link```` and ````strat_name_id```` in ````matches.txt````...
2. Find all Macrostrat units with the given ````strat_name_id````
3. Find all GMUS polygons with the given ````unit_link````
4. Find the distance between each feature in #2 and #3
5. Create a new match in ````gmus.geounits_macrounits_redo```` between the GMUS unit(s) and Macrostrat unit(s) with the smallest distance between them. Each of these matches will be of ````type```` 99.

## get-gmus-attributes
````run.sh```` is the task runner and excutes the below tasks in one fell swoop

1. Remove the directory ````csvs```` if it exists
2. Download all the GMUS CSV data from USGS
3. Unzip the contents of each state into the directory ````csvs````
4. Delete the original ````.zip```` files
5. Run ````setup.sql```` on the database ````geomacro````. This drops and recreates the tables ````gmus.ages````, ````gmus.liths````, ````gmus.reflinks````, ````gmus.refs````, and ````gmus.units````.
6. Run ````import.py````. This script cleans up the contents of each CSV file and inserts the cleaned data into Postgres.
7. Run ````fix_text.py````. Because some of the long text fields from the CSVs are truncated prematurely, we use the existing ````unit_link````s to get the "same" data from the USGS JSON service. For each ````unit_link````, we re-record the ````unit_name````, ````unitdesc````, ````unit_com````, and ````strat_unit```` in the respective ````new_X```` field in ````gmus.units````.