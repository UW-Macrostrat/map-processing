# Map processing
Various scripts for working between and among GMUS/GMNA and Macrostrat

## credentials.py
Update this first. Everything else depends on it.

## rebuild
Rebuilds everything that can/should be easily rebuilt, including the entire ````macrostrat```` schema, ````gmna.lookup_units````, ````gmus.lookup_units````, and ````gmus.best_geounits_macrounits````.

What it does:

1. Delete all CSVs in current folder
2. Dump ````unit_strat_names````, ````strat_names````, ````units_sections````, ````intervals````, ````lookup_unit_intervals````, ````units````, ````lookup_strat_names````, ````cols````, ````col_areas````, ````liths````, and ````lith_atts```` from MySQL to CSVs
3. Set permissions to ````777```` on all dumped CSVs
4. Drop and recreate the schema ````macrostrat```` from Postgres
5. Import all of the above tables into Postgres, building appropriate indices along the way.
6. ````VACUUM ANALYZE```` all imported tables
7. Delete all CSVs
8. Rebuild ````gmna.lookup_units````
9. Rebuild ````gmus.best_geounits_macrounits````
10. Rebuild ````gmus.lookup_units````
11. Vacuum ````gmna.lookup_units````, ````gmus.best_geounits_macrounits````, and ````gmus.lookup_units````
12. Fill the columns ````macro_containing_interval_id````, ````macro_min_interval_id````, and ````macro_max_interval_id```` in ````gmus.ages````. Parses the age ranges in ````gmus.ages```` and finds the corresponding Macrostrat interval IDs, along with whatever Macrostrat interval contains the min and max intervals.

The Postgres version of Macrostrat is as true to the MySQL version as possible, data types and indices included. **NB:** one major difference is that the table ````cols```` contains an additional field ````poly_geom```` that contains the geometry from ````col_areas.col_area````. It is simply a convinience.

## match
Attempts to match GMUS polygons to Macrostrat units on the basis of space, time, and description. Comprised of various scripts that are all run at once with ````./match.sh````.

#### prematch
Create and populate the table ````gmus.geounits_macrounits_types````. Create the table ````gmus.geounits_macrounits````, which is a join table that will hold the GMUS -> Macrostrat matches.

#### match_parallel
Match GMUS polygons to Macrostrat units and strat_name_ids on the basis of space, time, and string matches

#### match\_no\_space_parallel
Match GMUS polygons to Macrostrat units and strat_name_ids on the basis of time and string matches (excludes all GMUS polygons matched in the first step)

#### miss_parallel
Often times some GMUS polygons of a certain ````unit_link```` will be matched to Macrostrat units, and other GMUS polygons of the same ````unit_link```` will not be matched. Take all GMUS polygons whose ````unit_link```` has matches and apply those matches to the unmatched GMUS polygon.

#### find_polys
Manual ````unit_link```` - ````strat_name_id```` matches

1. Given a list of matched ````unit_link```` and ````strat_name_id```` in ````matches.txt````...
2. Find all Macrostrat units with the given ````strat_name_id````
3. Find all GMUS polygons with the given ````unit_link````
4. Find the distance between each feature in #2 and #3
5. Create a new match in ````gmus.geounits_macrounits```` between the GMUS unit(s) and Macrostrat unit(s) with the smallest distance between them. Each of these matches will be of ````type```` 99.

#### inherit_bests
If a GMUS polygon has a low quality match (i.e. not matched on ````unit_name```` or ````strat_unit````), but a polygon of the same ````unit_link```` has (a) high quality match(es), that GMUS polygon should inherit those high quality matches.

## get-gmus-attributes
Get all GMUS attribute data from USGS (both CSVs and JSON), import it into Postgres, and find the best attributes out of each. ````run.sh```` is the task runner and excutes the below tasks in one fell swoop

1. Remove the directory ````csvs```` if it exists
2. Download all the GMUS CSV data from USGS
3. Unzip the contents of each state into the directory ````csvs````
4. Delete the original ````.zip```` files
5. Run ````setup.sql```` on the database ````geomacro````. This drops and recreates the tables ````gmus.ages````, ````gmus.liths````, ````gmus.reflinks````, ````gmus.refs````, and ````gmus.units````.
6. Run ````import.py````. This script cleans up the contents of each CSV file and inserts the cleaned data into Postgres.
7. Run ````fix_text.py````. Because some of the long text fields from the CSVs are truncated prematurely, we use the existing ````unit_link````s to get the "same" data from the USGS JSON service. For each ````unit_link````, we re-record the ````unit_name````, ````unitdesc````, ````unit_com````, and ````strat_unit```` in the respective ````new_X```` field in ````gmus.units````.
8. Run ````best_attributes.py````. This populates the ````best_*```` fields using the best data from either the CSV or JSON response.
 

