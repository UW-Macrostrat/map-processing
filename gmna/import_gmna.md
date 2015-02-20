pgsql2shp -f eb_gmna.shp -h localhost -u john earthbase "select gid, objectid, unit_abbre, rocktype, lithology, min_age, max_age, min_max_re, unit_uncer, age_uncert, map_unit_n,  eb_lith_id AS macro_lith_id, eb_interval_id AS macro_containing_interval_id, eb_min_interval_id AS macro_min_interval_id, eb_max_interval_id AS macro_max_interval_id, st_setsrid(the_geom,4326) from gmna.geologic_units"

shp2pgsql -s 4326 eb_gmna.shp gmna.geologic_units | psql geomacro

ALTER TABLE gmna.geologic_units ADD COLUMN geom_clean geometry;
UPDATE gmna.geologic_units SET geom_clean = st_snaptogrid(st_buffer(st_makevalid(geom),0),0.00001);

ALTER TABLE gmna.geologic_units DROP COLUMN geom;
ALTER TABLE gmna.geologic_units RENAME COLUMN geom_clean to geom;

ALTER TABLE gmna.geologic_units rename column macro_lith to macro_lith_id;
ALTER TABLE gmna.geologic_units rename column macro_cont to macro_containing_interval_id;
ALTER TABLE gmna.geologic_units rename column macro_min_ to macro_min_interval_id;
ALTER TABLE gmna.geologic_units rename column macro_max_ to macro_max_interval_id;

CREATE INDEX ON gmna.geologic_units (gid);
CREATE INDEX ON gmna.geologic_units (macro_lith_id);
CREATE INDEX ON gmna.geologic_units (macro_containing_interval_id);
CREATE INDEX ON gmna.geologic_units (macro_min_interval_id);
CREATE INDEX ON gmna.geologic_units (macro_max_interval_id);
CREATE INDEX ON gmna.geologic_units USING GIST (geom);

vacuum analyze gmna.geologic_units;