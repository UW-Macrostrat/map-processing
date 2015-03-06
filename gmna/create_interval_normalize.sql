CREATE TABLE gmna.interval_normalize AS

WITH gmna_age AS (select distinct min_age AS gmna_interval from gmna.geologic_units where lower(min_age) IN (SELECT distinct lower(interval_name) from macrostrat.intervals) order by min_age asc),
  macro_age AS (select distinct min_age AS macro_interval from gmna.geologic_units where lower(min_age) IN (SELECT distinct lower(interval_name) from macrostrat.intervals) order by min_age asc)

select * from gmna_age
JOIN macro_age on gmna_age.gmna_interval = macro_age.macro_interval;

COPY gmna.interval_normalize FROM '/Users/john/code/macrostrat/map_processing/gmna/age_mapping.csv' DELIMITER ',' CSV;
