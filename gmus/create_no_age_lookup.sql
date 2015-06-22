CREATE TABLE gmus.no_age_lookup (
  unit_age text,
  min_eon text,
  min_era text,
  min_period text,
  min_epoch text,
  max_eon text,
  max_era text,
  max_period text,
  max_epoch text
);

COPY gmus.no_age_lookup FROM '/Users/john/code/macrostrat/map_processing/gmus/no_age_lookup.csv' NULL '\N' DELIMITER ',' CSV;

INSERT INTO gmus.ages(unit_link, min_eon, min_era, min_period, min_epoch, max_eon, max_era, max_period, max_epoch)
  WITH orphan_unit_links AS (
    SELECT DISTINCT unit_link, unit_age 
    FROM gmus.geologic_units 
    WHERE unit_link NOT IN (SELECT DISTINCT unit_link FROM gmus.ages)
  )
  SELECT distinct on (geologic_units.unit_link) geologic_units.unit_link, min_eon, min_era, min_period, min_epoch, max_eon, max_era, max_period, max_epoch 
  FROM gmus.geologic_units
  JOIN orphan_unit_links ON orphan_unit_links.unit_link = geologic_units.unit_link
  JOIN gmus.no_age_lookup ON orphan_unit_links.unit_age = no_age_lookup.unit_age;
  