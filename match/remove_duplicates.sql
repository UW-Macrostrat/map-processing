CREATE INDEX ON gmus.geounits_macrounits (id);
CREATE INDEX ON gmus.geounits_macrounits (geologic_unit_gid);
CREATE INDEX ON gmus.geounits_macrounits (unit_id);
CREATE INDEX ON gmus.geounits_macrounits (strat_name_id);
CREATE INDEX ON gmus.geounits_macrounits (unit_link);

VACUUM ANALYZE gmus.geounits_macrounits;

DROP TABLE IF EXISTS gmus.geounits_macrounits_backup;
CREATE TABLE gmus.geounits_macrounits_backup AS SELECT * FROM gmus.geounits_macrounits;

CREATE TABLE gmus.temp_duplicates AS select id from (
  select id, geologic_unit_gid, unit_id, strat_name_id, unit_link, type, row_number() OVER(PARTITION BY geologic_unit_gid, unit_id, strat_name_id, unit_link ORDER BY type asc) AS row
  FROM gmus.geounits_macrounits
) dups
WHERE dups.Row > 1
ORDER BY geologic_unit_gid, unit_id, strat_name_id, unit_link, type asc;

DELETE FROM gmus.geounits_macrounits gm USING gmus.temp_duplicates tn
WHERE gm.id = tn.id;

DROP TABLE gmus.temp_duplicates;
