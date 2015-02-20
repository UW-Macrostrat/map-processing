CREATE TABLE gmus.lookup_units AS 
SELECT gid, gu.state, gu.unit_link, source, gu.unit_age, gu.rocktype1, gu.rocktype2, new_unit_name AS unit_name, new_unitdesc AS unitdesc, new_strat_unit AS strat_unit, new_unit_com AS unit_com, u.rocktype1 AS u_rocktype1, u.rocktype2 AS u_rocktype2, u.rocktype3 AS u_rocktype3, i.interval_color, i.interval_name, a.macro_interval AS macro_interval_id, geom
FROM gmus.geologic_units gu
LEFT JOIN gmus.units u ON gu.unit_link = u.unit_link
LEFT JOIN gmus.ages a ON gu.unit_link = a.unit_link
LEFT JOIN macrostrat.intervals i ON a.macro_interval = i.id;

CREATE INDEX ON gmus.lookup_units (gid);
CREATE INDEX ON gmus.lookup_units (state);
CREATE INDEX ON gmus.lookup_units (unit_link);
CREATE INDEX ON gmus.lookup_units (macro_interval_id);
CREATE INDEX ON gmus.lookup_units USING GIST (geom);
