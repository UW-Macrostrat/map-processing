import MySQLdb
import MySQLdb.cursors
import os
import psycopg2
from psycopg2.extensions import AsIs
import sys
import subprocess

sys.path = [os.path.join(os.path.dirname(__file__), os.pardir)] + sys.path
import credentials

from macro_intervals_to_gmus_ages import *

# Connect to Postgres
pg_conn = psycopg2.connect(dbname=credentials.pg_db, user=credentials.pg_user, host=credentials.pg_host, port=credentials.pg_port)
pg_cur = pg_conn.cursor()

# Connect to MySQL
my_conn = MySQLdb.connect(host=credentials.mysql_host, user=credentials.mysql_user, passwd=credentials.mysql_passwd, db=credentials.mysql_db, unix_socket=credentials.mysql_socket, cursorclass=MySQLdb.cursors.DictCursor)
my_cur = my_conn.cursor()





# Functions for step #8
def is_valid(interval) :
  interval = replace_precam(interval)
  if (interval is not None) and (len(interval) > 1) and ((interval in interval_lookup) or (' '.join(fix_parts(interval.split("-"))) in interval_lookup)):
    return True
  else :
    return False

def fix_parts(parts) :
  for part in parts :
    replace_precam(part)
  return parts

def replace_precam(interval) :
  if interval == "preCambrian" :
    return "Precambrian"
  else :
    return interval

def parse_range(min_interval, max_interval, unit_link):
  min_interval = replace_precam(min_interval)
  max_interval = replace_precam(max_interval)

  if "-" in min_interval:
    parts = fix_parts(min_interval.split("-"))

    if (parts[0] == "Early") or (parts[0] == "Late") or (parts[0] == "Middle") :
      min_interval = ' '.join(parts)
    else :
      print "MIN_INTERVAL DOESN'T HAVE EARLY LATE OR MIDDLE ", min_interval

  if "-" in max_interval:
    parts = fix_parts(max_interval.split("-"))
    if (parts[0] == "Early") or (parts[0] == "Late") or (parts[0] == "Middle") :
      max_interval = ' '.join(parts)
    else :
      print "MAX_INTERVAL DOESN'T HAVE EARLY LATE OR MIDDLE ", max_interval

  try :
    age_bottom = interval_lookup[max_interval][1]
  except :
    if max_interval.split(" ")[0] == "Late" :
      max_interval = "Upper " + max_interval.split(" ")[1]
      age_bottom = interval_lookup[max_interval][1]
    elif max_interval.split(" ")[0] == "Early" :
      max_interval = "Lower " + max_interval.split(" ")[1]
      age_bottom = interval_lookup[max_interval][1]
    elif max_interval == "preCambrian" :
      age_bottom = interval_lookup["Precambrian"][1]
    else :
      print "MESS UP MAX", max_interval

  try :
    age_top = interval_lookup[min_interval][2]
  except :
    if min_interval.split(" ")[0] == "Late" :
      min_interval = "Upper " + min_interval.split(" ")[1]
      age_top = interval_lookup[min_interval][2]
    elif min_interval.split(" ")[0] == "Early" :
      min_interval = "Lower " + min_interval.split(" ")[1]
      age_top = interval_lookup[min_interval][2]
    elif min_interval == "preCambrian" :
      age_top = interval_lookup["Precambrian"][2]
    else :
      print "MESS UP MIN", min_interval

  min_interval_id = interval_lookup[replace_precam(min_interval)][0]
  max_interval_id = interval_lookup[replace_precam(max_interval)][0]

  pg_cur.execute("""
    SELECT id, interval_name, interval_color 
    FROM macrostrat.intervals 
    LEFT JOIN macrostrat.timescales_intervals ON intervals.id = timescales_intervals.interval_id 
    WHERE age_bottom >= %s AND age_top <= %s 
    AND (timescale_id != 6 or timescale_id is null) 
    ORDER BY rank DESC, id asc 
    LIMIT 1""", [age_bottom, age_top])

  match = pg_cur.fetchall()
  if len(match) > 0:
    update(match[0][0], min_interval_id, max_interval_id, unit_link)
  else :
    print str(unit_link) + "\r"

def update(containing_interval_id, min_interval_id, max_interval_id, unit_link) :
  pg_cur.execute("UPDATE gmus.ages SET macro_containing_interval_id = %s, macro_min_interval_id = %s, macro_max_interval_id = %s WHERE unit_link = %s", [containing_interval_id, min_interval_id, max_interval_id, unit_link])
  pg_conn.commit()





# Start the process

# Remove existing CSVs
subprocess.call("rm *.csv", shell=True)

directory = os.getcwd()

params = {
  "unit_strat_names_path": directory + "/unit_strat_names.csv",
  "strat_names_path": directory + "/strat_names.csv",
  "units_sections_path": directory + "/units_sections.csv",
  "intervals_path": directory + "/intervals.csv",
  "lookup_unit_intervals_path": directory + "/lookup_unit_intervals.csv",
  "units_path": directory + "/units.csv",
  "lookup_strat_names_path": directory + "/lookup_strat_names.csv",
  "cols_path": directory + "/cols.csv",
  "col_areas_path": directory + "/col_areas.csv",
  "liths_path": directory + "/liths.csv",
  "lith_atts_path": directory + "/lith_atts.csv",
  "timescales_intervals_path": directory + "/timescales_intervals.csv",
  "macrostrat_schema": AsIs(credentials.pg_macrostrat_schema)
}




print "(1 of 8)   Dumping from MySQL"
my_cur.execute("""

  SELECT * FROM unit_strat_names
  INTO OUTFILE %(unit_strat_names_path)s
  FIELDS TERMINATED BY ','
  ENCLOSED BY '"'
  LINES TERMINATED BY '\n';

  SELECT * FROM strat_names
  INTO OUTFILE %(strat_names_path)s
  FIELDS TERMINATED BY ','
  ENCLOSED BY '"'
  LINES TERMINATED BY '\n';

  SELECT * FROM units_sections
  INTO OUTFILE %(units_sections_path)s
  FIELDS TERMINATED BY ','
  ENCLOSED BY '"'
  LINES TERMINATED BY '\n';


  SELECT * FROM intervals
  INTO OUTFILE %(intervals_path)s
  FIELDS TERMINATED BY ','
  ENCLOSED BY '"'
  LINES TERMINATED BY '\n';

  SELECT * FROM lookup_unit_intervals
  INTO OUTFILE %(lookup_unit_intervals_path)s
  FIELDS TERMINATED BY ','
  ENCLOSED BY '"'
  LINES TERMINATED BY '\n';

  SELECT id, strat_name, color, outcrop, FO, FO_h, LO, LO_h, position_bottom, position_top, max_thick, min_thick, section_id, col_id FROM units
  INTO OUTFILE %(units_path)s
  FIELDS TERMINATED BY ','
  ENCLOSED BY '"'
  LINES TERMINATED BY '\n';

  SELECT * FROM lookup_strat_names
  INTO OUTFILE %(lookup_strat_names_path)s
  FIELDS TERMINATED BY ','
  ENCLOSED BY '"'
  LINES TERMINATED BY '\n';

  SELECT id, col_group_id, project_id, status_code, col_position, col, col_name, lat, lng, col_area, null AS coordinate, ST_AsText(coordinate) AS wkt, created FROM cols
  INTO OUTFILE %(cols_path)s
  FIELDS TERMINATED BY ','
  ENCLOSED BY '"'
  LINES TERMINATED BY '\n';

  SELECT id, col_id, null as col_area, ST_AsText(col_area) AS wkt FROM col_areas
  INTO OUTFILE %(col_areas_path)s
  FIELDS TERMINATED BY ','
  ENCLOSED BY '"'
  LINES TERMINATED BY '\n';

  SELECT * FROM liths
  INTO OUTFILE %(liths_path)s
  FIELDS TERMINATED BY ','
  ENCLOSED BY '"'
  LINES TERMINATED BY '\n';

  SELECT * FROM lith_atts
  INTO OUTFILE %(lith_atts_path)s
  FIELDS TERMINATED BY ','
  ENCLOSED BY '"'
  LINES TERMINATED BY '\n';

  SELECT * FROM timescales_intervals
  INTO OUTFILE %(timescales_intervals_path)s
  FIELDS TERMINATED BY ','
  ENCLOSED BY '"'
  LINES TERMINATED BY '\n';

""", params)





subprocess.call("chmod 777 *.csv", shell=True)






print "(2 of 8)   Importing into Postgres"
pg_cur.execute(""" 

DROP SCHEMA IF EXISTS %(macrostrat_schema)s cascade;

CREATE SCHEMA %(macrostrat_schema)s;

CREATE TABLE %(macrostrat_schema)s.unit_strat_names (
  id serial PRIMARY KEY NOT NULL,
  unit_id integer NOT NULL,
  strat_name_id integer NOT NULL
);

COPY %(macrostrat_schema)s.unit_strat_names FROM %(unit_strat_names_path)s DELIMITER ',' CSV;

CREATE INDEX ON %(macrostrat_schema)s.unit_strat_names (id);
CREATE INDEX ON %(macrostrat_schema)s.unit_strat_names (unit_id);
CREATE INDEX ON %(macrostrat_schema)s.unit_strat_names (strat_name_id);


CREATE TABLE %(macrostrat_schema)s.strat_names (
  id serial PRIMARY KEY NOT NULL,
  strat_name character varying(100) NOT NULL,
  rank character varying(50),
  ref_id  integer NOT NULL
);


COPY %(macrostrat_schema)s.strat_names FROM %(strat_names_path)s DELIMITER ',' CSV;

CREATE TABLE %(macrostrat_schema)s.units_sections (
  id serial PRIMARY KEY NOT NULL,
  unit_id integer NOT NULL,
  section_id integer NOT NULL,
  col_id integer NOT NULL
);

COPY %(macrostrat_schema)s.units_sections FROM %(units_sections_path)s DELIMITER ',' CSV;


CREATE INDEX ON %(macrostrat_schema)s.units_sections (id);
CREATE INDEX ON %(macrostrat_schema)s.units_sections (unit_id);
CREATE INDEX ON %(macrostrat_schema)s.units_sections (section_id);
CREATE INDEX ON %(macrostrat_schema)s.units_sections (col_id);

CREATE TABLE %(macrostrat_schema)s.intervals (
  id serial NOT NULL,
  age_bottom numeric,
  age_top numeric,
  interval_name character varying(200),
  interval_abbrev character varying(50),
  interval_type character varying(50),
  interval_color character varying(20)
);

COPY %(macrostrat_schema)s.intervals FROM %(intervals_path)s NULL '\N' DELIMITER ',' CSV;

ALTER TABLE %(macrostrat_schema)s.intervals ADD COLUMN rank integer DEFAULT NULL;

INSERT INTO %(macrostrat_schema)s.intervals (id, interval_name, interval_color) VALUES (0, 'Age Unknown', '#737373'), (0, 'Unknown', '#737373');

UPDATE %(macrostrat_schema)s.intervals SET rank = 6 WHERE interval_type = 'age';
UPDATE %(macrostrat_schema)s.intervals SET rank = 5 WHERE interval_type = 'epoch';
UPDATE %(macrostrat_schema)s.intervals SET rank = 4 WHERE interval_type = 'period';
UPDATE %(macrostrat_schema)s.intervals SET rank = 3 WHERE interval_type = 'era';
UPDATE %(macrostrat_schema)s.intervals SET rank = 2 WHERE interval_type = 'eon';
UPDATE %(macrostrat_schema)s.intervals SET rank = 1 WHERE interval_type = 'supereon';
UPDATE %(macrostrat_schema)s.intervals SET rank = 0 WHERE rank IS NULL;

CREATE INDEX ON %(macrostrat_schema)s.intervals (id);
CREATE INDEX ON %(macrostrat_schema)s.intervals (age_top);
CREATE INDEX ON %(macrostrat_schema)s.intervals (age_bottom);
CREATE INDEX ON %(macrostrat_schema)s.intervals (interval_type);
CREATE INDEX ON %(macrostrat_schema)s.intervals (interval_name);


CREATE TABLE %(macrostrat_schema)s.lookup_unit_intervals (
  unit_id integer,
  FO_age numeric,
  b_age numeric,
  FO_interval character varying(50),
  FO_period character varying(50),
  LO_age numeric,
  t_age numeric,
  LO_interval character varying(50),
  LO_period character varying(50),
  age character varying(50),
  age_id integer,
  epoch character varying(50),
  epoch_id integer,
  period character varying(50),
  period_id integer,
  era character varying(50),
  era_id integer,
  eon character varying(50),
  eon_id integer
);

COPY %(macrostrat_schema)s.lookup_unit_intervals FROM %(lookup_unit_intervals_path)s NULL '\N' DELIMITER ',' CSV;

ALTER TABLE %(macrostrat_schema)s.lookup_unit_intervals ADD COLUMN best_interval_id integer;

WITH bests AS (
  select unit_id, 
    CASE 
      WHEN age_id > 0 THEN
        age_id
      WHEN epoch_id > 0 THEN
        epoch_id
      WHEN period_id > 0 THEN
        period_id
      WHEN era_id > 0 THEN
        era_id
      WHEN eon_id > 0 THEN
        eon_id
      ELSE
        0
    END
   AS b_interval_id from %(macrostrat_schema)s.lookup_unit_intervals
)
UPDATE %(macrostrat_schema)s.lookup_unit_intervals lui
SET best_interval_id = b_interval_id
FROM bests
WHERE lui.unit_id = bests.unit_id;

CREATE INDEX ON %(macrostrat_schema)s.lookup_unit_intervals (unit_id);
CREATE INDEX ON %(macrostrat_schema)s.lookup_unit_intervals (best_interval_id);



CREATE TABLE %(macrostrat_schema)s.units (
  id integer PRIMARY KEY,
  strat_name character varying(150),
  color character varying(20),
  outcrop character varying(20),
  FO integer,
  FO_h integer,
  LO integer,
  LO_h integer,
  position_bottom numeric,
  position_top numeric,
  max_thick numeric,
  min_thick numeric,
  section_id integer,
  col_id integer
);

COPY %(macrostrat_schema)s.units FROM %(units_path)s DELIMITER ',' CSV;


CREATE INDEX ON %(macrostrat_schema)s.units (id);
CREATE INDEX ON %(macrostrat_schema)s.units (section_id);
CREATE INDEX ON %(macrostrat_schema)s.units (col_id);
CREATE INDEX ON %(macrostrat_schema)s.units (strat_name);
CREATE INDEX ON %(macrostrat_schema)s.units (color);


CREATE TABLE %(macrostrat_schema)s.lookup_strat_names (
  strat_name_id integer,
  strat_name character varying(100),
  rank character varying(20),
  rank_name character varying(20),
  bed_id integer,
  bed_name character varying(100),
  mbr_id integer,
  mbr_name character varying(100),
  fm_id integer,
  fm_name character varying(100),
  gp_id integer,
  gp_name character varying(100),
  sgp_id integer,
  sgp_name character varying(100),
  early_age numeric,
  late_age numeric,
  gsc_lexicon character varying(20)
);

COPY %(macrostrat_schema)s.lookup_strat_names FROM %(lookup_strat_names_path)s NULL '\N' DELIMITER ',' CSV;


CREATE INDEX ON %(macrostrat_schema)s.lookup_strat_names (strat_name_id);
CREATE INDEX ON %(macrostrat_schema)s.lookup_strat_names (bed_id);
CREATE INDEX ON %(macrostrat_schema)s.lookup_strat_names (mbr_id);
CREATE INDEX ON %(macrostrat_schema)s.lookup_strat_names (fm_id);
CREATE INDEX ON %(macrostrat_schema)s.lookup_strat_names (gp_id);
CREATE INDEX ON %(macrostrat_schema)s.lookup_strat_names (sgp_id);
CREATE INDEX ON %(macrostrat_schema)s.lookup_strat_names (strat_name);


CREATE TABLE %(macrostrat_schema)s.cols (
  id integer PRIMARY KEY,
  col_group_id smallint,
  project_id smallint,
  status_code character varying(25),
  col_position character varying(25),
  col numeric,
  col_name character varying(100),
  lat numeric,
  lng numeric,
  col_area numeric,
  coordinate geometry,
  wkt text,
  created text
);

COPY %(macrostrat_schema)s.cols FROM %(cols_path)s NULL '\N' DELIMITER ',' CSV;

UPDATE %(macrostrat_schema)s.cols SET coordinate = ST_GeomFromText(wkt);
UPDATE %(macrostrat_schema)s.cols SET coordinate = ST_GeomFromText(wkt);
CREATE INDEX ON %(macrostrat_schema)s.cols (id);
CREATE INDEX ON %(macrostrat_schema)s.cols (project_id);
CREATE INDEX ON %(macrostrat_schema)s.cols USING GIST (coordinate);
CREATE INDEX ON %(macrostrat_schema)s.cols (col_group_id);
CREATE INDEX ON %(macrostrat_schema)s.cols (status_code);


CREATE TABLE %(macrostrat_schema)s.col_areas (
  id integer PRIMARY KEY,
  col_id integer,
  col_area geometry,
  wkt text
);

COPY %(macrostrat_schema)s.col_areas FROM %(col_areas_path)s NULL '\N' DELIMITER ',' CSV;

UPDATE %(macrostrat_schema)s.col_areas SET col_area = ST_GeomFromText(wkt);

CREATE INDEX ON %(macrostrat_schema)s.col_areas (col_id);
CREATE INDEX ON %(macrostrat_schema)s.col_areas USING GIST (col_area);


ALTER TABLE %(macrostrat_schema)s.cols ADD COLUMN poly_geom geometry;
UPDATE %(macrostrat_schema)s.cols AS c
SET poly_geom = a.col_area
FROM %(macrostrat_schema)s.col_areas a
WHERE c.id = a.col_id;

UPDATE %(macrostrat_schema)s.cols SET poly_geom = ST_SetSRID(poly_geom, 4326);

CREATE INDEX ON %(macrostrat_schema)s.cols USING GIST (poly_geom);


CREATE TABLE %(macrostrat_schema)s.liths (
  id integer PRIMARY KEY NOT NULL,
  lith character varying(75),
  lith_type character varying(50),
  lith_class character varying(50),
  lith_fill integer,
  comp_coef numeric,
  initial_porosity numeric,
  bulk_density numeric,
  lith_color character varying(12)
);
COPY %(macrostrat_schema)s.liths FROM %(liths_path)s NULL '\N' DELIMITER ',' CSV;

CREATE INDEX ON %(macrostrat_schema)s.liths (id);
CREATE INDEX ON %(macrostrat_schema)s.liths (lith);
CREATE INDEX ON %(macrostrat_schema)s.liths (lith_class);
CREATE INDEX ON %(macrostrat_schema)s.liths (lith_type);


CREATE TABLE %(macrostrat_schema)s.lith_atts (
  id integer PRIMARY KEY NOT NULL,
  lith_att character varying(75),
  att_type character varying(25),
  lith_att_fill integer
);
COPY %(macrostrat_schema)s.lith_atts FROM %(lith_atts_path)s NULL '\N' DELIMITER ',' CSV;

CREATE INDEX ON %(macrostrat_schema)s.lith_atts (id);
CREATE INDEX ON %(macrostrat_schema)s.lith_atts (att_type);
CREATE INDEX ON %(macrostrat_schema)s.lith_atts (lith_att);


CREATE TABLE %(macrostrat_schema)s.timescales_intervals (
  timescale_id integer,
  interval_id integer
);
COPY %(macrostrat_schema)s.timescales_intervals FROM %(timescales_intervals_path)s NULL '\N' DELIMITER ',' CSV;

CREATE INDEX ON %(macrostrat_schema)s.timescales_intervals (timescale_id);
CREATE INDEX ON %(macrostrat_schema)s.timescales_intervals (interval_id);

""", params)
pg_conn.commit()





print "(3 of 8)   Vacuuming macrostrat"
pg_conn.set_isolation_level(0)
pg_cur.execute("VACUUM ANALYZE %(macrostrat_schema)s.strat_names;", {"macrostrat_schema": AsIs(credentials.pg_macrostrat_schema)})
pg_cur.execute("VACUUM ANALYZE %(macrostrat_schema)s.unit_strat_names;", {"macrostrat_schema": AsIs(credentials.pg_macrostrat_schema)})
pg_cur.execute("VACUUM ANALYZE %(macrostrat_schema)s.units_sections;", {"macrostrat_schema": AsIs(credentials.pg_macrostrat_schema)})
pg_cur.execute("VACUUM ANALYZE %(macrostrat_schema)s.intervals;", {"macrostrat_schema": AsIs(credentials.pg_macrostrat_schema)})
pg_cur.execute("VACUUM ANALYZE %(macrostrat_schema)s.lookup_unit_intervals;", {"macrostrat_schema": AsIs(credentials.pg_macrostrat_schema)})
pg_cur.execute("VACUUM ANALYZE %(macrostrat_schema)s.units;", {"macrostrat_schema": AsIs(credentials.pg_macrostrat_schema)})
pg_cur.execute("VACUUM ANALYZE %(macrostrat_schema)s.lookup_strat_names;", {"macrostrat_schema": AsIs(credentials.pg_macrostrat_schema)})
pg_cur.execute("VACUUM ANALYZE %(macrostrat_schema)s.cols;", {"macrostrat_schema": AsIs(credentials.pg_macrostrat_schema)})
pg_cur.execute("VACUUM ANALYZE %(macrostrat_schema)s.col_areas;", {"macrostrat_schema": AsIs(credentials.pg_macrostrat_schema)})
pg_cur.execute("VACUUM ANALYZE %(macrostrat_schema)s.liths;", {"macrostrat_schema": AsIs(credentials.pg_macrostrat_schema)})
pg_cur.execute("VACUUM ANALYZE %(macrostrat_schema)s.lith_atts;", {"macrostrat_schema": AsIs(credentials.pg_macrostrat_schema)})
pg_conn.commit()

subprocess.call("rm *.csv", shell=True)





print "(4 of 8)   Rebuilding gmna.lookup_units"
pg_cur.execute("""
  DROP TABLE IF EXISTS gmna.interval_normalize;

  CREATE TABLE gmna.interval_normalize AS
    WITH gmna_age AS (select distinct min_age AS gmna_interval from gmna.geologic_units where lower(min_age) IN (SELECT distinct lower(interval_name) from macrostrat.intervals) order by min_age asc),
         macro_age AS (select distinct min_age AS macro_interval from gmna.geologic_units where lower(min_age) IN (SELECT distinct lower(interval_name) from macrostrat.intervals) order by min_age asc)

    SELECT * FROM gmna_age
    JOIN macro_age on gmna_age.gmna_interval = macro_age.macro_interval;

  COPY gmna.interval_normalize FROM '/Users/john/code/macrostrat/map_processing/gmna/age_mapping.csv' DELIMITER ',' CSV;

  DROP TABLE IF EXISTS gmna.lookup_units;

  CREATE TABLE gmna.lookup_units AS 
    SELECT 
      gid, 
      unit_abbre, 
      rocktype, 
      lithology, 
      lith, 
      lith_type, 
      lith_class, 
      lith_color, 
      min.age_top AS min_age, 
      min.interval_name AS min_interval, 
      max.age_bottom AS max_age, 
      max.interval_name AS max_interval, 
      (SELECT interval_name from macrostrat.intervals where age_bottom >= max.age_bottom AND age_top <= min.age_top ORDER BY rank DESC LIMIT 1 ) AS containing_interval,
      (SELECT interval_color from macrostrat.intervals where age_bottom >= max.age_bottom AND age_top <= min.age_top ORDER BY rank DESC LIMIT 1 ) AS interval_color,
      geom
    FROM gmna.geologic_units gu
    JOIN gmna.interval_normalize gin_min ON gu.min_age = gin_min.gmna_interval
    JOIN gmna.interval_normalize gin_max ON gu.max_age = gin_max.gmna_interval
    JOIN %(macrostrat_schema)s.intervals min ON gin_min.macro_interval = min.interval_name
    JOIN %(macrostrat_schema)s.intervals max ON gin_max.macro_interval = max.interval_name
    JOIN %(macrostrat_schema)s.liths l ON gu.macro_lith_id = l.id;

  CREATE INDEX ON gmna.lookup_units (gid);
  CREATE INDEX ON gmna.lookup_units (lith_type);
  CREATE INDEX ON gmna.lookup_units (lith_class);
  CREATE INDEX ON gmna.lookup_units (min_age);
  CREATE INDEX ON gmna.lookup_units (max_age);
  CREATE INDEX ON gmna.lookup_units USING GIST (geom);

""", {"macrostrat_schema": AsIs(credentials.pg_macrostrat_schema)})
pg_conn.commit()





print "(5 of 8)   Rebuilding gmus.best_geounits_macrounits"
pg_cur.execute(""" 
  DROP TABLE IF EXISTS gmus.best_geounits_macrounits;
  CREATE TABLE gmus.best_geounits_macrounits AS 
  WITH a AS (
     SELECT DISTINCT ON (geounits_macrounits.geologic_unit_gid) geounits_macrounits.geologic_unit_gid, array_agg(geounits_macrounits.unit_id) AS best_units
      FROM gmus.geounits_macrounits
      WHERE strat_name_id NOT IN (7186, 7213, 7030, 2817)
      GROUP BY geounits_macrounits.geologic_unit_gid, type
      HAVING type = min(type)
      ORDER BY geounits_macrounits.geologic_unit_gid asc
  ),
  result AS (
    SELECT geologic_unit_gid, best_units, (
      SELECT min(t_age) as t_age
      FROM macrostrat.lookup_unit_intervals
      WHERE unit_id = ANY(best_units)
    ) t_age,
    (
      SELECT max(b_age) as b_age
      FROM macrostrat.lookup_unit_intervals
      WHERE unit_id = ANY(best_units)
    ) b_age
    FROM a
  )
  SELECT geologic_unit_gid, best_units, t_age, b_age, (
    SELECT id
    FROM macrostrat.intervals
    LEFT JOIN macrostrat.timescales_intervals ON intervals.id = timescales_intervals.interval_id
    WHERE age_bottom >= b_age AND age_top <= t_age AND (timescale_id != 6 or timescale_id is null)
    ORDER BY rank DESC
    LIMIT 1
  ) macro_interval_id
  FROM result;

  CREATE INDEX ON gmus.best_geounits_macrounits (geologic_unit_gid);
  CREATE INDEX ON gmus.best_geounits_macrounits (best_units);
  CREATE INDEX ON gmus.best_geounits_macrounits (t_age);
  CREATE INDEX ON gmus.best_geounits_macrounits (b_age);
  CREATE INDEX ON gmus.best_geounits_macrounits (macro_interval_id);
""")
pg_conn.commit()





print "(6 of 8)   Rebuilding gmus.lookup_units"
pg_cur.execute("""
    DROP TABLE IF EXISTS gmus.lookup_units;

    CREATE TABLE gmus.lookup_units AS 
    SELECT 
      gid, 
      gu.state, 
      gu.unit_link, 
      source, 
      gu.unit_age, 
      gu.rocktype1, 
      gu.rocktype2, 
      new_unit_name AS unit_name, 
      new_unitdesc AS unitdesc, 
      new_strat_unit AS strat_unit, 
      new_unit_com AS unit_com, 
      u.rocktype1 AS u_rocktype1, 
      u.rocktype2 AS u_rocktype2, 
      u.rocktype3 AS u_rocktype3, 
      i.interval_color, 
      i.interval_name, 
      i.age_bottom, 
      i.age_top, 
      a.macro_containing_interval_id AS macro_interval_id, 
      i2.interval_name AS macro_interval_name,
      bgm.b_age AS macro_b_age, 
      bgm.t_age AS macro_t_age, 
      i2.interval_color AS macro_color, 
      geom, 
      to_tsvector('macro', concat(unit_name, ' ', strat_unit, ' ', unitdesc, ' ', unit_com)) AS text_search

    FROM gmus.geologic_units gu
    LEFT JOIN gmus.units u ON gu.unit_link = u.unit_link
    LEFT JOIN gmus.ages a ON gu.unit_link = a.unit_link
    LEFT JOIN %(macrostrat_schema)s.intervals i ON a.macro_containing_interval_id = i.id
    LEFT JOIN gmus.best_geounits_macrounits bgm ON gu.gid = bgm.geologic_unit_gid
    LEFT JOIN %(macrostrat_schema)s.intervals i2 ON bgm.macro_interval_id = i2.id;

    UPDATE gmus.lookup_units SET macro_color = interval_color WHERE macro_color IS null;

    CREATE INDEX ON gmus.lookup_units (gid);
    CREATE INDEX ON gmus.lookup_units (state);
    CREATE INDEX ON gmus.lookup_units (unit_link);
    CREATE INDEX ON gmus.lookup_units (macro_interval_id);
    CREATE INDEX ON gmus.lookup_units (unit_name);
    CREATE INDEX ON gmus.lookup_units (age_bottom);
    CREATE INDEX ON gmus.lookup_units (age_top);
    CREATE INDEX ON gmus.lookup_units USING GIST (geom);
    CREATE INDEX ON gmus.lookup_units USING GIN (text_search);

""", {"macrostrat_schema": AsIs(credentials.pg_macrostrat_schema)})
pg_conn.commit()





print "(7 of 8)   Vacuuming lookup tables"
pg_cur.execute("VACUUM ANALYZE gmus.best_geounits_macrounits;")
pg_cur.execute("VACUUM ANALYZE gmna.lookup_units;")
pg_cur.execute("VACUUM ANALYZE gmus.lookup_units;")
pg_conn.commit()





print "(8 of 8)   Matching Macrostrat intervals to GMUS"
pg_cur.execute("""
  SELECT * FROM macrostrat.intervals i 
  LEFT JOIN macrostrat.timescales_intervals ti ON i.id = ti.interval_id 
  WHERE (timescale_id != 6 OR timescale_id IS NULL) 
  ORDER BY interval_name ASC""")
intervals = pg_cur.fetchall()

interval_lookup = {}

for i, interval in enumerate(intervals):
  interval_lookup[interval[3]] = interval

pg_cur.execute("SELECT unit_link, min_age, max_age, min_era, max_era, min_eon, max_eon, min_period, max_period, min_epoch, max_epoch FROM gmus.ages")
units = pg_cur.fetchall()

total = len(units)
for i, unit in enumerate(units):
  # Check for min_age
  if is_valid(unit[1]) :
    min_interval = unit[1]
  # min_epoch
  elif is_valid(unit[9]) :
    min_interval = unit[9]
  # min_period
  elif is_valid(unit[7]) :
    min_interval = unit[7]
  # min era
  elif is_valid(unit[3]) :
    min_interval = unit[3]
  # min eon
  elif is_valid(unit[5]) :
    min_interval = unit[5]


  # Check for max_age
  if is_valid(unit[2]) :
    max_interval = unit[2]
  # max_epoch
  elif is_valid(unit[10]) :
    max_interval = unit[10]
  # max_period
  elif is_valid(unit[8]) :
    max_interval = unit[8]
  # max era
  elif is_valid(unit[4]) :
    max_interval = unit[4]
  # max eon
  elif is_valid(unit[6]) :
    max_interval = unit[6]

  try :
    max_interval
    min_interval
  except NameError:
    print "MIN OR MAX_INTERVAL NOT DEFINED ", unit


  else :
    if min_interval == max_interval :
      if "-" in min_interval:

        parts = fix_parts(min_interval.split("-"))
        if (parts[0] == "Early") or (parts[0] == "Late") or (parts[0] == "Middle") :
          try:
            int_id = interval_lookup[' '.join(parts)][0]
          except:
            print ' '.join(parts)

          update(int_id, int_id, int_id, unit[0])
        else :
          ## Split it on the '-' and treat it like a range
          parts = min_interval.split("-")
          parse_range(parts[0], parts[1], unit[0])

      else:
        try:
          int_id = interval_lookup[replace_precam(min_interval)][0]
        except:
          print "EQUIVALENCY MESS UP ", min_interval

        update(int_id, int_id, int_id, unit[0])

    else :
      parse_range(min_interval, max_interval, unit[0])


print "Done!"

