import MySQLdb
import MySQLdb.cursors
import os
import psycopg2
import sys
import subprocess
from credentials import *

# Connect to Postgres
pg_conn = psycopg2.connect(dbname=pg_db, user=pg_user, host=pg_host, port=pg_port)
pg_cur = pg_conn.cursor()

# Connect to MySQL
my_conn = MySQLdb.connect(host=mysql_host, user=mysql_user, passwd=mysql_passwd, db=mysql_db, unix_socket=mysql_socket, cursorclass=MySQLdb.cursors.DictCursor)
my_cur = my_conn.cursor()

subprocess.call("rm *.csv", shell=True)

directory = os.getcwd()

params = {
  "unit_strat_names_path": directory + "/unit_strat_names.csv",
  "strat_names_path": directory + "/strat_names.csv",
  "units_sections_path": directory + "/units_sections.csv",
  "intervals_path": directory + "/intervals.csv",
  "lookup_unit_intervals_path": directory + "/lookup_unit_intervals.csv",
  "units_path": directory + "/units.csv",
  "lookup_strat_names_path": directory + "/lookup_strat_names.csv"
}

print "(1 of 4)   Dumping from MySQL"

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

""", params)


subprocess.call("chmod 777 *.csv", shell=True)

print "(2 of 4)   Importing into Postgres"

pg_cur.execute(""" 

DROP SCHEMA IF EXISTS new_macrostrat cascade;

CREATE SCHEMA new_macrostrat;

CREATE TABLE new_macrostrat.unit_strat_names (
  id serial PRIMARY KEY NOT NULL,
  unit_id integer NOT NULL,
  strat_name_id integer NOT NULL
);

COPY new_macrostrat.unit_strat_names FROM %(unit_strat_names_path)s DELIMITER ',' CSV;

CREATE INDEX ON new_macrostrat.unit_strat_names (id);
CREATE INDEX ON new_macrostrat.unit_strat_names (unit_id);
CREATE INDEX ON new_macrostrat.unit_strat_names (strat_name_id);


CREATE TABLE new_macrostrat.strat_names (
  id serial PRIMARY KEY NOT NULL,
  strat_name character varying(100) NOT NULL,
  rank character varying(50),
  ref_id  integer NOT NULL
);


COPY new_macrostrat.strat_names FROM %(strat_names_path)s DELIMITER ',' CSV;

CREATE TABLE new_macrostrat.units_sections (
  id serial PRIMARY KEY NOT NULL,
  unit_id integer NOT NULL,
  section_id integer NOT NULL,
  col_id integer NOT NULL
);

COPY new_macrostrat.units_sections FROM %(units_sections_path)s DELIMITER ',' CSV;


CREATE INDEX ON new_macrostrat.units_sections (id);
CREATE INDEX ON new_macrostrat.units_sections (unit_id);
CREATE INDEX ON new_macrostrat.units_sections (section_id);
CREATE INDEX ON new_macrostrat.units_sections (col_id);

CREATE TABLE new_macrostrat.intervals (
  id serial PRIMARY KEY NOT NULL,
  age_bottom numeric NOT NULL,
  age_top numeric NOT NULL,
  interval_name character varying(200),
  interval_abbrev character varying(50),
  interval_type character varying(50),
  interval_color character varying(20)
);

COPY new_macrostrat.intervals FROM %(intervals_path)s DELIMITER ',' CSV;


CREATE INDEX ON new_macrostrat.intervals (id);
CREATE INDEX ON new_macrostrat.intervals (age_top);
CREATE INDEX ON new_macrostrat.intervals (age_bottom);
CREATE INDEX ON new_macrostrat.intervals (interval_type);
CREATE INDEX ON new_macrostrat.intervals (interval_name);


CREATE TABLE new_macrostrat.lookup_unit_intervals (
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

COPY new_macrostrat.lookup_unit_intervals FROM %(lookup_unit_intervals_path)s DELIMITER ',' CSV;

CREATE INDEX ON new_macrostrat.lookup_unit_intervals (unit_id);


CREATE TABLE new_macrostrat.units (
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

COPY new_macrostrat.units FROM %(units_path)s DELIMITER ',' CSV;


CREATE INDEX ON new_macrostrat.units (id);
CREATE INDEX ON new_macrostrat.units (section_id);
CREATE INDEX ON new_macrostrat.units (col_id);
CREATE INDEX ON new_macrostrat.units (strat_name);
CREATE INDEX ON new_macrostrat.units (color);


CREATE TABLE new_macrostrat.lookup_strat_names (
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

COPY new_macrostrat.lookup_strat_names FROM %(lookup_strat_names_path)s NULL '\N' DELIMITER ',' CSV;


CREATE INDEX ON new_macrostrat.lookup_strat_names (strat_name_id);
CREATE INDEX ON new_macrostrat.lookup_strat_names (bed_id);
CREATE INDEX ON new_macrostrat.lookup_strat_names (mbr_id);
CREATE INDEX ON new_macrostrat.lookup_strat_names (fm_id);
CREATE INDEX ON new_macrostrat.lookup_strat_names (gp_id);
CREATE INDEX ON new_macrostrat.lookup_strat_names (sgp_id);
CREATE INDEX ON new_macrostrat.lookup_strat_names (strat_name);

""", params)
pg_conn.commit()


print "(3 of 4)   Vacuuming Postgres"
pg_conn.set_isolation_level(0)
pg_cur.execute("VACUUM ANALYZE new_macrostrat.strat_names;")
pg_cur.execute("VACUUM ANALYZE new_macrostrat.unit_strat_names;")
pg_cur.execute("VACUUM ANALYZE new_macrostrat.units_sections;")
pg_cur.execute("VACUUM ANALYZE new_macrostrat.intervals;")
pg_cur.execute("VACUUM ANALYZE new_macrostrat.lookup_unit_intervals;")
pg_cur.execute("VACUUM ANALYZE new_macrostrat.units;")
pg_cur.execute("VACUUM ANALYZE new_macrostrat.lookup_strat_names;")
pg_conn.commit()

subprocess.call("rm *.csv", shell=True)

print "(4 of 4)   Done"

