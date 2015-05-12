import csv
import psycopg2
from psycopg2.extensions import AsIs
import argparse
import sys
import os

sys.path = [os.path.join(os.path.dirname(__file__), os.pardir)] + sys.path
import credentials

parser = argparse.ArgumentParser(
  description="Manually match a GMUS unit_link to a Macrostrat strat_name_id or unit_id",
  epilog="Example usage goes here")

parser.add_argument("-ul", "--unit-link", dest="unit_link",
  default="na", type=str, required=True,
  help="GMUS unit_link (surround with single quotes)")

parser.add_argument("-sn", "--strat_name_id", dest="strat_name_id",
  default="na", type=str, required=False,
  help="Macrostrat strat_name_id")

parser.add_argument("-u", "--unit_id", dest="unit_id",
  default="na", type=str, required=False,
  help="Macrostrat unit_id")

arguments = parser.parse_args()

if (arguments.unit_id == "na" and arguments.strat_name_id == "na") or (arguments.unit_id != "na" and arguments.strat_name_id != "na") :
  print "Unit or strat_name_id parameter required, but not both"
  sys.exit()


conn = psycopg2.connect(dbname=credentials.pg_db, user=credentials.pg_user, host=credentials.pg_host, port=credentials.pg_port)
cur = conn.cursor()


def unit_match() :
  # Check if unit has more than 1 strat_name
  cur.execute("""
    WITH sub AS (SELECT unit_id, count(*) a FROM macrostrat.unit_strat_names 
      WHERE unit_id = %(unit_id)s
      GROUP BY unit_id)
    select * from sub where a > 1 order by a desc LIMIT 1
   """, {"unit_id": arguments.unit_id})
  unit_names = cur.fetchall()

  if len(unit_names) > 0:
    print "Sorry, this unit_id has more than one strat_name"
    sys.exit()


  # Check if this unit_link already has matches
  cur.execute("""
    SELECT distinct unit_link FROM gmus.best_geounits_macrounits where unit_link = %(unit_link)s
   """, { "unit_link": arguments.unit_link })
  exists = cur.fetchall()

  # Not spatially constrainted
  if len(exists) < 1 :
    cur.execute("""
      INSERT INTO gmus.%(pg_geounits_macrounits)s (geologic_unit_gid, unit_id, strat_name_id, unit_link, type) (
        SELECT gid, %(unit_id)s as unit_id, (select strat_name_id FROM macrostrat.unit_strat_names where unit_id = %(unit_id)s) AS strat_name_id, unit_link, 0 as type
        FROM gmus.lookup_units
        WHERE unit_link = %(unit_link)s
      )
    """, {
    "unit_id": arguments.unit_id, 
    "unit_link": arguments.unit_link, 
    "pg_geounits_macrounits": AsIs(credentials.pg_geounits_macrounits) 
    })
    conn.commit()

   

  # spatially constrained
  else :
    cur.execute("""
      INSERT INTO gmus.%(pg_geounits_macrounits)s (geologic_unit_gid, unit_id, strat_name_id, unit_link, type) (
        WITH unit_geom AS (
          SELECT poly_geom 
          FROM macrostrat.units_sections us
          JOIN macrostrat.cols c ON us.col_id = c.id
          WHERE us.unit_id = %(unit_id)s
        )  
        select gid, %(unit_id)s as unit_id, (select strat_name_id FROM macrostrat.unit_strat_names where unit_id = %(unit_id)s) AS strat_name_id, unit_link, 0 as type
        FROM gmus.lookup_units
        WHERE unit_link = %(unit_link)s AND ST_Intersects(geom, (SELECT poly_geom FROM unit_geom))
      )
    """, {
    "unit_id": arguments.unit_id, 
    "unit_link": arguments.unit_link, 
    "pg_geounits_macrounits": AsIs(credentials.pg_geounits_macrounits) 
    })
    conn.commit()


  # Delete from gmus.best_geounits_macrounits
  cur.execute(""" 
    DELETE FROM gmus.best_geounits_macrounits WHERE geologic_unit_gid IN (
      SELECT geologic_unit_gid FROM gmus.geounits_macrounits WHERE unit_link = %(unit_link)s AND unit_id = %(unit_id)s
    )
  """, {
    "unit_id": arguments.unit_id, 
    "unit_link": arguments.unit_link, 
    "pg_geounits_macrounits": AsIs(credentials.pg_geounits_macrounits) 
  })
  conn.commit()

  # Add to gmus.best_geounits_macrounits
  cur.execute(""" 
    INSERT INTO gmus.best_geounits_macrounits (geologic_unit_gid, unit_link, best_units, t_age, b_age, macro_interval_id) (
      WITH result AS (
        SELECT geologic_unit_gid, unit_link, unit_id, (
          SELECT min(t_age) as t_age
          FROM macrostrat.lookup_unit_intervals
          WHERE unit_id = %(unit_id)s
        ) t_age,
        (
          SELECT max(b_age) as b_age
          FROM macrostrat.lookup_unit_intervals
          WHERE unit_id = %(unit_id)s
        ) b_age
        FROM gmus.geounits_macrounits
        WHERE unit_link = %(unit_link)s AND unit_id = %(unit_id)s
      )
      SELECT geologic_unit_gid, unit_link, array[unit_id] AS best_units, t_age, b_age, (
        SELECT id
        FROM macrostrat.intervals
        LEFT JOIN macrostrat.timescales_intervals ON intervals.id = timescales_intervals.interval_id
        WHERE age_bottom >= b_age AND age_top <= t_age AND (timescale_id != 6 or timescale_id is null)
        ORDER BY rank DESC
        LIMIT 1
      ) macro_interval_id
      FROM result
    )
  """, {
    "unit_id": arguments.unit_id, 
    "unit_link": arguments.unit_link, 
    "pg_geounits_macrounits": AsIs(credentials.pg_geounits_macrounits) 
  })
  conn.commit()

  print arguments.unit_link, " - ", arguments.unit_id



def strat_name_match() :
  cur.execute("""
    INSERT INTO gmus.%(pg_geounits_macrounits)s (geologic_unit_gid, unit_id, strat_name_id, unit_link, type) (
      WITH units AS (
         SELECT us.unit_id AS unit_id, lsn.strat_name_id, lsn.fm_name AS strat_name, c.poly_geom
         FROM %(macrostrat_schema)s.units_sections us
         JOIN %(macrostrat_schema)s.unit_strat_names usn ON us.unit_id = usn.unit_id
         JOIN %(macrostrat_schema)s.lookup_strat_names lsn ON usn.strat_name_id = lsn.strat_name_id
         JOIN %(macrostrat_schema)s.cols c ON us.col_id = c.id
         WHERE c.status_code = 'active' AND lsn.strat_name_id = %(strat_name_id)s
      ), 
      distance AS (
        SELECT gu.gid, ST_Distance(gu.geom::geography, u.poly_geom::geography)/1000 AS distance, gu.unit_link, u.unit_id, u.strat_name_id
        FROM gmus.lookup_units gu, units u
        WHERE unit_link = %(unit_link)s
        ORDER BY gid, distance
      ),
      min_dist AS (
        SELECT gid, min(distance) AS distance from distance GROUP BY gid ORDER BY gid
      )
      SELECT distance.gid, distance.unit_id, strat_name_id, unit_link, 0 AS type from distance 
      JOIN min_dist ON distance.gid = min_dist.gid
      WHERE distance.distance = min_dist.distance
    )
    
  """, {
    "strat_name_id": arguments.strat_name_id, 
    "unit_link": arguments.unit_link, 
    "macrostrat_schema": AsIs(credentials.pg_macrostrat_schema),
    "pg_geounits_macrounits": AsIs(credentials.pg_geounits_macrounits) 
  })

  conn.commit()

  # Delete from best_geounits_macrounits
  cur.execute(""" 
    DELETE FROM gmus.best_geounits_macrounits WHERE geologic_unit_gid IN (
      SELECT geologic_unit_gid FROM gmus.geounits_macrounits WHERE unit_link = %(unit_link)s AND type > 0
    )
  """, {
    "strat_name_id": arguments.strat_name_id, 
    "unit_link": arguments.unit_link
  })
  conn.commit()

  # Insert into best_geounits_macrounits
  cur.execute("""
  INSERT INTO gmus.best_geounits_macrounits (geologic_unit_gid, unit_link, best_units, t_age, b_age, macro_interval_id) (
    WITH first as (SELECT geologic_unit_gid, unit_link, unit_id AS unit
      FROM gmus.%(pg_geounits_macrounits)s 
      WHERE unit_link = %(unit_link)s AND strat_name_id = %(strat_name_id)s AND type = 0
    ),
    result AS (
      SELECT geologic_unit_gid, unit_link, unit, (
        SELECT min(t_age) as t_age
        FROM %(macrostrat_schema)s.lookup_unit_intervals
        WHERE unit_id = unit
      ) t_age,
      (
        SELECT max(b_age) as b_age
        FROM %(macrostrat_schema)s.lookup_unit_intervals
        WHERE unit_id = unit
      ) b_age
      FROM first
    )
    SELECT geologic_unit_gid, unit_link, array[unit] AS best_units, t_age, b_age, (
      SELECT id
      FROM %(macrostrat_schema)s.intervals
      LEFT JOIN %(macrostrat_schema)s.timescales_intervals ON intervals.id = timescales_intervals.interval_id
      WHERE age_bottom >= b_age AND age_top <= t_age AND (timescale_id != 6 or timescale_id is null)
      ORDER BY rank DESC
      LIMIT 1
    ) macro_interval_id
    FROM result
   )
  """, {
    "strat_name_id": arguments.strat_name_id, 
    "unit_link": arguments.unit_link, 
    "macrostrat_schema": AsIs(credentials.pg_macrostrat_schema),
    "pg_geounits_macrounits": AsIs(credentials.pg_geounits_macrounits) 
  })
  conn.commit()

  print arguments.unit_link, " - ", arguments.strat_name_id


# Make a unit_link -> strat_name_id match
if arguments.strat_name_id != "na" :
  strat_name_match()
  cur.execute(""" 
    DELETE FROM gmus.alterations WHERE unit_link = %(unit_link)s AND strat_name_id = %(strat_name_id)s AND addition is TRUE;

    INSERT INTO gmus.alterations (unit_link, strat_name_id, addition) VALUES (%(unit_link)s, %(strat_name_id)s, TRUE);
  """, {
    "unit_link": arguments.unit_link,
    "strat_name_id": arguments.strat_name_id
  })
  conn.commit()

elif arguments.unit_id != "na" :
  unit_match()
  cur.execute(""" 
    DELETE FROM gmus.alterations WHERE unit_link = %(unit_link)s AND unit_id = %(unit_id)s AND addition is TRUE;

    INSERT INTO gmus.alterations (unit_link, unit_id, addition) VALUES (%(unit_link)s, %(unit_id)s, TRUE);
  """, {
    "unit_link": arguments.unit_link,
    "unit_id": arguments.unit_id
  })
  conn.commit()

else :
  print "Huh?"







      
