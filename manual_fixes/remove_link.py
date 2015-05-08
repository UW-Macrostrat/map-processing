import csv
import psycopg2
from psycopg2.extensions import AsIs
import argparse
import sys
import os

sys.path = [os.path.join(os.path.dirname(__file__), os.pardir)] + sys.path
import credentials

parser = argparse.ArgumentParser(
  description="Manually remove matches from a GMUS unit_link or GID to a Macrostrat strat_name_id or unit_id",
  epilog="Example usage goes here")

parser.add_argument("-ul", "--unit-link", dest="unit_link",
  default="na", type=str, required=False,
  help="GMUS unit_link (surround with single quotes)")

parser.add_argument("-g", "--gid", dest="gid",
  default="na", type=str, required=False,
  help="GMUS unit_link (surround with single quotes)")

parser.add_argument("-sn", "--strat_name_id", dest="strat_name_id",
  default="na", type=str, required=False,
  help="Macrostrat strat_name_id")

parser.add_argument("-u", "--unit_id", dest="unit_id",
  default="na", type=str, required=False,
  help="Macrostrat unit_id")

arguments = parser.parse_args()

# Make sure we're good to go
if (arguments.unit_link == "na" and arguments.strat_name_id == "na") or (arguments.unit_link == "na" and arguments.unit_id == "na") or (arguments.gid == "na" and arguments.strat_name_id == "na") or (arguments.gid == "gid" and arguments.unit_id == "na") :
  print "Some combination of GMUS/Macrostrat data required. For example, unit_link -> strat_name_id, or gid -> unit_id, etc"
  sys.exit()

conn = psycopg2.connect(dbname=credentials.pg_db, user=credentials.pg_user, host=credentials.pg_host, port=credentials.pg_port)
cur = conn.cursor()

'''
# General flow 

- Delete from gmus.geounits_macrounits
- Update gmus.best_geounits_macrounits

'''


def remove_unit_link_strat_name_id() :
  cur.execute(""" 
    DELETE FROM gmus.geounits_macrounits WHERE unit_link = %(unit_link)s AND strat_name_id = %(strat_name_id)s
  """, {
    "unit_link": arguments.unit_link,
    "strat_name_id": arguments.strat_name_id
  })
  conn.commit()

  cur.execute("""
    DELETE FROM gmus.alterations WHERE unit_link = %(unit_link)s AND strat_name_id = %(strat_name_id)s AND removal is TRUE;

    INSERT INTO gmus.alterations (unit_link, strat_name_id, removal) VALUES (%(unit_link)s, %(strat_name_id)s, TRUE);
    """,{
      "unit_link": arguments.unit_link,
      "strat_name_id": arguments.strat_name_id
    })
  conn.commit()

  update_bests_unit_link()


def remove_unit_link_unit_id() :
  cur.execute(""" 
    DELETE FROM gmus.geounits_macrounits WHERE unit_link = %(unit_link)s AND unit_id = %(unit_id)s
  """, {
    "unit_link": arguments.unit_link,
    "unit_id": arguments.unit_id
  })
  conn.commit()

  cur.execute("""
    DELETE FROM gmus.alterations WHERE unit_link = %(unit_link)s AND unit_id = %(unit_id)s AND removal is TRUE;

    INSERT INTO gmus.alterations (unit_link, unit_id, removal) VALUES (%(unit_link)s, %(unit_id)s, TRUE);
    """,{
      "unit_link": arguments.unit_link,
      "unit_id": arguments.unit_id
    })
  conn.commit()

  update_bests_unit_link()


def remove_gid_strat_name_id() :
  cur.execute(""" 
    DELETE FROM gmus.geounits_macrounits WHERE geologic_unit_gid = %(gid)s AND strat_name_id = %(strat_name_id)s
  """, {
    "gid": arguments.gid,
    "strat_name_id": arguments.strat_name_id
  })
  conn.commit()

  cur.execute("""
    DELETE FROM gmus.alterations WHERE gid = %(gid)s AND strat_name_id = %(strat_name_id)s AND removal is TRUE;

    INSERT INTO gmus.alterations (geologic_unit_gid, strat_name_id, removal) VALUES (%(gid)s, %(strat_name_id)s, TRUE);
    """,{
      "gid": arguments.gid,
      "strat_name_id": arguments.strat_name_id
    })
  conn.commit()

  update_bests_gid()



def remove_gid_unit_id() :
  cur.execute(""" 
    DELETE FROM gmus.geounits_macrounits WHERE geologic_unit_gid = %(gid)s AND unit_id = %(unit_id)s
  """, {
    "gid": arguments.gid,
    "unit_id": arguments.unit_id
  })
  conn.commit()

  cur.execute("""
    DELETE FROM gmus.alterations WHERE gid = %(gid)s AND unit_id = %(unit_id)s AND removal is TRUE;

    INSERT INTO gmus.alterations (geologic_unit_gid, unit_id, removal) VALUES (%(gid)s, %(unit_id)s, TRUE);
    """,{
      "gid": arguments.gid,
      "unit_id": arguments.strat_name_id
    })
  conn.commit()

  update_bests_gid()


def update_bests_unit_link() :
  # Remove current bests
  cur.execute("""
    DELETE FROM gmus.best_geounits_macrounits WHERE unit_link = %(unit_link)s
  """,{
    "unit_link": arguments.unit_link
  })
  conn.commit()

  # Add new ones
  cur.execute(""" 
    INSERT INTO gmus.best_geounits_macrounits (geologic_unit_gid, unit_link, best_units, t_age, b_age, macro_interval_id) (
      WITH a AS (
         SELECT DISTINCT ON (geounits_macrounits.geologic_unit_gid) geounits_macrounits.geologic_unit_gid, unit_link, array_agg(DISTINCT geounits_macrounits.unit_id) AS best_units
          FROM gmus.geounits_macrounits
          WHERE strat_name_id NOT IN (7186, 7213, 7030, 2817) AND unit_link = %(unit_link)s
          GROUP BY geounits_macrounits.geologic_unit_gid, unit_link, type
          HAVING type = min(type)
          ORDER BY geounits_macrounits.geologic_unit_gid asc
      ),
      result AS (
        SELECT geologic_unit_gid, unit_link, best_units, (
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
      SELECT geologic_unit_gid, unit_link, best_units, t_age, b_age, (
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
    "unit_link": arguments.unit_link
  })
  conn.commit()

  print "Cleaned up ", arguments.unit_link



def update_bests_gid() :
  # Remove current bests
  cur.execute("""
    DELETE FROM gmus.best_geounits_macrounits WHERE geologic_unit_gid = %(gid)s
  """,{
    "gid": arguments.gid
  })
  conn.commit()

  # Add new ones
  cur.execute(""" 
    INSERT INTO gmus.best_geounits_macrounits (geologic_unit_gid, unit_link, best_units, t_age, b_age, macro_interval_id) (
      WITH a AS (
         SELECT DISTINCT ON (geounits_macrounits.geologic_unit_gid) geounits_macrounits.geologic_unit_gid, unit_link, array_agg(DISTINCT geounits_macrounits.unit_id) AS best_units
          FROM gmus.geounits_macrounits
          WHERE strat_name_id NOT IN (7186, 7213, 7030, 2817) AND geologic_unit_gid = %(gid)s
          GROUP BY geounits_macrounits.geologic_unit_gid, unit_link, type
          HAVING type = min(type)
          ORDER BY geounits_macrounits.geologic_unit_gid asc
      ),
      result AS (
        SELECT geologic_unit_gid, unit_link, best_units, (
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
      SELECT geologic_unit_gid, unit_link, best_units, t_age, b_age, (
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
    "gid": arguments.gid
  })
  conn.commit()

  print "Cleaned up ", arguments.gid



# The process
if arguments.unit_link != "na" and arguments.strat_name_id != "na" :
  remove_unit_link_strat_name_id()

elif arguments.unit_link != "na" and arguments.unit_id != "na" :
  remove_unit_link_unit_id()

elif arguments.gid != "na" and arguments.strat_name_id != "na" :
  remove_gid_strat_name_id()

elif arguments.gid != "na" and arguments.unit_id != "na" :
  remove_gid_unit_id()

else :
  print "Huh?"


