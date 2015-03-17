import csv
import psycopg2
from psycopg2.extensions import AsIs
import sys
import os

sys.path = [os.path.join(os.path.dirname(__file__), os.pardir)] + sys.path
import credentials

conn = psycopg2.connect(dbname=credentials.pg_db, user=credentials.pg_user, host=credentials.pg_host, port=credentials.pg_port)
cur = conn.cursor()

with open("matches.txt", "rb") as input_file :
  reader = csv.reader(input_file, delimiter=',', quotechar='"')
  for row in reader :
    # filter out empty lines and the headers
    if len(row) > 0 and row[0] != "unit_link" :

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
          SELECT distance.gid, distance.unit_id, strat_name_id, unit_link, 99 AS type from distance 
          JOIN min_dist ON distance.gid = min_dist.gid
          WHERE distance.distance = min_dist.distance
        )
        
      """, {
        "strat_name_id": row[1], 
        "unit_link": row[0], 
        "macrostrat_schema": AsIs(credentials.pg_macrostrat_schema),
        "pg_geounits_macrounits": AsIs(credentials.pg_geounits_macrounits) 
      })
      
      conn.commit()
      print row[0], " - ", row[1]

      