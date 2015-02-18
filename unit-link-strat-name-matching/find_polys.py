import csv
import psycopg2
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

        INSERT INTO gmus.geounits_macrounits_redo (geologic_unit_gid, unit_id, strat_name_id, unit_link, type) (
          WITH units AS (
             SELECT us.id AS unit_id, lsn.fm_id AS strat_name_id, lsn.fm_name AS strat_name, c.the_geom_voronoi
             FROM new_macrostrat.units_sections us
             JOIN new_macrostrat.unit_strat_names usn ON us.unit_id = usn.unit_id
             JOIN new_macrostrat.lookup_strat_names lsn ON usn.strat_name_id = lsn.strat_name_id
             JOIN macrostrat.cols c ON us.col_id = c.id
             WHERE c.status_code = 'active' AND lsn.strat_name_id = %(strat_name_id)s
          ), 
          distance AS (
            SELECT gu.gid, ST_Distance(gu.the_geom::geography, u.the_geom_voronoi::geography)/1000 AS distance, gu.unit_link, u.unit_id, u.strat_name_id
            FROM gmus.geologic_units_with_intervals gu, units u
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
        
      """, {"strat_name_id": row[1], "unit_link": row[0]})
      
      conn.commit()
      print row[0], " - ", row[1]