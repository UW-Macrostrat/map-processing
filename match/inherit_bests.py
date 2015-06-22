import csv
import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.extensions import AsIs
import sys
import os

sys.path = [os.path.join(os.path.dirname(__file__), os.pardir)] + sys.path
import credentials

conn = psycopg2.connect(dbname=credentials.pg_db, user=credentials.pg_user, host=credentials.pg_host, port=credentials.pg_port)
cur = conn.cursor(cursor_factory=DictCursor)

cur.execute("SELECT DISTINCT unit_link FROM gmus.geounits_macrounits")
unit_links = cur.fetchall()

for idx, unit_link in enumerate(unit_links) :
  print idx, " of ", len(unit_links)


  cur.execute("""
    SELECT * FROM gmus.geounits_macrounits WHERE unit_link = %(unit_link)s
  """, {"unit_link": unit_link["unit_link"]})

  matches = cur.fetchall()
  types = [match["type"] for match in matches]
  best_match_type = min(types)

  if best_match_type < 9 :
    best_matches = []

    # Find the best tuples
    for match in matches :
      if match["type"] == best_match_type :
        best_matches.append({
          "unit_id" : match["unit_id"],
          "strat_name_id" : match["strat_name_id"],
          "unit_link": match["unit_link"],
          "type": match["type"]
        })

    # Remove dupes
    best_matches = [dict(tupleized) for tupleized in set(tuple(match.items()) for match in best_matches)]

    # Find geologic_unit_gids that need to inherit the best matches
    polys = list(set([match["geologic_unit_gid"] for match in matches]))
    for poly in polys :
      # get all matches with this poly
      poly_matches = [match for match in matches if match["geologic_unit_gid"] == poly]
      # get the best match for this poly
      best_poly_match = min([match["type"] for match in poly_matches])

      # if this, we need to inherit
      if best_poly_match > 8 :
        for best_match in best_matches :
          cur.execute("""
            INSERT INTO gmus.geounits_macrounits (geologic_unit_gid, unit_id, strat_name_id, unit_link, type) 
              VALUES (%(geologic_unit_gid)s, %(unit_id)s, %(strat_name_id)s, %(unit_link)s, %(type)s)
          """, {
            "geologic_unit_gid" : poly,
            "unit_id" : best_match["unit_id"],
            "strat_name_id" : best_match["strat_name_id"],
            "unit_link" : best_match["unit_link"],
            "type" : best_match["type"]
          })
          conn.commit()

  else :
    continue


      