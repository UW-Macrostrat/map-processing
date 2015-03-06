import psycopg2
import json
from collections import OrderedDict
import sys, os
sys.path = [os.path.join(os.path.dirname(__file__), os.pardir)] + sys.path
import credentials


# Connect to the database
try:
  conn = psycopg2.connect(dbname=credentials.pg_db, user=credentials.pg_user, host=credentials.pg_host, port=credentials.pg_port)
except:
  print "Could not connect to database: ", sys.exc_info()[1]
  sys.exit()

cur = conn.cursor()

cur.execute("select distinct interval_color, lith_color FROM gmna.lookup_units")
colors = cur.fetchall()

css = ""
vector_css = [
  OrderedDict([
    ("id", "gmna-layer"),
    ("type", "fill"),
    ("source", "gmna"),
    ("source-layer", "gmna"),
    ("paint", OrderedDict([("fill-outline-color", "#aaa"), ("fill-color", "#fff"), ("fill-opacity", "0.5")]))
  ])
]

for color in colors :
  css += '#gmna[interval_color="' + color[0] + '"] {\n   polygon-fill: ' + color[0] + ';\n}\n'
  vector_css.append(OrderedDict([
      ("id", color[0]),
      ("type", "fill"),
      ("source", "gmna"),
      ("source-layer", "gmna"),
      ("filter", ["==", "interval_color", color[0]]),
      ("paint", OrderedDict([("fill-color", color[0]), ("fill-opacity", "0.7")])),
      ("paint.lith", OrderedDict([("fill-color", color[1]), ("fill-opacity", "0.7")]))
    ])
  )

with open("styles.txt", "w") as output:
    output.write(json.dumps(vector_css, indent=2))

