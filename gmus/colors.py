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

cur.execute("select distinct macro_color FROM gmus.lookup_units WHERE macro_color IS NOT NULL AND macro_color != ''")
colors = cur.fetchall()

css = ""
macro_css = """
Map {
  background-color: transparent;
}

#gmus {
  polygon-opacity:1;
  polygon-fill: #000;
  line-color: #aaa;
}
#gmus[zoom<=9] {
  line-width: 0.0;
}
#gmus[zoom>9] {
  line-width: 0.2;
}
#gmus[macro_color="null"] {
   polygon-fill: #777777;
}
#gmus[macro_color=null] {
   polygon-fill: #777777;
}
#gmus[macro_color=""] {
   polygon-fill: #777777;
}

"""

for color in colors :
 # css += '#gmus[interval_color="' + color[0] + '"] {\n   polygon-fill: ' + color[0] + ';\n}\n'
  macro_css += '#gmus[macro_color="' + color[0] + '"] {\n   polygon-fill: ' + color[0] + ';\n}\n'

#with open("gmus_styles.txt", "w") as output:
#    output.write(css)

with open("gmus_macro_styles.txt", "w") as output:
    output.write(macro_css)
