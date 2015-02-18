import os
import psycopg2
import sys
import urllib
import json

sys.path = [os.path.join(os.path.dirname(__file__), os.pardir)] + sys.path
import credentials

conn = psycopg2.connect(dbname='geomacro', user=credentials.pg_user, host=credentials.pg_host, port=credentials.pg_host)
cur = conn.cursor()

cur.execute("SELECT DISTINCT unit_link FROM gmus.units WHERE new_unit_name IS NULL")
unit_links = cur.fetchall()

def clean(field) :
  field = field.replace('\"', '')
  field = field.strip()
  field = " ".join(field.split())

  return field

for unit_link in unit_links :
  print unit_link[0]
  req = urllib.urlopen('http://mrdata.usgs.gov/geology/state/sgmc-unit.php?unit=' + unit_link[0] + '&f=JSON')
  
  if req.getcode() != 200 :
    print "------ Bad request for ", unit_link[0], " ------"
    continue

  data = json.loads(req.read())
  
  sql = []
  params = {
    "unit_link": unit_link[0]
  }

  if "unit_name" in data :
    sql.append("new_unit_name = %(unit_name)s")
    params["unit_name"] = clean(data["unit_name"])

  if "unitdesc" in data :
    sql.append("new_unitdesc = %(unitdesc)s")
    params["unitdesc"] = clean(data["unitdesc"])

  if "unit_com" in data :
    sql.append("new_unit_com = %(unit_com)s")
    params["unit_com"] = clean(data["unit_com"])

  if "strat_unit" in data :
    sql.append("new_strat_unit = %(strat_unit)s")
    params["strat_unit"] = clean(data["strat_unit"])


  cur.execute("UPDATE gmus.units SET " + ", ".join(sql) + "  WHERE unit_link = %(unit_link)s", params)
  conn.commit()

  
