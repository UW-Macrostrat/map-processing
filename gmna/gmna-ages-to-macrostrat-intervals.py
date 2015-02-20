import re
import psycopg2
import sys, os

import credentials

'''
alter table gmna.geologic_units add column macro_containing_interval_id integer;
alter table gmna.geologic_units add column macro_min_interval_id integer;
alter table gmna.geologic_units add column macro_max_interval_id integer;
'''

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

def parse_range(min_interval, max_interval, gid):
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
    elif max_interval.split(" ")[0] == "Upper" :
      max_interval = "Late " + max_interval.split(" ")[1]
      age_bottom = interval_lookup[max_interval][1]
    elif max_interval.split(" ")[0] == "Early" :
      max_interval = "Lower " + max_interval.split(" ")[1]
      age_bottom = interval_lookup[max_interval][1]
    elif max_interval.split(" ")[0] == "Lower" :
      max_interval = "Early " + max_interval.split(" ")[1]
      age_bottom = interval_lookup[max_interval][1]
    elif max_interval == "preCambrian" :
      age_bottom = interval_lookup["Precambrian"][1]
    else :
      print "FUCK UP MAX", max_interval

  try :
    age_top = interval_lookup[min_interval][2]
  except :
    if min_interval.split(" ")[0] == "Late" :
      min_interval = "Upper " + min_interval.split(" ")[1]
      age_top = interval_lookup[min_interval][2]

    elif min_interval.split(" ")[0] == "Upper" :
      min_interval = "Late " + min_interval.split(" ")[1]
      age_top = interval_lookup[min_interval][2]

    elif min_interval.split(" ")[0] == "Early" :
      min_interval = "Lower " + min_interval.split(" ")[1]
      age_top = interval_lookup[min_interval][2]

    elif min_interval.split(" ")[0] == "Lower" :
      min_interval = "Early " + min_interval.split(" ")[1]
      age_top = interval_lookup[min_interval][2]
    elif min_interval == "preCambrian" :
      age_top = interval_lookup["Precambrian"][2]
    else :
      print "FUCK UP MIN", min_interval

  min_interval_id = interval_lookup[replace_precam(min_interval)][0]
  max_interval_id = interval_lookup[replace_precam(max_interval)][0]

  cur.execute("SELECT id, interval_name, interval_color FROM macrostrat.intervals WHERE age_bottom >= %s AND age_top <= %s ORDER BY rank DESC LIMIT 1", [age_bottom, age_top])
  match = cur.fetchall()
  if len(match) > 0:
    update(match[0][0], min_interval_id, max_interval_id, gid)
  else :
    print str(unit_link) + "\r"

def update(containing_interval_id, min_interval_id, max_interval_id, gid) :
  #print "UPDATING ", unit_link, " - ", interval_id
  cur.execute("UPDATE gmna.geologic_units SET macro_containing_interval_id = %s, macro_min_interval_id = %s, macro_max_interval_id = %s WHERE gid = %s", [containing_interval_id, min_interval_id, max_interval_id, gid])
  conn.commit()

# Connect to the database
try:
  conn = psycopg2.connect(dbname=credentials.pg_db, user=credentials.pg_user, host=credentials.pg_host, port=credentials.pg_port)
except:
  print "Could not connect to database: ", sys.exc_info()[1]
  sys.exit()

cur = conn.cursor()

cur.execute("select * from macrostrat.intervals order by interval_name asc")
intervals = cur.fetchall()

interval_lookup = {}

for i, interval in enumerate(intervals):
  interval_lookup[interval[3]] = interval


cur.execute("select gid, min_age, max_age FROM gmna.geologic_units")
units = cur.fetchall()

total = len(units)
for i, unit in enumerate(units):
  sys.stdout.write("\r" + str(i) + " of " + str(total))
  sys.stdout.flush()

  min_interval = unit[1]
  max_interval = unit[2]

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

          update(int_id, int_id, int_id, unit[0])
        except:

          parse_range(min_interval, min_interval, unit[0])
          print "EQUIVALENCY FUCK UP ", min_interval

        

    else :
      parse_range(min_interval, max_interval, unit[0])
