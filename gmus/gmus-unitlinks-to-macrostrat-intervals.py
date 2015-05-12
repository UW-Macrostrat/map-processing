import re
import psycopg2
import sys, os

sys.path = [os.path.join(os.path.dirname(__file__), os.pardir)] + sys.path
import credentials

def is_valid(interval) :
  if interval is not None :
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
  if "preCambrian" in interval :
    return interval.replace("preCambrian", "Precambrian")
  else :
    return interval

def parse_range(min_interval, max_interval, unit_link):
  min_interval = replace_precam(min_interval)
  max_interval = replace_precam(max_interval)

  if "-" in min_interval:
    parts = fix_parts(min_interval.split("-"))

    if (parts[0] == "Early") or (parts[0] == "Late") or (parts[0] == "Middle") :
      min_interval = ' '.join(parts)
    elif (parts[1] == "Proterozoic") :
      min_interval = "Proterozoic"
    else :
      print "MIN_INTERVAL DOESN'T HAVE EARLY LATE OR MIDDLE ", min_interval

  if "-" in max_interval:
    parts = fix_parts(max_interval.split("-"))
    if (parts[0] == "Early") or (parts[0] == "Late") or (parts[0] == "Middle") :
      max_interval = ' '.join(parts)
    elif (parts[1] == "Proterozoic") :
      max_interval = "Proterozoic"
    else :
      print "MAX_INTERVAL DOESN'T HAVE EARLY LATE OR MIDDLE ", max_interval

  try :
    age_bottom = interval_lookup[max_interval][1]
  except :
    if max_interval.split(" ")[0] == "Late" :
      max_interval = "Upper " + max_interval.split(" ")[1]
      age_bottom = interval_lookup[max_interval][1]
    elif max_interval.split(" ")[0] == "Early" :
      max_interval = "Lower " + max_interval.split(" ")[1]
      age_bottom = interval_lookup[max_interval][1]
    elif max_interval == "preCambrian" :
      age_bottom = interval_lookup["Precambrian"][1]
    else :
      print "MESS UP MAX", max_interval

  try :
    age_top = interval_lookup[min_interval][2]
  except :
    if min_interval.split(" ")[0] == "Late" :
      min_interval = "Upper " + min_interval.split(" ")[1]
      age_top = interval_lookup[min_interval][2]
    elif min_interval.split(" ")[0] == "Early" :
      min_interval = "Lower " + min_interval.split(" ")[1]
      age_top = interval_lookup[min_interval][2]
    elif min_interval == "preCambrian" :
      age_top = interval_lookup["Precambrian"][2]
    else :
      print "MESS UP MIN", min_interval

  min_interval_id = interval_lookup[replace_precam(min_interval)][0]
  max_interval_id = interval_lookup[replace_precam(max_interval)][0]

  cur.execute("""
    SELECT id, interval_name, interval_color 
    FROM macrostrat.intervals 
    LEFT JOIN macrostrat.timescales_intervals ON intervals.id = timescales_intervals.interval_id 
    WHERE age_bottom >= %s AND age_top <= %s 
    AND (timescale_id != 6 or timescale_id is null) 
    ORDER BY rank DESC, id asc 
    LIMIT 1""", [age_bottom, age_top])

  match = cur.fetchall()
  if len(match) > 0:
    update(match[0][0], min_interval_id, max_interval_id, unit_link)
  else :
    print str(unit_link) + "\r"

def update(containing_interval_id, min_interval_id, max_interval_id, unit_link) :
  cur.execute("UPDATE gmus.ages SET macro_containing_interval_id = %s, macro_min_interval_id = %s, macro_max_interval_id = %s WHERE unit_link = %s", [containing_interval_id, min_interval_id, max_interval_id, unit_link])
  conn.commit()

def findBestMinInterval(unit):
  # Check for min_age
  if unit[1] :
    return unit[1]
  # min_epoch
  elif unit[9] :
    return unit[9]
  # min_period
  elif unit[7] :
    return unit[7]
  # min era
  elif unit[3] :
    return unit[3]
  # min eon
  elif unit[5] :
    return unit[5]
  else :
    return ""

def findBestMaxInterval(unit):
  # Check for max_age
  if unit[2] :
    return unit[2]
  # max_epoch
  elif unit[10] :
    return unit[10]
  # max_period
  elif unit[8] :
    return unit[8]
  # max era
  elif unit[4] :
    return unit[4]
  # max eon
  elif unit[6] :
    return unit[6]
  else :
    return ""

# Connect to the database
try:
  conn = psycopg2.connect(dbname=credentials.pg_db, user=credentials.pg_user, host=credentials.pg_host, port=credentials.pg_port)
except:
  print "Could not connect to database: ", sys.exc_info()[1]
  sys.exit()

cur = conn.cursor()

cur.execute("select * from macrostrat.intervals i LEFT JOIN macrostrat.timescales_intervals ti ON i.id = ti.interval_id WHERE (timescale_id != 6 or timescale_id is null) order by interval_name asc")
intervals = cur.fetchall()

interval_lookup = {}

for i, interval in enumerate(intervals):
  interval_lookup[interval[3]] = interval


#cur.execute("select gid, unit_link, min_age, max_age, min_era, max_era, min_eon, max_eon, min_period, max_period, min_epoch, max_epoch from gmus where color is null")
cur.execute("select unit_link, min_age, max_age, min_era, max_era, min_eon, max_eon, min_period, max_period, min_epoch, max_epoch from gmus.ages")
units = cur.fetchall()

total = len(units)
for i, unit in enumerate(units):
  sys.stdout.write("\r" + str(i) + " of " + str(total))
  sys.stdout.flush()

  # Check for min_age
  if is_valid(unit[1]) :
    min_interval = unit[1]
  # min_epoch
  elif is_valid(unit[9]) :
    min_interval = unit[9]
  # min_period
  elif is_valid(unit[7]) :
    min_interval = unit[7]
  # min era
  elif is_valid(unit[3]) :
    min_interval = unit[3]
  # min eon
  elif is_valid(unit[5]) :
    min_interval = unit[5]


  # Check for max_age
  if is_valid(unit[2]) :
    max_interval = unit[2]
  # max_epoch
  elif is_valid(unit[10]) :
    max_interval = unit[10]
  # max_period
  elif is_valid(unit[8]) :
    max_interval = unit[8]
  # max era
  elif is_valid(unit[4]) :
    max_interval = unit[4]
  # max eon
  elif is_valid(unit[6]) :
    max_interval = unit[6]

  else :
    min_interval = findBestMinInterval(unit)
    max_interval = findBestMaxInterval(unit)

  try :
    max_interval
    min_interval
  except NameError:
    print "MIN OR MAX_INTERVAL NOT DEFINED ", unit


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
      except:
        print "\n EQUIVALENCY MESS UP ", unit
        continue

      update(int_id, int_id, int_id, unit[0])

  else :
    parse_range(min_interval, max_interval, unit[0])
