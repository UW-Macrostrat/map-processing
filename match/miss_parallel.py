import multiprocessing
from miss_multiprocessor import *
import psycopg2
from psycopg2.extensions import AsIs
import sys
import os

sys.path = [os.path.join(os.path.dirname(__file__), os.pardir)] + sys.path
import credentials


if __name__ == '__main__':
  tasks = multiprocessing.JoinableQueue()
  results = multiprocessing.Queue()

  num_processors = multiprocessing.cpu_count() - 2
  processors = [Processor(tasks, results) for i in xrange(num_processors)]

  for each in processors:
    each.start()

  conn = psycopg2.connect(dbname=credentials.pg_db, user=credentials.pg_user, host=credentials.pg_host, port=credentials.pg_port)
  cur = conn.cursor()

  # Get all the units that have a unit_link that has a match
  cur.execute("""
    with unit_links AS (select distinct unit_link from gmus.geounits_macrounits),
         gids as (select distinct geologic_unit_gid from gmus.geounits_macrounits)
    select gid, unit_link, geom from gmus.lookup_units lu 
    WHERE gid not in (select geologic_unit_gid from gids) AND unit_link in (select unit_link from unit_links)
  """)
  orphans = cur.fetchall()

  print "----- ", len(orphans), " ------"
  
  for idx, orphan in enumerate(orphans) :
    # Create a task for each poor orphan
    tasks.put(Task(idx, orphan))


  for i in range(num_processors):
    tasks.put(None)
