# Adapted/borrowed from http://stackoverflow.com/a/7556042/1956065
import multiprocessing
import psycopg2
from psycopg2.extensions import AsIs
import sys
import os

sys.path = [os.path.join(os.path.dirname(__file__), os.pardir)] + sys.path
import credentials


class Processor(multiprocessing.Process):

  def __init__(self, task_queue, result_queue):
    multiprocessing.Process.__init__(self)
    self.task_queue = task_queue
    self.result_queue = result_queue
    self.pyConn = psycopg2.connect(dbname=credentials.pg_db, user=credentials.pg_user, host=credentials.pg_host, port=credentials.pg_port)
    self.pyConn.set_isolation_level(0)

  def run(self):
    proc_name = self.name
    while True:
      next_task = self.task_queue.get()
      if next_task is None:
          print 'Tasks complete on this thread'
          self.task_queue.task_done()
          break            
      answer = next_task(connection=self.pyConn)
      self.task_queue.task_done()
      self.result_queue.put(answer)
    return


class Task(object):
  # Assign check and year when initialized
  def __init__(self, type, rank, gmus_field):
    self.type = type
    self.rank = rank
    self.gmus_field = gmus_field

  # Acts as the controller for a given year
  def __call__(self, connection=None):
    pyConn = connection
    pyCursor1 = pyConn.cursor()

    print "------ Working on ", self.rank, " - ", self.gmus_field, " ------"
    
    
    pyCursor1.execute("""
      INSERT INTO gmus.%(pg_geounits_macrounits)s (geologic_unit_gid, unit_id, strat_name_id, unit_link, type) (
         WITH gmus AS (SELECT gid, unit_link, %(gmus_field)s AS unit_text, age_top, age_bottom, 25 as age_buffer
                FROM gmus.lookup_units
                JOIN macrostrat.intervals on macro_interval_id = macrostrat.intervals.id
                WHERE unit_link NOT IN (SELECT DISTINCT unit_link FROM gmus.%(pg_geounits_macrounits)s)),
              macro AS (SELECT us.unit_id AS unit_id, lsn.%(rank)s_id AS strat_name_id, lsn.%(rank)s_name AS strat_name, lui.lo_age as age_top, lui.fo_age as age_bottom 
                 FROM %(macrostrat_schema)s.units_sections us
                 JOIN %(macrostrat_schema)s.lookup_unit_intervals lui ON us.unit_id = lui.unit_id
                 JOIN %(macrostrat_schema)s.unit_strat_names usn ON us.unit_id = usn.unit_id
                 JOIN %(macrostrat_schema)s.lookup_strat_names lsn ON usn.strat_name_id = lsn.strat_name_id
                 JOIN %(macrostrat_schema)s.cols c ON us.col_id = c.id
                 WHERE c.status_code = 'active'
              )
         SELECT DISTINCT ON (gmus.gid, strat_name_id, unit_link) gmus.gid, macro.unit_id, macro.strat_name_id, gmus.unit_link, %(type)s AS type FROM gmus, macro
         WHERE strat_name != '' AND gmus.unit_text ~* concat('\y', macro.strat_name, '\y')
         AND (((macro.age_top) <= (gmus.age_bottom + gmus.age_buffer)) AND ((gmus.age_top - gmus.age_buffer) <= (macro.age_bottom)))
      )
    """, {
      "macrostrat_schema": AsIs(credentials.pg_macrostrat_schema), 
      "pg_geounits_macrounits": AsIs(credentials.pg_geounits_macrounits),
      "gmus_field": AsIs(self.gmus_field),
      "rank": AsIs(self.rank),
      "type": AsIs(self.type)
    })
    pyConn.commit()

    print "-- DONE WITH ", self.rank, " - ", self.gmus_field, " --"

