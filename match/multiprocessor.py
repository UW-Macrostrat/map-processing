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
      INSERT INTO gmus.geounits_macrounits_redo (geologic_unit_gid, unit_id, strat_name_id, unit_link, type) (
         WITH gmus AS (SELECT gid, unit_link, """ + self.gmus_field + """ AS unit_text, the_geom FROM gmus.geologic_units_with_intervals),
              macro AS (SELECT us.unit_id AS unit_id, lsn.""" + self.rank +"""_id AS strat_name_id, lsn.""" + self.rank +"""_name AS strat_name, c.poly_geom
                 FROM %(macrostrat_schema)s.units_sections us
                 JOIN %(macrostrat_schema)s.unit_strat_names usn ON us.unit_id = usn.unit_id
                 JOIN %(macrostrat_schema)s.lookup_strat_names lsn ON usn.strat_name_id = lsn.strat_name_id
                 JOIN %(macrostrat_schema)s.cols c ON us.col_id = c.id
                 WHERE c.status_code = 'active'
              )
         SELECT gmus.gid, macro.unit_id, macro.strat_name_id, gmus.unit_link, """ + str(self.type) + """ AS type FROM gmus, macro
         WHERE strat_name != '' AND ST_Intersects(gmus.the_geom, macro.poly_geom) AND gmus.unit_text ~* concat('\y', macro.strat_name, '\y')
      )
    """, {"macrostrat_schema": AsIs(credentials.pg_macrostrat_schema)})
    pyConn.commit()

    print "-- DONE WITH ", self.rank, " - ", self.gmus_field, " --"

