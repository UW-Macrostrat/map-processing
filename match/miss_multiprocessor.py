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
  def __init__(self, idx, orphan):
    self.idx = idx
    self.orphan = orphan

  # Acts as the controller for a given year
  def __call__(self, connection=None):
    pyConn = connection
    pyCursor1 = pyConn.cursor()

    print self.idx, "     (", self.orphan[0], ", ", self.orphan[1], ")"
    
    pyCursor1.execute("""
      select gid, unit_id, strat_name_id, gm.unit_link, st_distance(lu.geom::geography, %(geom)s) AS distance
      from gmus.lookup_units lu
      JOIN gmus.geounits_macrounits gm ON lu.gid = gm.geologic_unit_gid
      WHERE lu.unit_link = %(link)s AND gm.type != 88
      ORDER BY st_distance(lu.geom::geography, %(geom)s)
    """, {"link": self.orphan[1], "geom": self.orphan[2]})

    potentials = pyCursor1.fetchall()

    min_distance = potentials[0][4]

    insert_string = ""
    for each in potentials :
      if each[4] == min_distance :
        insert_string += "(%(gid)s, " + str(each[1]) + ", " + str(each[2]) + ", %(unit_link)s, 88), "

    sql = "INSERT INTO gmus.geounits_macrounits (geologic_unit_gid, unit_id, strat_name_id, unit_link, type) VALUES " + insert_string[:-2]

    #print pyCursor1.mogrify(sql, {"gid": self.orphan[0], "unit_link": self.orphan[1]})
    pyCursor1.execute(sql, {"gid": self.orphan[0], "unit_link": self.orphan[1]})
    pyConn.commit()

