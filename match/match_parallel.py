import multiprocessing
from multiprocessor import *

if __name__ == '__main__':
  tasks = multiprocessing.JoinableQueue()
  results = multiprocessing.Queue()

  num_processors = multiprocessing.cpu_count() - 2
  processors = [Processor(tasks, results) for i in xrange(num_processors)]

  for each in processors:
    each.start()
  
  # Define our dozen tasks
  
  tasks.put(Task(1, "mbr", "unit_name"))
  tasks.put(Task(2, "fm", "unit_name"))
  tasks.put(Task(3, "gp", "unit_name"))
  tasks.put(Task(4, "sgp", "unit_name"))

  tasks.put(Task(5, "mbr", "strat_unit"))
  tasks.put(Task(6, "fm", "strat_unit"))
  tasks.put(Task(7, "gp", "strat_unit"))
  tasks.put(Task(8, "sgp", "strat_unit"))

  tasks.put(Task(9, "mbr", "unitdesc"))
  tasks.put(Task(10, "fm", "unitdesc"))
  tasks.put(Task(11, "gp", "unitdesc"))
  tasks.put(Task(12, "sgp", "unitdesc"))

  tasks.put(Task(13, "mbr", "unit_com"))
  tasks.put(Task(14, "fm", "unit_com"))
  tasks.put(Task(15, "gp", "unit_com"))
  tasks.put(Task(16, "sgp", "unit_com"))


  for i in range(num_processors):
    tasks.put(None)
