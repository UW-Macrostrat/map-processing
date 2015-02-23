import multiprocessing
from no_space_multiprocessor import *

if __name__ == '__main__':
  tasks = multiprocessing.JoinableQueue()
  results = multiprocessing.Queue()

  num_processors = multiprocessing.cpu_count() - 2
  processors = [Processor(tasks, results) for i in xrange(num_processors)]

  for each in processors:
    each.start()
  
  # Define our dozen tasks
  
  tasks.put(Task(17, "mbr", "unit_name"))
  tasks.put(Task(18, "fm", "unit_name"))
  tasks.put(Task(19, "gp", "unit_name"))
  tasks.put(Task(20, "sgp", "unit_name"))

  tasks.put(Task(21, "mbr", "strat_unit"))
  tasks.put(Task(22, "fm", "strat_unit"))
  tasks.put(Task(23, "gp", "strat_unit"))
  tasks.put(Task(24, "sgp", "strat_unit"))

  tasks.put(Task(25, "mbr", "unitdesc"))
  tasks.put(Task(26, "fm", "unitdesc"))
  tasks.put(Task(27, "gp", "unitdesc"))
  tasks.put(Task(28, "sgp", "unitdesc"))

  tasks.put(Task(29, "mbr", "unit_com"))
  tasks.put(Task(30, "fm", "unit_com"))
  tasks.put(Task(31, "gp", "unit_com"))
  tasks.put(Task(32, "sgp", "unit_com"))


  for i in range(num_processors):
    tasks.put(None)
