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
  
  tasks.put(Task(13, "mbr", "unit_name"))
  tasks.put(Task(14, "fm", "unit_name"))
  tasks.put(Task(15, "gp", "unit_name"))
  tasks.put(Task(16, "sgp", "unit_name"))

  tasks.put(Task(17, "mbr", "unitdesc"))
  tasks.put(Task(18, "fm", "unitdesc"))
  tasks.put(Task(19, "gp", "unitdesc"))
  tasks.put(Task(20, "sgp", "unitdesc"))

  tasks.put(Task(21, "mbr", "unit_com"))
  tasks.put(Task(22, "fm", "unit_com"))
  tasks.put(Task(23, "gp", "unit_com"))
  tasks.put(Task(24, "sgp", "unit_com"))

  for i in range(num_processors):
    tasks.put(None)
