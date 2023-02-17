import time
from contextlib import contextmanager
import Logger

@contextmanager
def measure_time(name):
    # measure time beforehand 
    wallTime = time.time()
    processTime = time.process_time()
    
    # process context
    yield
    
    # calculate time results
    wallTime = time.time() - wallTime
    processTime = time.process_time() - processTime
    
    strWallTime = time.strftime("%H:%M:%S", time.gmtime(wallTime))
    strProcessTime = time.strftime("%H:%M:%S", time.gmtime(processTime))
    
    Logger.log("%s: Wall Time: '%s' and Process Time: '%s'"%(name, strWallTime, strProcessTime))