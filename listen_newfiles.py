import listener
import apptools

listener.run( filter = 'process/with_process&last=0', 
              callback = apptools.handle_newfiles , 
              configfile = 'edw.ini')

