import listener
import apptools

listener.run( filter = 'process/with_process&last=move_to_sps',  
              callback = apptools.handle_signalprocessing, 
              configfile = 'edw.ini')
