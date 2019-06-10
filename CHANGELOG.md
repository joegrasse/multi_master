## ChangeLog for multi_master script

## 2.0
  * Added support for different connection managers
  * Removed --lvs-write-group and --lvs-read-group options
  * Added --write-connections and --read-connections options
  * Added --connection-manager option

## 1.1

  * Don't kill off slave connections using GTIDs

## 1.0

  * Reworked they way interactions with the lvs servers are done
  * Added support for lvs status
  * Added lvs-write-group and lvs-read-group options
  * Changed the way system commands are ran
  * Enabled printing of multiline messages
  * Added more debug messages
  * Changed checking for current primary
  * Changed the way cleanup on failure is done  
  * Code cleanup            

## 0.5.3

  * Created get_read_event_status function

## 0.5.2

  * Cleaned up the hostname variables to the ipvsadm calls

## 0.5.1

  * Changed enabling of events to place back tics around the username and
    host of the definer

## 0.5

  * Changed proxy manipulation code

## 0.4

  * Added slave status check
  * Changed order of turning on event schedulers

## 0.3

  * Added --skip-proxy to disable proxy changes
  * Added checking of return status of proxy change
  * Added ability to past host as localhost:port
  * Added --ask-password to prompt for mysql password
  * Added --prompt-once to only prompt for password once
  * Set Event Scheduler to "NOT AVAILABLE" if not detected
  * Added check for status of MASTER_POS_WAIT
  * Code cleanup

## 0.2 

  * Added a wait for current primary to get caught back up to new primary, 
    after altering of events, to turn back on event scheduler

## 0.1

  * Initial Release  