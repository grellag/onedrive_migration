import shlex
import datetime
from subprocess import check_output, CalledProcessError, STDOUT
import subprocess

def cmdline(command):
    
    cmdArr = shlex.split(command)
    try:
        output = check_output(cmdArr,  stderr=STDOUT).decode()
        
    except (CalledProcessError) as e:
        output = e.output.decode()
        
    except (Exception) as e:
        output = str(e);
        
    return str(output)





address = '10.1.63.58'
res = subprocess.check_output(['ping', address, '-c', '3'])


for line in res.splitlines():
    print(line)