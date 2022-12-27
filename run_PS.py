import subprocess
import os

def run(self, cmd):
    completed = subprocess.run(["powershell", "-Command", cmd], capture_output=True)
    return completed


if __name__ == '__main__':
    #hello_command = ""{0} MB" -f ((Get-ChildItem \\lnshare\grellaG\ -Recurse | Measure-Object -Property Length -Sum -ErrorAction Stop).Sum / 1MB)"
    #hello_info = run(hello_command)
    
    #hello_info = os.system("powershell.exe " + hello_command)


    process = subprocess.Popen(["powershell","((Get-ChildItem \\\\lnshare\grellaG\ -Recurse | Measure-Object -Property Length -Sum -ErrorAction Stop).Sum / 1MB)"],
                                stdout=subprocess.PIPE,
                                stderr = subprocess.PIPE,
                                text = True,
                                shell = True,
                                universal_newlines=True)
    # Separate the output and error by communicating with process variable.
    # This is similar to Tuple where we store two values to two different variables
    std_out, std_err = process.communicate()
    std_out.strip(), std_err

    # Store the return code in rc variable
    rc = process.wait()

    result, err = process.communicate()
    print ('output is: \n', result)
    print('error is: \n', err)

    if process.returncode != 0:
        print("An error occured: %s", process.stderr)
    else:
        print("Hello command executed successfully!")
    
    print("-------------------------")
    
    #As you observe, the return code is 0 which means the command was executed successfully
    #But the output is not clear, because by default file objects are opened in binary mode. Observe the 'b' in the starting of output which means the output is in byte code, we will get to this later.
    #The error code is also empty, this is again because our command was successful.