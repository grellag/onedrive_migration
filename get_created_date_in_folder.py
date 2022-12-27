import os
import time
import stat
import pandas as pd


path = "./Data/Output"
files_sorted_by_date = []

cols = ['filename', 'date_creation']
lst = []


filepaths = [os.path.join(path, file) for file in os.listdir(path)]

file_statuses = [(os.stat(filepath), filepath) for filepath in filepaths]

files = ((status[stat.ST_CTIME], filepath) for status, filepath in file_statuses if stat.S_ISREG(status[stat.ST_MODE]))

for creation_time, filepath in sorted(files):
    creation_date = time.ctime(creation_time)
    filename = os.path.basename(filepath)
    files_sorted_by_date.append(creation_date + " " + filename)
    lst.append([filename, creation_date])
df1 = pd.DataFrame(lst, columns=cols)
print(df1.info())
#print(files_sorted_by_date)