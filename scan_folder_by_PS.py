import os
import pandas as pd
import glob
import numpy as np
import Code.logger.logger
import seaborn as sns
import matplotlib.pyplot as plt
import smtplib
import io
import textwrap

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email import encoders
import imghdr
from pretty_html_table import build_table

from email.message import EmailMessage
from email.utils import make_msgid
from pretty_html_table import build_table
import smtplib
import imghdr
from base64 import b64encode
import subprocess
import unicodedata
from base64 import b64encode
import subprocess
import unicodedata
import pyodbc
import openpyxl
from fast_to_sql import fast_to_sql as fts

def df_from_network_folder():
    listOfPath = [r"\\lnshare"]
    df = pd.DataFrame(columns=['FolderName', 'DomainUser'])
    for path_i in listOfPath:
        #directory = subprocess.Popen(['powershell', r"(net view {}) -match '\sDisk\s' -replace '\s+Disk.*'".format(path_i)], stdout=subprocess.PIPE)
        directory = subprocess.Popen(['powershell', r"(Get-ChildItem {}) -match '\sDisk\s' -replace '\s+Disk.*'".format(path_i)], stdout=subprocess.PIPE)
Get-ChildItem D:\MSSQL2K8\DATA\*.* -include *.ldf,*.mdf | select name,length -last 8

        print(directory)
        list_items = directory.communicate()[0].splitlines()
        result = [simplify(item) for item in list_items]
        FolderNameL = [path_i for item in list_items]

        df_temp = pd.DataFrame(list(zip(FolderNameL, result)), columns=['FolderName', 'DomainUser'])
        df = df.append(df_temp, ignore_index=True)
    df.rename(columns={'folder_name': 'o365Account',
                        'FolderName': 'FolderName'
                        }, inplace=True)
    return df

'''
    Check if there are special characters 
'''
def simplify(text):
	try:
		text = text.decode('ISO-8859-1')
	except NameError:
		pass
	text = unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode("ISO-8859-1")
	return str(text)

'''
    Concert hh:mm:ss into MINUTES 
'''
def dhms_to_min(dhms_str):
    '''supports also format of d:h:m:s, h:m:s, m:s and s'''
    _,d,h,m,s = (':0'*10+dhms_str).rsplit(':',4)
    return int(d)*24*60*60+int(h)*60*60+int(m)*60+int(s)/60

'''
    For the given path, get the List of all files in the directory tree 
'''
def getListOfFiles(dirName):
    # create a list of file and sub directories 
    # names in the given directory 
    listOfFile = os.listdir(dirName)
    allFiles = list()
    # Iterate over all the entries
    for entry in listOfFile:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory 
        if os.path.isdir(fullPath):
            allFiles = allFiles + getListOfFiles(fullPath)
        else:
            allFiles.append(fullPath)
                
    return allFiles     

'''
    Transform dataframe in INFO
'''
def df_transform(df_user_exp):
    temp = ''
    user_prev = ''
    data = []
    i = 0

    for index in range(len(df_user_exp)):
        folder = str(df_user_exp['Source'].iloc[index])
        user = df_user_exp['Destination'].iloc[index].rsplit('/', 1)[1]
        user = user.replace('_', '.')
        o365Account = user.replace('.msclenavi.it', '@msclenavi.it')
        user = user.replace('.msclenavi.it', '@msclenavi.it')
        total_bytes = df_user_exp['Total_bytes'].iloc[index]
        migrated_GB = df_user_exp['Migrated_GB'].iloc[index]
        migrated_bytes = df_user_exp['Migrated_bytes'].iloc[index]
        
        duration = dhms_to_min(str(df_user_exp['Duration'].iloc[index]))
        Start_time = df_user_exp['Start_time'].iloc[index]
        End_time = df_user_exp['End_time'].iloc[index]
        
        date_only = df_user_exp['date_only'].iloc[index]
        
        if migrated_bytes > 0:
            data.append([folder, user, o365Account, total_bytes, migrated_GB, migrated_bytes, duration, Start_time, End_time, date_only])
            #log.debug(f'DF transform: {user} from {Start_time} to {Start_time} for duration: {duration} sec.')
            if user == user_prev:
                temp += '\n' + user + ' ' + str(migrated_GB)
            else:
                temp = user + ' ' + str(migrated_GB)
                user_prev = user
                str_body = f'Moved folder: {folder} - Migrated GB: {migrated_GB} in {duration}'
                #df_send_email(fromaddr, user, str_body, 'detail', '')
        else:
            log.debug(f'DF transform: {user} from {Start_time} for duration: {duration} sec. - NO DATA!!!')

    df_format = pd.DataFrame(data, columns=['folder', 'user', 'o365Account', 'total_bytes', 'migrated_GB', 'migrated_bytes', 'duration', 'Start_time', 'End_time', 'date_only'])
    df_format.sort_values(by = ['Start_time', 'user'], axis=0, ascending=[True, True], inplace=True, kind='quicksort', na_position='first', ignore_index=True, key=None)
    df_format['user'].str.replace('@msclenavi.it', '', regex=True)
    df_format['user'] = df_format['user'].str.replace('@msclenavi.it', "", regex=True)
    from datetime import datetime
    #df_format['date_only'] = data['date_only'].apply(lambda x: datetime.strptime(x,'%Y/%m/%d') if len(x[:str(x).find ('-')]) == 4 else datetime.strptime(x, '%d/%m/%Y'))
    return df_format

'''
    Module to send EMAIL
'''
def df_send_email(fromaddr, toaddr, str_body, img_dashboard, str_type, df):
    #toaddr = fromaddr + ", IT016-lnitsys.lenavi@msclenavi.it, fabio.galluzzo@msclenavi.it"
    #toaddr = "angelo.allegri@msclenavi.it, niccolo.rotondi@msclenavi.it, giorgio.grella@msclenavi.it, lorenzo.camplone@msclenavi.it"
    df_email = df[['folder', 'user', 'migrated_GB', 'duration', 'bytes_percentage', 'date_only']]
    df_email = df_email[~df_email.user.isin(['giorgio.grella','lorenzo.camplone'])]
    df_email = df_email.sort_values(['date_only','user'], ascending=False)
    df_email.duration = df_email.duration.round(2)
    df_email.rename(columns=({'folder': 'Folder Migrated', 
                            'migrated_GB': 'GByte Migrated', 
                            'duration': 'Migrated in sec.', 
                            'bytes_percentage': 'Perc_Migrated', 
                            'date_only': 'Migration_started'}), inplace=True)
    
    max_date = max(df['Start_time'])
    min_date = min(df['Start_time'])
    subject = f"[PROD] - Report - OnedriveMigration from {min_date} to {max_date}"
    log.debug(f"Sending email - Subject: {subject}")

    if str_type != "summary":
        htmlEmail = f"""
                    <p> Buon giorno {toaddr}, <br/><br/>
                        elenco cartelle migrate in Onedrive.<br/><br/>
                        {str_body}
                    <br/></p>
                    <p> Per favore contattare lnitsys@msclenavi.it  per chiarimenti su mancanze o attività incomplete. <br/>
                        Grazie<br/><br/>
                        Saluti, <br/>
                        lnitdev@msclenavi.it </p>
                    <br/><br/>
                    <font color="red">Please do not reply to this email as it is auto-generated. </font>
        """
    else:
        unique_value = df_email["user"].nunique()
        df_email_no_full = df_email[(df_email.user != "giorgio.grella") & (df_email.Perc_Migrated != 1.0)]
        unique_value_no_full = df_email_no_full["user"].nunique()
        htmlEmail = """
                    <p> Buon giorno, <br/><br/>
                        I colleghi che hanno proceduto alla migrazione sono <font color="red"><b>{0}, di cui {4} non al 100%</b></font>,<br/><br/>
                        questo è l'elenco cartelle migrate in Onedrive.<br/><br/>
                        {1}
                    <br/></p>
                    <img src="data:image/{2};base64, {3}" width="90%" >
                    <br/><br/>
                    <font color="red">Please do not reply to this email as it is auto-generated. </font>
        """.format(unique_value,build_table(df_email, 'blue_light'),img_format,b64encode(img_dashboard).decode('ascii'),unique_value_no_full)
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg["To"] = ", ".join(toaddr)
    msg["Cc"] = ", ".join(ccaddr)
    msg['Subject'] = subject
    msg.attach(MIMEText(htmlEmail, 'html'))
    server = smtplib.SMTP('10.21.56.22', 25)
    server.starttls()
    #server.login(fromaddr, "yourpassword")
    text = msg.as_string()

    try:
        log.debug(f"Sending email - To {toaddr} and in CC: {ccaddr}")
        server.sendmail(fromaddr, toaddr, text)
    except Exception as e:
        log.debug(f"Exception occurred on sending email to {toaddr}  and in CC: {ccaddr}", exc_info=True)
    server.quit()
    return subject

def run(self, cmd):
    completed = subprocess.run(["powershell", "-Command", cmd], capture_output=True)
    return completed

def DU_LNSHARE_sql_INSERT(df):
    tsql = "DELETE FROM [Lenavi_Tools].[dbo].[PY_DomainUser_Lnshare]"
    cursor = conn_msql.cursor()
    cursor.execute(tsql)
    for index,row in df.iterrows():
        cursor.execute('INSERT INTO dbo.PY_DomainUser_Lnshare([DomainUser],[Dim_folder]) values (?,?)', 
                        row['DomainUser'], 
                        row['Dim_folder'])
        conn_msql.commit()
    conn_msql.commit()
    cursor.close()

def DU_LNSHARE_sql_SELECT():
    tsql = "SELECT [DomainUser],[Dim_folder]*1000000 AS [Dim_folder] FROM [Lenavi_Tools].[dbo].[PY_DomainUser_Lnshare]"
    cursor = conn_msql.cursor()
    df = pd.read_sql(tsql, conn_msql)
    print(df.head())
    return df   

def DU_OneDrive_sql_INSERT(df):
    table_name = "PY_DomainUser_OneDrive"
    tsql = "DELETE FROM [Lenavi_Tools].[dbo].[PY_DomainUser_OneDrive]"
    cursor = conn_msql.cursor()
    cursor.execute(tsql)
    df['Start_time'] = df['Start_time'].astype(str).replace({'NaT': None})
    df['End_time'] = df['End_time'].astype(str).replace({'NaT': None})
    df['date_only'] = df['date_only'].astype(str).replace({'NaT': None})   
    #-------------------------------------------------------------------------- 
    create_statement = fts.fast_to_sql(df, table_name, conn_msql)
    #--------------------------------------------------------------------------
    df['[Start_time]'] = pd.to_datetime(df['[Start_time]'], dayfirst=True)
    df['[End_time]'] = pd.to_datetime(df['[End_time]'], dayfirst=True)
    df['[date_only]'] = pd.to_datetime(df['[date_only]'], dayfirst=True)
    df.rename(columns={'[FolderName]': 'FolderName',
                        '[DomainUser]': 'DomainUser',
                        '[o365Account]': 'DomainUser',
                        '[user]': 'user',
                        '[folder]': 'folder',
                        '[total_bytes]': 'total_bytes',
                        '[migrated_bytes]': 'migrated_bytes',
                        '[migrated_GB]': 'migrated_GB',
                        '[duration]': 'duration',
                        '[Start_time]': 'Start_time',
                        '[End_time]': 'End_time',
                        '[date_only]': 'date_only',
                        '[bytes_percentage]': 'bytes_percentage',
                        '[Dim_folder]': 'Dim_folder'},
          inplace=True, errors='raise')
    conn_msql.commit()
    cursor.close()
    print(df_list_shared_migr.info())


def search_dim_folder(df_folder_source):
    num_folder_dim_search = 0
    new_cols = ["DomainUser", "Dim_folder"]
    list_DomainUser = []
    list_Dim_folder = []
    #df_folder_source = df_folder_source.head()
    #df_folder_source = df_folder_source[df_folder_source.DomainUser == 'GrellaG']
    for row in df_folder_source.itertuples():
        cmd_pshell = f"((Get-ChildItem {row.FolderName}\{row.DomainUser}\ -Recurse | Measure-Object -Property Length -Sum -ErrorAction Stop).Sum / 1MB)"
        process = subprocess.Popen(["powershell", cmd_pshell],
                                    stdout              = subprocess.PIPE,
                                    stderr              = subprocess.PIPE,
                                    text                = True,
                                    shell               = True,
                                    universal_newlines  = True)
        # Separate the output and error by communicating with process variable.
        # This is similar to Tuple where we store two values to two different variables
        std_out, std_err = process.communicate()
        std_out.strip(), std_err
        # Store the return code in rc variable
        rc = process.wait()
        result, err = process.communicate()
        if process.returncode != 0:
            print("An error occured: %s", process.stderr)
            log.debug(f'Search_dim_folder: {row.FolderName} DomainUser: {row.DomainUser} in errore: {process.stderr}')
        else:
            num_folder_dim_search = num_folder_dim_search + 1
            list_DomainUser.append(row.DomainUser)
            list_Dim_folder.append(result)
            log.debug(f'Search_dim_folder with cmd: {cmd_pshell}. Dimensione folder: {result}')
    user_list = pd.DataFrame(list(zip(list_DomainUser, list_Dim_folder)), columns=new_cols)
    user_list['Dim_folder'] = user_list['Dim_folder'].apply(lambda x : float(x.replace("\n","").replace(",",".")))
    user_list['Dim_folder'] = user_list['Dim_folder']*1000000
    DU_LNSHARE_sql_INSERT(user_list)
    return user_list

'''
MAIN
'''
if __name__ == '__main__':
    os.environ['MPLCONFIGDIR'] = r'C:\Windows\system32\config\systemprofile\.matplotlib'
    log             = Code.logger.logger.setup_applevel_logger(file_name = 'Data/Logs/app_scan_folder_by_PS_debug.log')
    log_excel_df    = r'Data/Logs/export_dataframe_scan_folder_by_PS.xlsx'
    sql_server      = {'server': '10.1.63.147', 'database': 'Lenavi_Tools', 'username': 'dblinkuser', 'password': 'jasmine'}
    conn_msql       = Code.odbc.odbc.createDBConnection(sql_server['server'], sql_server['database'], sql_server['username'], sql_server['password'])
    script          = ''
    img_format      = 'png'
    user_prev       = ''
    str_body        = ''
    fromaddr        = 'giorgio.grella@msclenavi.it'
    toaddr          = ['giorgio.grella@msclenavi.it']
    ccaddr          = ["giorgio.grella@msclenavi.it"]
    #folderPath      = r'C:\inetpub\wwwroot\Python\Onedrive_Migration'
    folderPath      = ''
    recal_DIM       = True
    status_mail     = True
    df_list_folder  = df_from_network_folder()
    writer          = pd.ExcelWriter(log_excel_df)
    df_list_folder.to_excel(writer, 'df_final', index = False)
    writer.save()
    
        
    
    
