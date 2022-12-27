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

def createDBConnection(server, database, user, password):
    connection = pyodbc.connect(Driver="{ODBC Driver 17 for SQL Server}",
                                Server=server,
                                Database=database,
                                UID=user,
                                PWD=password)
    return connection

def closeDBConnection(connection):
    try:
        connection.close()
    except pyodbc.ProgrammingError:
        pass

def tsql_user_365(conn_msql):
    sql = "SELECT DomainUser, o365Account FROM Lenavi_Tools.dbo.VPN_STATS_ADGROUP GROUP BY DomainUser, o365Account"
    # print(sql)
    df = pd.read_sql(sql, conn_msql)
    df.drop_duplicates()
    return df

def simplify(text):
	try:
		text = text.decode('ISO-8859-1')
	except NameError:
		pass
	text = unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode("ISO-8859-1")
	return str(text)

def df_from_network_folder():
    listOfPath = [r"\\lnshare", r"\\sharefiliali"]
    df = pd.DataFrame(columns=['FolderName', 'DomainUser'])
    for path_i in listOfPath:
        directory = subprocess.Popen(['powershell', r"(net view {}) -match '\sDisk\s' -replace '\s+Disk.*'".format(path_i)], stdout=subprocess.PIPE)
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
    For each files, insert rows from Excel File into dataframe 
'''
def df_from_scan_report_summary():
    dirName = r'\\lnshare\public\IT\Onedrive_Migration\MigrationTool\msclenavi.it'
    
    # Get the list of all files in directory tree at given path
    listOfFiles = getListOfFiles(dirName)
    
    # Get the list of all files in directory tree at given path
    listOfFiles = list()
    for (dirpath, dirnames, filenames) in os.walk(dirName):
        listOfFiles += [os.path.join(dirpath, file) for file in filenames]
        
    new_names = ['Source', 'Destination', 'Total_bytes', 'Migrated_bytes', 'Migrated_GB', 'Start_time', 'End_time', 'Duration']
    df_full = pd.DataFrame(columns=new_names)
    content = [] 
    # Print the files    
    for elem in listOfFiles:
        basename = os.path.basename(elem)
        if basename == 'SummaryReport.csv':
            #print('File Name:', elem.split("\\")[-1])
            #0 - Source	
            #1 - Destination	
            #2 - Status	
            #3 - Total bytes	
            #4 - Total GB	
            #5 - Migrated bytes	
            #6 - Migrated GB	
            #7 - GB not migrated	
            #8 - Total scanned item	
            #9 - Total to be migrated	
            #10 - Migrated items	
            #11 - Items not migrated	
            #12 - Warning count	
            #13 - Start time	
            #14 - End time	
            #15 - Duration	
            #16 - GB/hour	
            #17 - Round number	
            #18 - Workflow ID	
            #19 - Task ID	
            #20 - Log Path
            df = pd.read_csv(elem, usecols=[0,1,3,5,6,13,14,15], decimal=',', skiprows=1, names = new_names).reset_index()
            df_full = pd.concat([df_full, df], ignore_index=True)
    #df_full["Total_bytes"] = df_full.Total_bytes.astype(int64)
    df.astype({'Total_bytes': 'int64'}).dtypes
    df_full = df_full[df_full.Total_bytes != 0]
    df_full['Start_time'] = pd.to_datetime(df_full['Start_time'], dayfirst=True)
    df_full['End_time'] = pd.to_datetime(df_full['End_time'], dayfirst=True)
    #df_full['Start_date'] = pd.to_datetime(df_full['Start_time'], format="%d-%m-%Y")
    df_full["year"] = df_full["Start_time"].dt.year
    df_full["month"] = df_full["Start_time"].dt.month
    df_full["day"] = df_full["Start_time"].dt.day
    df_full['date_only'] = df_full['Start_time'].dt.date
    df_full['date_only'] = pd.to_datetime(df_full['date_only'], dayfirst=True)
    return(df_full)

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
    For the user data, draw plots
'''
def df_dashboard(folderPath, df_plot):
    #df_plot = df_plot[(df_plot.user != "giorgio.grella")]
    #df_plot.query('user!="giorgio.grella"')
    full_path_df_plot = folderPath + 'Data/Imgs/dashboard.png'
    plt.style.use('fivethirtyeight') 
    sns.set_context("paper")
    plt.rcParams['font.size'] = 6           # customise font size of a particular graph title, x-axis ticker and y-axis ticker
    plt.rcParams['legend.fontsize'] = 6    # customise legend size
    plt.rcParams['figure.dpi'] = 600
    px = 1/plt.rcParams['figure.dpi'] 

    # create figure, define figure size, create an empty canvas (axes) and add tile to the graph
    rows = 2
    columns = 2
    fig, axes = plt.subplots(rows,columns,figsize=[7, 5.5],frameon = False)  # setup a figure and define 6 empty axes (3 rows and 2 columns)
    # To adjust subplot positions and avoid overlapping on components and between subplots
    plt.subplots_adjust(left=None, bottom=None, right=None, top= 0.8, wspace=0.2 , hspace=0.8)
    # assign a name to each ax
    d = {}
    i = 0
    for r in range(rows):
        for c in range(columns):
            d[i] = axes[r][c]
            i += 1

    df_plot_ordered = df_plot.sort_values(by='migrated_GB')
    g1 = sns.barplot(x="user", y="migrated_GB", data = df_plot_ordered, ax = d[0])
    g1.set_title('Migrated Users & GBytes')
    for p in g1.patches:
        g1.annotate("%.1f" % p.get_height(), 
                    (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha='center',  
                    va='center', 
                    fontsize=5, 
                    color='red', 
                    xytext=(0, 5),
                    textcoords='offset points')
    labels = g1.get_xticklabels()
    g1.set_xticklabels([textwrap.fill(t.get_text(), 20)  for t in labels], rotation=90, horizontalalignment='left', fontweight='light', fontsize='x-small')
    #for i, label in enumerate(labels):
    #    label.set_y(label.get_position()[1] - (i % 2) * 0.075)
    g1.set(xlabel=None)
    plt.tight_layout()  # avoid overlapping ticklabels, axis labels, and titles (can not control suptitle)
    
    df_plot_ordered = df_plot.sort_values(by='duration')
    g2 = sns.lineplot(x="user", y="duration", data=df_plot_ordered, ax = d[1], linewidth=0.5)
    g2.set_title('Migrated Users & Migration Duration in sec.')
    fig.canvas.draw()
    labels = g2.get_xticklabels()
    g2.set_xticklabels([textwrap.fill(t.get_text(), 20)  for t in labels], rotation=90, horizontalalignment='left', fontweight='light', fontsize='x-small')
    g2.set(xlabel=None, ylabel='Duration in sec.')

    plt.tight_layout()  # avoid overlapping ticklabels, axis labels, and titles (can not control suptitle)

    g3 = sns.barplot(x="user", y="bytes_percentage", data = df_plot[(df_plot.bytes_percentage != 1.0)], ax = d[2])
    g3.set_title('Percentage < 1 on TOT bytes')
    for p in g3.patches:
        g3.annotate("%.2f" % p.get_height(), 
                    (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha='center',  
                    va='center', 
                    fontsize=5, 
                    color='red', 
                    xytext=(0, 5),
                    textcoords='offset points')
    labels = g3.get_xticklabels()
    g3.set_xticklabels([textwrap.fill(t.get_text(), 20)  for t in labels], rotation=90, horizontalalignment='left', fontweight='light', fontsize='x-small')
    #for i, label in enumerate(labels):
    #    label.set_y(label.get_position()[1] - (i % 2) * 0.075)
    g3.set(xlabel=None)
    plt.tight_layout()  # avoid overlapping ticklabels, axis labels, and titles (can not control suptitle)

    
    df_pivot = df_plot.groupby(["date_only"]).agg({"user": pd.Series.nunique})

    # Converting the index as date
    #df_pivot['dates'] = pd.to_datetime(df_pivot.index, format='%Y%m%d')
    df_pivot['dates'] = pd.to_datetime(df_pivot.index, format='%d%m%Y')
    
    print('Ho creato pivot per g4:')
    print(df_pivot.info())
    print(df_pivot)
    g4 = sns.barplot(data=df_pivot, x="dates", y="user", ax = d[3],  palette="ch:.25")
    g4.set_title('# Migrated Users by Days')
    fig.canvas.draw()
    #g4.set_xticklabels([textwrap.fill(t.get_text(), 10)  for t in g4.get_xticklabels()], rotation=45, fontsize=5, ha='left')
    g4.set_xticklabels(g4.get_xticklabels(), rotation=30, ha='right')
    #g4.set(xlabel=None)
    # annotation here
    for p in g4.patches:
        g4.annotate("%.0f" % p.get_height(), 
                    (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha='center',  
                    va='center', 
                    fontsize=7, 
                    color='black', 
                    xytext=(0, 5),
                    textcoords='offset points')
    plt.tight_layout()  # avoid overlapping ticklabels, axis labels, and titles (can not control suptitle)
 
    # nicer label format for dates
    #fig.autofmt_xdate()
    fig = plt.gcf()
    plt.draw()

    try:
        #fig.savefig(full_path_df_plot)
        fig.savefig('Data/Imgs/figure.png', dpi = 600)
        log.debug(f'Salvataggio df_plot: ' + 'Data/Imgs/figure.png')
    except:
        log.debug(f'Salvataggio in ERR df_plot: ' + 'Data/Imgs/figure.png')
    try:
        f = io.BytesIO()
        plt.savefig(f, format=img_format)
        f.seek(0)
        img_dashboard = f.read()
    except:
        log.debug(f'Salvataggio in ERR df_plot: ' + 'Data/Imgs/figure.png')
    return img_dashboard
  
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
    log             = Code.logger.logger.setup_applevel_logger(file_name = 'Data/Logs/app_onedrive_migration_debug.log')
    log_excel_df    = r'Data/Logs/export_dataframe_o365.xlsx'
    log.debug('-------------------- START Script -------------------------')
    sql_server      = {'server': 'MSCITACLUSQL03', 'database': 'Lenavi_Tools', 'username': 'dblinkuser', 'password': 'jasmine'}
    conn_msql       = createDBConnection(sql_server['server'], sql_server['database'], sql_server['username'], sql_server['password'])
    script          = ''
    img_format      = 'png'
    user_prev       = ''
    str_body        = ''
    fromaddr        = 'giorgio.grella@msclenavi.it'
    #toaddr          = ["daniele.casaccio@msclenavi.it", "angelo.allegri@msclenavi.it", "niccolo.rotondi@msclenavi.it", "luca.marcenaro@msclenavi.it", "alessio.campaiola@msclenavi.it", "fabio.galluzzo@msclenavi.it"]
    #toaddr          = ["giorgio.grella@msclenavi.it", "lorenzo.camplone@msclenavi.it"]
    toaddr          = ['giorgio.grella@msclenavi.it']
    #toaddr          = "lorenzo.camplone@msclenavi.it, giorgio.grella@msclenavi.it"
    #ccaddr          = ["giorgio.grella@msclenavi.it", "lorenzo.camplone@msclenavi.it"]
    ccaddr          = ["giorgio.grella@msclenavi.it"]
    #folderPath      = r'C:\inetpub\wwwroot\Python\Onedrive_Migration'
    folderPath      = ''
    recal_DIM       = True
    status_mail     = True
    df_list_folder  = df_from_network_folder()
    print(df_list_folder)
    df_list_o365    = tsql_user_365(conn_msql)
    index           = df_list_o365.index
    number_of_rows  = len(index)
    print(f'N. of row in df_list_o365: {number_of_rows}')
    df_list_shared_o365   = pd.merge(df_list_folder, df_list_o365, on='DomainUser', how='inner')
    index           = df_list_shared_o365.index
    number_of_rows  = len(index)
    print(f'N. of row in df_list_shared_o365: {number_of_rows}')
    df_list_shared_o365.drop_duplicates()
    index           = df_list_shared_o365.index
    number_of_rows  = len(index)
    print(f'N. of row in df_list_shared_o365: {number_of_rows}')
    #df_list_shared_o365.to_excel (r'export_dataframe_o365.xlsx', index = False, header=True)
    writer          = pd.ExcelWriter(log_excel_df)
    df_list_shared_o365.to_excel(writer, 'List_folder_ADusers', index = False)
    # Scanning migrated folders - START
    df_user_exp     = df_from_scan_report_summary() 
    df_user_exp.to_excel(writer, 'scan_report', index = False)
    df_final        = df_transform(df_user_exp)
    df_final.to_excel(writer, 'df_final', index = False)
    # Scanning migrated folders - END
    
    # GROUP BY user - START
    df_pivot_final  = df_final.groupby(['user', 'o365Account'], as_index=False).agg({'folder': ','.join,
                                            'total_bytes':sum,
                                            'migrated_bytes':sum,
                                            'migrated_GB':sum,
                                            'duration':sum,
                                            'Start_time': 'last',
                                            'End_time': 'last',
                                            'date_only': 'last'
                                            })
    df_pivot_final["bytes_percentage"] = df_pivot_final["migrated_bytes"] / df_pivot_final["total_bytes"]
    df_pivot_final.to_excel(writer, 'df_pivot_final', index = False)
    # GROUP BY user - END
    if status_mail == True:
        img_dashboard   = df_dashboard(folderPath, df_pivot_final)
        str_msg         = df_send_email(fromaddr, toaddr, str_body, img_dashboard, 'summary', df_pivot_final)
    df_list_shared_migr = pd.merge(df_list_shared_o365, df_pivot_final, on='o365Account', how='left')
    #Aggiungere IF dati aggiornati o dati da SQL Server DB
    if recal_DIM == True:
        num_folder_dim_search = search_dim_folder(df_list_shared_migr)
    else:
        num_folder_dim_search = DU_LNSHARE_sql_SELECT()

    df_list_shared_migr= pd.merge(df_list_shared_migr, num_folder_dim_search, on='DomainUser', how='left')

    df_list_shared_migr.to_excel(writer, 'merge_df', index = False)
    DU_OneDrive_sql_INSERT(df_list_shared_migr)
    
    item_counts_DU = df_list_shared_migr.DomainUser.nunique()
    item_counts_DU_Migration = df_list_shared_migr.user.nunique()
    percentage_user = round((item_counts_DU_Migration/item_counts_DU)*100)
    print(f'Tot AD users: {item_counts_DU} - Tot Migrated users: {item_counts_DU_Migration} - Percentuale TOT: {percentage_user} %')
    writer.save()
    
        
    
    
