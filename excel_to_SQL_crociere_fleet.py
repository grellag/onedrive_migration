import os
import shutil
import pandas as pd
import pyodbc
from fast_to_sql import fast_to_sql as fts
import shutil
from datetime import date
import logging

# ------- GLOBAL VARIABLES ---------
local_path = r"\\10.63.244.160\sambasrv\cruises"
dest_path = r"\\10.1.63.191\C$\Users\Administrator.MSCITA\OneDrive - Mediterranean Shipping Company\Documentazione DEV\CROCIERE\Storico"
sql = {'server': '10.63.244.157',
               'database': 'CROCIERE',
               'table_name': 'Fleet',
               'username': 'Lenavi_Giornali',
               'password': 'Lenavi2020'}

excel = {'excel_name': 'MasterCruiseCalendar.xlsx',
         'sheet_name': 'MASTER CRUISE CALENDAR',
         'list_columns': ["Vessel", "Cruise ID",	"From", "To", "Nights",	"Itinerary Code", "Itinerary Description", "Season",
                          "Remark", "Area Code", "Area Description", "Region Code", "Region", "Subregion code", "Subregion"],
         'columns_renamed': {"Vessel":"Ship", "Cruise ID":"Cruise", "Nights":"Nts", "Itinerary Code":"Itin", "Itinerary Description":"Destination", 
                             "Remark":"Remarks", "Area Code":"Area", "Area Description":"Area_Desc", "Region Code":"Region", "Region":"Region_Desc", 
                             "Subregion code":"SubRegion", "Subregion":"SubRegion_Desc"}
        }
# -------- END GLOBAL VARIABLES ----------
logging.basicConfig(filename='Excel_to_SQL_Crociere_Fleet.log', filemode='a+',
                        format='%(asctime)s - %(message).200s', datefmt='%d-%b-%y %H:%M:%S')

# --------- CONNECTION TO SQL SERVER ----------
try:
    logging.warning(f'Connecting to Server: {sql["server"]}, Database: {sql["database"]}')
    connection = pyodbc.connect(Driver="{SQL Server}", # for 191 use ODBC Driver 17 for SQL Server
                                        Server=sql["server"],
                                        Database=sql["database"],
                                        UID=sql["username"],
                                        PWD=sql["password"])
except Exception:
            logging.error(f'Failure connecting to Server: {sql["server"]}, Database: {sql["database"]}', exc_info=True)

# --------- READING EXCEL -----------
file_names = os.listdir(local_path)
logging.warning(f'Searching for file: {excel["excel_name"]} in path: {local_path}')
for file in file_names:
    fullpath = os.path.join(local_path, file)
    if os.path.isfile(fullpath) and file == excel["excel_name"]:
        logging.warning(f'Found file: {file}')
        df = pd.read_excel(fullpath, sheet_name= excel['sheet_name'], usecols=excel["list_columns"], engine="openpyxl") # use engine='xlrd' for '.xls', engine='openpyxl' for '.xlsx'
        season = ['Summer', 'Winter']
        df = df[df['Season'].isin(season)]
        logging.warning(f'Dataframe columns BEFORE renaming: {df.columns}')
        print(df.head())
        try:
            df = df.rename(columns= excel["columns_renamed"])
            logging.warning(f'Column headers renamed into: {df.columns}')
        except Exception:
            logging.error('Error occurred while renaming columns', exc_info=True)

        # ------- DELETE SQL TABLE --------
        tsql = f"DELETE FROM [CROCIERE].[dbo].[{sql['table_name']}]"
        cursor = connection.cursor()
        try:
            cursor.execute(tsql)
            logging.warning(f"Deleted table: {sql['table_name']}, from database: {sql['database']}")
        except Exception:
            logging.error(f'Error occurred while deleting table {sql["table_name"]}', exc_info=True)
        
        # --------- UPLOAD DATAFRAME TO SQL ----------
        try:
            create_statement = fts.fast_to_sql(df, sql['table_name'], connection)
            connection.commit()
            logging.warning(f'Succesfully loaded dataframe from Excel into SQL table: {sql["table_name"]}')
        except Exception:
            logging.error(f'Failure uploading dataframe into SQL', exc_info=True)
        cursor.close()

        # ------- MOVING FILE ---------
        week = date.today().isocalendar().week
        year = date.today().year

        split_filename = file.split('.xlsx')[0]       
        try:
            shutil.move(fullpath, f'{dest_path}\{split_filename}_{year}_{week}.xlsx')
            logging.warning(f'Succesfully moved {file} FROM {local_path} TO: {dest_path}\n')
        except Exception:
            logging.error(f'Error while moving {file} FROM {local_path} TO: {dest_path}', exc_info=True)
