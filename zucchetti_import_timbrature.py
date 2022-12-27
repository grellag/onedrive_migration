from sqlite3 import Date
from suds.client import Client
from suds.sudsobject import asdict
import time
import pandas as pd
import xml.etree.ElementTree as et 
import json
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from datetime import datetime as dt, date
import datetime
import Code.logger.logger as logging
import Code.smtp.smtp
import numpy as np
import os
import matplotlib.pyplot as plt
import pathlib

global min_data
global max_data
global x
global y
global path_plot

def create_visualization(x,y):
    '''
        Create Visualization
        return:
            - saves the created plot in ./results/
            - dir: path to working directory
            - path: path to saved plot
    '''

    # Font size
    plt.rc('font', size=8)          # controls default text sizes
    plt.rc('axes', titlesize=10)    # fontsize of the axes title
    plt.rc('axes', labelsize=10)    # fontsize of the x and y labels
    plt.rc('xtick', labelsize=5)    # fontsize of the tick labels
    plt.rc('ytick', labelsize=8)    # fontsize of the tick labels
    plt.rc('legend', fontsize=8)    # legend fontsize
    plt.rc('figure', titlesize=12)  # fontsize of the figure title
    #plt.xticks(rotation=45)
    #create new plot with matplotlib
    #fig, ax = plt.subplots(figsize=(45,9))
    fig, ax = plt.subplots(figsize=(20,5))
    for index in range(len(x)):
        ax.text(x[index], y[index], y[index], size=10, rotation=45, 
        verticalalignment='top', horizontalalignment='center')
    # add a bar plot to the figure
    ax.scatter(x,y, color='cyan', edgecolor = 'darkblue')
    ax.set(xlabel='Date', ylabel='Employee [#]')
    plt.setp(ax.get_xticklabels(), rotation = 45)
    plt.subplots_adjust(left=0.1, bottom=0.1, right=0.6, top=0.8)
    #fig.autofmt_xdate()

    # define filename with current date e.g. "2021-04-08.png"
    filename = str(date.today()) + ".png"

    # working directory
    dir = pathlib.Path(__file__).parent.absolute()

    # folder where the plots should be saved
    folder = r"/results/"

    path_plot = str(dir) + folder + filename

    # save plot
    fig.savefig(path_plot, dpi=fig.dpi, bbox_inches='tight', pad_inches=0)

    return path_plot, dir

def fastest_object_to_dict(obj):
    if not hasattr(obj, '__keylist__'):
        return obj
    data = {}
    fields = obj.__keylist__
    for field in fields:
        val = getattr(obj, field)
        if isinstance(val, list):  # tuple not used
            data[field] = []
            for item in val:
                data[field].append(fastest_object_to_dict(item))
        else:
            data[field] = fastest_object_to_dict(val)
    return data

def recursive_dict(d):
    out = {}
    for k, v in asdict(d).iteritems():
        if hasattr(v, '__keylist__'):
            out[k] = recursive_dict(v)
        elif isinstance(v, list):
            out[k] = []
            for item in v:
                if hasattr(item, '__keylist__'):
                    out[k].append(recursive_dict(item))
                else:
                    out[k].append(item)
        else:
            out[k] = v
    return out

def recursive_asdict(d):
    """Convert Suds object into serializable format."""
    out = {}
    for k, v in asdict(d).items():
        if hasattr(v, '__keylist__'):
            out[k] = recursive_asdict(v)
        elif isinstance(v, list):
            out[k] = []
            for item in v:
                if hasattr(item, '__keylist__'):
                    out[k].append(recursive_asdict(item))
                elif not isinstance(item, list):
                    out[k] = item
                else:
                    out[k].append(item)
        else:
            out[k] = v
    return out
    
def suds_to_json(data):
    return json.dumps(recursive_asdict(data))

def send_email_detail():
    fromaddr = "giorgio.grella@msclenavi.it"
    ccaddr = ""
    #toaddr = fromaddr + ", IT016-lnitsys.lenavi@msclenavi.it, angelo.allegri@msclenavi.it, niccolo.rotondi@msclenavi.it"
    toaddr = "giorgio.grella@msclenavi.it"
    subject = "[PROD] - Zucchetti - Timbrature dal " + str(min_data) + " al " + str(max_data)
    htmlEmail = """
                <p> Buon giorno, <br/><br/>
                <br/></p>
                <p> Per favore contattare lnitsys@msclenavi.it  per chiarimenti. <br/>
                    Grazie<br/><br/>
                    Saluti, <br/>
                    lnitdev@msclenavi.it </p>
                <br/><br/>
                <font color="red">Please do not reply to this email as it is auto-generated. </font>
    """

    Code.smtp.smtp.send_email(path_plot, fromaddr, toaddr, ccaddr, subject, htmlEmail)

filename = str(date.today()) + ".png"
dir = pathlib.Path(__file__).parent.absolute()
folder = r"/results/"
path_plot = str(dir) + folder + filename

#today = str(dt.today().date()).replace('-', '')
today = str(dt.today().date())
#print(today)
filename = r'Data\Logs\zucchetti_timbrature.log'
log  = logging.setup_applevel_logger(filename)          
logging.set_warning('[START]')


url = "https://saas.hrzucchetti.it/Gpres3lenavispa/servlet/SQLDataProviderServer/SERVLET/hptg_timbrature_g2?wsdl"

try:
    client = Client(url, retxml=True)
except:
    logging.set_error(f'Error connecting to {url}')
#print(client)
try:
    r = client.service.hptg_timbrature_g2_TabularQuery('hrwebservice', 'Zucchetti123!', '000001', '2021-01-01', today)
    print(len(r))
except:
    logging.set_error(f'Error connecting to {r}')
soup = BeautifulSoup(r, 'xml')
# find <p> with child 2 value
el_IDCOMPANY = soup.find_all("IDCOMPANY")
#logging.set_warning(f'el_IDCOMPANY list: {el_IDCOMPANY}')
el_IDEMPLOY = soup.find_all("IDEMPLOY")
el_BNDATO = soup.find_all("BNDATO")
el_BNORAOE1 = soup.find_all("BNORAOE1")
el_BNORAOU1 = soup.find_all("BNORAOU1")
el_BNORAOE2 = soup.find_all("BNORAOE2")
el_BNORAOU2 = soup.find_all("BNORAOU2")
el_BNORAOE3 = soup.find_all("BNORAOE3")
el_BNORAOU3 = soup.find_all("BNORAOU3")
data = []
for i in range(0,len(el_IDEMPLOY)):
   rows = [el_IDCOMPANY[i].get_text(),el_IDEMPLOY[i].get_text(),el_BNDATO[i].get_text(),
           el_BNORAOE1[i].get_text(),el_BNORAOU1[i].get_text(),
           el_BNORAOE2[i].get_text(),el_BNORAOU2[i].get_text(),
           el_BNORAOE3[i].get_text(),el_BNORAOU3[i].get_text()]         
   data.append(rows)
df = pd.DataFrame(data,columns = ['IDCOMPANY','IDEMPLOY','BNDATO',
                                  'BNORAOE1','BNORAOU1',
                                  'BNORAOE2','BNORAOU2',
                                  'BNORAOE3','BNORAOU3'])
df["BNDATO"] = pd.to_datetime(df["BNDATO"]) 
#print(df.info())
#print(df.head())
path_Share = r'C:\Users\Administrator.MSCITA\MSC\IT016-BI Cubes - Documents\HR\CSV Automatic sending\hrw_vqr_Timbrature_438.csv'
try:
    df.to_csv(path_Share, index = False)
except:
    logging.set_error(f'Error path to {path_Share}')
    exit()

min_data = df['BNDATO'].min().strftime("%d/%m/%Y")
max_data = df['BNDATO'].max().strftime("%d/%m/%Y")
#print(df['BNDATO'].min())
#print(df['BNDATO'].max())
#df_subset = df.loc[:, ['BNDATO', 'IDEMPLOY']
df['Date'] = pd.to_datetime(df['BNDATO'], utc=True).dt.date
cur_month = str(df['BNDATO'].max().strftime("%d/%m/%Y"))
day, month, year = map(int, cur_month.split('/'))
cur_month = datetime.date(year, month, 1)
#print(cur_month)
df_subset = (
    df.groupby(['Date'], sort=False)
    .agg(count_distinct_ID=('IDEMPLOY', 'nunique'))
    .reset_index()
)
df_subset2= df_subset[df_subset['Date'] >= cur_month]
#print(df_subset2.info())
#print(df_subset2.head())
x = df_subset2['Date'].values.tolist()
y = df_subset2['count_distinct_ID'].values.tolist()
path_plot, dir = create_visualization(x, y)

send_email_detail()