from suds.client import Client
from suds.sudsobject import asdict
import pandas as pd
import xml.etree.ElementTree as et 
import json
#import logging
import sys
import xmltodict
import csv
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import Code.logger.logger as logging

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

filename = r'Data\Logs\zucchetti_import.log'
log  = logging.setup_applevel_logger(filename)          
logging.set_warning('[START]')
url = "https://saas.hrzucchetti.it/WFLENAVISPA/servlet/SQLDataProviderServer/SERVLET/hrw_vqr_monitorassenze2?wsdl"
client = Client(url, retxml=True)
r = client.service.hrw_vqr_monitorassenze2_TabularQuery('hrwebservice', 'Zucchetti123!', '000001','2022-01-01','2023-12-31')
print(len(r))
soup = BeautifulSoup(r, 'xml')
el_Key_EmployeeANAG = soup.find_all("Key_EmployeeANAG")
el_Voce_cod = soup.find_all("Voce_cod")
el_DTSTARTVL = soup.find_all("DTSTARTVL")
el_DTENDVL = soup.find_all("DTENDVL")
el_IDPROCED = soup.find_all("IDPROCED")
el_Approved = soup.find_all("Approved")
data = []
for i in range(0,len(el_Key_EmployeeANAG)):
   rows = [el_Key_EmployeeANAG[i].get_text(),el_Voce_cod[i].get_text(),
           el_DTSTARTVL[i].get_text(),el_DTENDVL[i].get_text(),el_IDPROCED[i].get_text(),
           el_Approved[i].get_text()]
   data.append(rows)
df = pd.DataFrame(data,columns = ['Key_EmployeeANAG','Voce_cod',
                                  'DTSTARTVL','DTENDVL','IDPROCED',
                                  'Approved'])
df["DTSTARTVL"] = pd.to_datetime(df["DTSTARTVL"]) 
df["DTENDVL"] = pd.to_datetime(df["DTENDVL"])
print(df.info())
print(df.head())
df.to_csv(r'C:\Users\Administrator.MSCITA\MSC\IT016-BI Cubes - Documents\HR\CSV Automatic sending\hrw_vqr_MonitorAssenze_438.csv', index = False)
logging.set_warning('[END]')