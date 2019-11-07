import numpy as np
import pandas as pd
import json
import sys
import os
sys.path.append(os.path.abspath('../src/'))
import re

demand = r'(hitch|htich|hithc|htihc|hitxher).*(for|look)'
supply = r'(driv|rid|bik|htihc|hich).*(for|look)'
drop   = r'drop.*:(?P<drop>.*)'
pick   = r'pick.*:(?P<pick>.*)'
date   = r'date.*:(?P<date>.*)'
time   = r'time.*:(?P<time>.*)'
pax    = r'pax.*:(?P<pax>.*)'

dem_o  = re.compile(demand,re.I | re.M)
sup_o  = re.compile(supply,re.I | re.M)
drop_o = re.compile(drop,  re.I | re.M)
pick_o = re.compile(pick,  re.I | re.M)
date_o = re.compile(date,  re.I | re.M)
time_o = re.compile(time,  re.I | re.M)
pax_o  = re.compile(pax,   re.I | re.M)

def parce_mess(row):
    """
    Parce str in column 'message' in pd.DataFrame and add 6 
    columnt to the dataframe:
    fl: 0 if messeage from hitcher, 1 if message from driver, 2 otherwise
    m_date: x from "Date: x"  pattern in the row['message'] string
    time: x from "Time: x" pattern in the row['message'] string
    pick: pick-up addres
    drop: drop off addres
    pax: number of passengers

    Usage: dataframe.apply(parce_mess, axis = 1).
    """
    mess = row['message']
    row['fl'    ] = parce_side(mess)
    row['m_date'] = parce_date(mess)
    row['time'  ] = parce_time(mess)
    row['pick'  ] = parce_pick(mess)
    row['drop'  ] = parce_drop(mess)
    row['pax'   ] = parce_pax( mess)

    return row

def parce_side(mess):
    """
    Parcing mess:str using pre-compiled regex object to determin whether it contains
    data from hitcher or driver
    Return 0 - for hitcher, 1 - for driver, 2 - for all other cases
    """
    if   dem_o.search(mess): return 0
    elif sup_o.search(mess): return 1
    else: return 2

def parce_date(mess):
    """
    Extracting date data from mess:str using pre-compiled regex object.
    Return string.
    """
    if not date_o.search(mess) is None:
        return date_o.search(mess).group(1).strip().lower()
    else:
        return np.nan

def parce_time(mess):
    """
    Extracting time data from mess:str using pre-compiled regex object.
    Return string.
    """
    if not time_o.search(mess) is None:
        return time_o.search(mess).group(1).strip().lower()
    else:
        return np.nan

def parce_pick(mess):
    """
    Extracting pick-up location from mess:str using pre-compiled regex object.
    Return string.
    """
    if not pick_o.search(mess) is None:
        return pick_o.search(mess).group(1).strip().lower()
    else:
        return np.nan

def parce_drop(mess):
    """
    Extracting drop off location from mess:str using pre-compiled regex object.
    Return string.
    """
    if not drop_o.search(mess) is None:
        return drop_o.search(mess).group(1).strip().lower()
    else:
        return np.nan

def parce_pax(mess):
    """
    Extracting pax number data from mess:str using pre-compiled regex object.
    Return string.
    """
    if not pax_o.search(mess) is None:
        return pax_o.search(mess).group(1).strip().lower()
    else:
        return np.nan

if __name__ == "__main__":

    data_pd = pd.read_csv('../data/SGHitch_msg.csv')