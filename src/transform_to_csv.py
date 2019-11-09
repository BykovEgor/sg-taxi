# To-Do:
# - use asyncio
# - try multiprocessing

import pickle
from telethon.tl.types import MessageEntityUrl
from telethon.tl.types import MessageMediaWebPage
from telethon.tl.types import WebPage
from telethon.tl.types import Photo
import telethon.tl.types

import pandas as pd
import numpy as np
import csv
import os

csv_encoding = 'utf-8-sig'
csv_err_handl = 'backslashreplace'

file_names = ['SGHitch_msg.pkl']
output_names = ['SGHitch']

test_file_name = 'test.csv'
N_test = 1000
chunk_size = 5000

cols = ["date", "mess_id", "user_id", "username", "bot",
    "deleted", "action", "act_entr", "message", "reply_to_msg_id",
    "mentioned"]

def f(x,field):

    try:
        return x['_sender']['User'][field] #['_sender']['User']['username']

    except TypeError:
        return 'None'

def parce_mess(entry):
    
    return [entry['date'],
            entry['id'],
            entry['_sender_id'],
          f(entry,'username'),
          f(entry,'bot'),
          f(entry,'deleted'),
            entry['action'],
            entry['_action_entities'],
            entry['message'],
            entry['reply_to_msg_id'],
            entry['mentioned']]

def parce_serv(entry):
    
    return [entry['date'],
            entry['id'],
            entry['_sender_id'],
          f(entry,'username'),
          f(entry,'bot'),
          f(entry,'deleted'),
       list(entry['action'].keys())[0],
            entry['_action_entities'],
            entry['message'],
            entry['reply_to_msg_id'],
            entry['mentioned']]

def parce_obj(obj):
    
    error_line = [-1] * len(cols)
    
    try:
        obj_type = list(obj.keys())[0]
    except: return error_line
    
    if obj_type == 'Message':
        return parce_mess(obj['Message'])
    elif obj_type == 'MessageService':
        return parce_serv(obj['MessageService'])
    else: return error_line

# Initiating DataFrame

def initiate_df(cols:list):
    return pd.DataFrame(dict(zip(cols,[[0] for i in cols])),
                        columns=cols)

def concat_df(df1, new_line:list, cols):
    line = pd.DataFrame(dict(zip(cols,new_line)),
                        index = list(range(df1.shape[0])))
    return pd.concat([df1,line], axis = 0)

def read_from_pkl(file_name, new_file_name, n = -1):

    def update_progress(pers):

        print("Reading pickle: %d%% is done\r"%pers, end = '')

    # amending params for test mode
    if n < 0:
        total_blocks = file_size(file_name)
        ch_size = chunk_size
    else:
        total_blocks = 100000000000
        ch_size = round(N_test/3)  

    i = n
    block = 0
    chunk = []
    with open(file_name, 'rb') as fl_o:
        with open(new_file_name,"w", encoding=csv_encoding, errors=csv_err_handl) as csvfileOut:
            
            writer = csv.writer(csvfileOut,  delimiter=',',quotechar='"')
            # writing headers
            writer.writerow(cols)
        
            while True:
                i -= 1
                block += 1

                # Updating progress
                if block % 1000 == 0:
                    pers = round(block*100 / total_blocks,0)
                    update_progress(pers)
            
                # Exiting in case of partial conferiton
                if i == 0: break
            
                try:
                    o = pickle.load(fl_o)
                    chunk.append(parce_obj(o))
                    
                    # Saving current chunk, erasing dataframe
                    if block % ch_size == 0:
                        writer.writerows(chunk)
                        chunk = []
                
                except EOFError:
                    writer.writerows(chunk)
                    chunk = []
                    print('\nPikle file has been read: {} entries'.format(block))
                    break

def Input_File(file_name):

    n_file = file_name

    while True:
        try:
            with open(n_file,'rb') as file_o:
                break

        except FileNotFoundError:
            n_file = input('Wrong FileName or File not found. Enter correct path:')

        else: print('File "{}" found!'.format(n_file))
    return n_file

def Output_File(file_name):

    n_file = file_name
    
    while True:
        try:
            with open(n_file,'rb') as file_o:
                if input('File {} alresdy exist. Replace? [Y/n]'.format(n_file)) == 'n':
                    n_file = input('Enter new file name: ')
                    continue
                else: return n_file

        except FileNotFoundError:
            return n_file

def file_size(file_name):

    size = 0
    print('Cheking file size...', end='')
    with open(file_name,'rb') as file_o:
        while True:
            try:
                _ = pickle.load(file_o)
                size += 1
            except EOFError:
                print('file size: {} blocks'.format(size))
                return size

if __name__ == "__main__":

    for i in range(len(file_names)):
        file_names[i] = os.path.join(os.path.dirname(__file__).replace('src','data'),file_names[i])
    print(file_names)

    new_file_names = [i[:-3] + 'csv' for i in file_names]
    print(new_file_names)

    for i in range(len(file_names)):
        file_names[i] = Input_File(file_names[i])
        new_file_names[i] = Output_File(new_file_names[i])

    for i in range(len(file_names)):
        
        print('Test convertion. Reading and saving {} entries.... (file "{}")'.format(N_test,test_file_name))
        read_from_pkl(file_names[i], test_file_name, N_test)
        test = pd.read_csv(test_file_name, encoding=csv_encoding)
        print(test.head(),"\n")
        print(test.tail(),'\n')
        print(test.describe(),"\n")

    for i in range(len(file_names)):

        print('Saving {} to CSV: {}'.format(file_names[i], new_file_names[i]))
        read_from_pkl(file_names[i], new_file_names[i])
        with open(file_names[i], encoding=csv_encoding, errors=csv_err_handl) as file_o:
            data_df = pd.read_csv(file_o)

        # Getting rid of rows processed with errors: data = 0 or -1

        data_df = data_df[data_df['mess_id']>0]
        data_df.fillna(0,inplace =True)

        data_df['act_flag'] = data_df['action'] != 0
        data_df['mess_flag'] = data_df['message'] !=0
        data_df['reply_flag'] = data_df['reply_to_msg_id'] !=0
        data_df['bot'] = data_df['bot'] == "True"
        data_df['deleted'] = data_df['deleted'] == "True"
        data_df['date'] = pd.to_datetime(data_df['date'])

        # Process text messegaes (getting rid of URLs)

        # Saving to final HDF5

        data_df.to_hdf(output_names[i], key = 'df')