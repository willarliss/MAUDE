import zipfile as zf
from glob import glob
import os
import sqlite3 as sql
import pandas as pd
import dask.dataframe as dd
import csv



def device_reader(conn):
"""Decompress many zip files and form new database of their contents for devices"""

    folders = glob('./device/*')
    c = conn.cursor()
    count = 1
    
    for folder in folders:
    
        # If not all folders are present in glob folder
        if len(folders) < 46:
            
            try:
                files = zf.ZipFile(folder, 'r')
                files.extractall(path=os.path.join(os.getcwd(), 'device')) 
            except:
               pass
         
        # Iterate over every text file in folder
        if folder[-4:] == '.txt':
            
            df = dd.read_csv(folder, delimiter='|', 
                                     dtype='str', 
                                     error_bad_lines=False,
                                     encoding='cp1252')
            df = df.compute()
            df['YEAR'] = folder[-8:-4]
            
            # If first file in folder, create new table
            if count == 1:
                df.to_sql('devices', con=conn, index=False, if_exists='replace')
                size = len(list(df.iloc[0]))
            
            # If not first file in folder, append to existing table
            else:
                for i in range(len(df)):
                    dta = list(df.iloc[i])
                    
                    while len(dta) < size:
                        dta.insert(-1, None)                
                    
                    # Number of '?' should equal var 'size'
                    c.execute("INSERT INTO devices VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", dta)
                    
            count += 1
            del df
    
    conn.commit()
    
    

def master_reader(conn):
"""Decompress zipped folder of master file and read contents into database"""
    
    file = zf.ZipFile('mdrfoiThru2019.zip', 'r')
    file.extractall()
    
    file = 'mdrfoiThru2019.txt'
    df = dd.read_csv(file, delimiter='|', 
                           dtype='str', 
                           error_bad_lines=False,
                           encoding='cp1252',
                           quoting=csv.QUOTE_NONE)
    
    df = df.compute()
    # Create new table
    df.to_sql('master', con=conn, index=False, if_exists='replace')
    del df
    
    conn.commit()
    
    

def patient_reader(conn):
"""Decompress zipped folder of patient file and read contents into database"""
    
    file = zf.ZipFile('patientthru2019.zip', 'r')
    file.extractall()
    
    file = 'patientthru2019.txt'
    df = dd.read_csv(file, delimiter='|', 
                           dtype='str', 
                           error_bad_lines=False,
                           encoding='cp1252')
    
    df = df.compute()
    # Create new table
    df.to_sql('patient', con=conn, index=False, if_exists='replace')
    del df

    conn.commit()
    
    
    
if __name__ == '__main__':
    
    connection = sql.connect('data.db')
    
    try:
       device_reader(connection)
       master_reader(connection)
       patient_reader(connection)
    except Exception as e:
        print('EXCEPTION', e)
        
    connection.close()
    
    
    

