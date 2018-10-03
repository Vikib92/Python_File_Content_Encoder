import pandas as pd
from Crypto.Cipher import ARC4
import os
from sqlalchemy import create_engine
    
def DB_Table_Query_Exec(tab_name):
    
#    global cols, data
    try:
        cursor = create_engine('sqlite:///db.sqlite', echo=False)
#        tablename = "DC_Marvel"
        query = "select * from {}".format(tab_name)
        data = cursor.execute(query).fetchall()
        cols = cursor.execute(query).keys()
        cursor.dispose()
        return cols, data
    except Exception as e:
        print(e)

    
def XL_Table_Query_Exec(fle_name):
    
    try:
        df = pd.read_excel(fle_name + '.xlsx')
    except Exception as e:
        print(e)
    else:
        cols = df.columns
        data = df.values
        return cols, data

def CSV_Table_Query_Exec(fle_name):
    
    try:
        df = pd.read_csv(fle_name + '.csv')
    except Exception as e:
        print(e)
    else:
        cols = df.columns
        data = df.values
        return cols, data

def Tab_Export_Excel_Unprot(cols,data,obj_name):
    
    df = pd.DataFrame(list(data), columns = cols)
    if 'index' in df.columns:
        df = df.drop('index',1)
    writer = pd.ExcelWriter(obj_name + '_Dec.xlsx',engine='xlsxwriter')
    df.to_excel(writer, sheet_name = obj_name + '_data',index = False)
    writer.save()
    
def Tab_Export_Excel_Prot(cols,data,obj_name,cols_enc):
    
    df = pd.DataFrame(list(data), columns = cols)
    Mask_Conf_Data(df,cols_enc)
    if 'index' in df.columns:
        df = df.drop('index',1)
    writer = pd.ExcelWriter(obj_name + '_enc.xlsx',engine='xlsxwriter')
    df.to_excel(writer, sheet_name = obj_name + '_data',index = False)
    writer.save()
    
def CSV_Export_Excel_Unprot(cols,data,obj_name):
    
    df = pd.DataFrame(list(data), columns = cols)
    if 'index' in df.columns:
        df = df.drop('index',1)
    df.to_csv(obj_name + '_Dec.CSV',index = False)

    
def CSV_Export_Excel_Prot(cols,data,obj_name,cols_enc):
    
    df = pd.DataFrame(list(data), columns = cols)
    Mask_Conf_Data(df,cols_enc)
    if 'index' in df.columns:
        df = df.drop('index',1)
    df.to_csv(obj_name + '_Enc.CSV',index = False)
    
    
def Mask_Conf_Data(df,cols_enc):
    
    for ln in cols_enc:    
        for i in df.index:
            if isinstance(df[ln][i], int):
                df[ln][i] = str(df[ln][i])
            df[ln][i] = Data_Encrypt(df[ln][i]).decode("utf-8","replace")
    
    file.close()
    
def Data_Encrypt(data):
    
    key = os.urandom(16)
    enc = ARC4.new(key)
#   dec = ARC4.new(key)
    encpt = enc.encrypt(data)
    return encpt    
    

def db_tab_enc(access,tab_name,cols_enc):    
    if access.lower() == 'p':
        c,d = DB_Table_Query_Exec(tab_name)
        Tab_Export_Excel_Prot(c,d,tab_name,cols_enc)
    else:
        c, d = DB_Table_Query_Exec(tab_name)
        Tab_Export_Excel_Unprot(c, d,tab_name)

def xl_fle_enc(access,fle_name,cols_enc):    
    if access.lower() == 'p':
        c,d = XL_Table_Query_Exec(fle_name)
        Tab_Export_Excel_Prot(c,d,fle_name,cols_enc)
    else:
        c,d = XL_Table_Query_Exec(fle_name)
        Tab_Export_Excel_Unprot(c, d,fle_name)

def csv_fle_enc(access,fle_name,cols_enc):    
    if access.lower() == 'p':
        c,d = CSV_Table_Query_Exec(fle_name)
        CSV_Export_Excel_Prot(c,d,fle_name,cols_enc)
    else:
        c,d = CSV_Table_Query_Exec(fle_name)
        CSV_Export_Excel_Unprot(c, d,fle_name)


acc_lvl = "P"
table_name = "DC_Marvel"    
file_name = "DC_Marvel"
csv_name = "DC_Mar"
config = "config.txt"
file = open(config,"r")
rd = file.readlines()
cols_enc = []
    
for fld in rd:
    cols_enc.append(fld.strip('\n'))

xl_fle_enc(acc_lvl,file_name,cols_enc)

db_tab_enc(acc_lvl,table_name,cols_enc)

csv_fle_enc(acc_lvl,csv_name,cols_enc)



