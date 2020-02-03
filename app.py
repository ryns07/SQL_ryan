from flask import Flask
import requests
import pandas as pd
import numpy as np
import sqlite3

app = Flask(__name__)

#GET_ALL_TABLE_NAME
#Funsi API dimulai dari @app.route
@app.route('/data/get/<database_name>',methods=['GET'])
#Fungsi untuk mendapatkan semua nama table pada database
def GET_ALL_TABLE_NAME(database_name):
    #definisikan object connection /data/databasename
    conn = sqlite3.connect('data/' + str(database_name))
    #perintah SQL yang digunakan SELECT semua nama pada sqlite_master ambil semua nama table yang tidak ada string sqlite%
    sql_command = "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    #simpan pada dataframe data1 hasil SQL Query tersebut
    data1 = pd.read_sql_query(str(sql_command),conn)
    #kembalikan dataframe
    return (data1.to_json())

#GET_ALL_COlUMN_AT_TABLE
# API ditambahkan adalah <table> dimana object <table> akan diparsing ke dalam SQL Query
@app.route('/data/get/<database_name>/<table>',methods=['GET'])
#Fungsi untuk mendapatkan semua nama kolom pada table
def GET_TABLE_CONTENT(database_name,table):
    #definisikan object connection /data/databasename
    conn = sqlite3.connect('data/' + str(database_name))
    # pilih semua kolom pada <table>
    sql_command = "SELECT * FROM "+table
    #simpan pada dataframe data1 hasil SQL Query tersebut
    data2 = pd.read_sql_query(str(sql_command),conn)
    #kembalikan dataframe
    return (data2.to_json())

#GET_TABLE_AND_NAME
@app.route('/data/get/<database_name>/<table>/<column>',methods=['GET'])
def GET_TABLE_AND_NAME(database_name,table,column):
    conn = sqlite3.connect('data/' + str(database_name))
    sql_command = "SELECT "+column+" FROM "+table
    data3 = pd.read_sql_query(str(sql_command),conn)
    return (data3.to_json())

#COMBINED_TABLE
@app.route('/data/get/<database_name>/<table1>/<table2>/<key>',methods=['GET'])
def COMBINED_TABLE(database_name,table1,table2,key):
    conn = sqlite3.connect('data/' + str(database_name))
    #Lakukan parsing pada table1, table2 dan key
    sql_command = "SELECT Customers.Firstname,Customers.LastName,Customers. \
    City,Customers.Address,Invoices.Total FROM " +table1+ " LEFT JOIN "+table2+" ON "+table1+"."+key+"="+table2+"."+key
    data4 = pd.read_sql_query(str(sql_command),conn)
    #kembalikan dataframe
    return (data4.to_json())

#COMBINED_TABLE_WITH_CONFIGURABLE_COLUMN
@app.route('/data/get/<database_name>/<table1>/<table2>/<key>/<column>',methods=['GET'])
def COMBINED_TABLE_WITH_CONFIGURABLE_COLUMN(database_name,table1,table2,key,column):
    conn = sqlite3.connect('data/' + str(database_name))
     # Lakukan parsing pada table 1, table 2 dan key kedalam SQL Query
    sql_command = "SELECT "+table1+"."+column+" FROM " +table1+ " LEFT JOIN "+table2+" ON "+table1+"."+key+"="+table2+"."+key
    data5 = pd.read_sql_query(str(sql_command),conn)
    return (data5.to_json())

#COMBINED_TABLE_WITH_CONFIGURABLE_TWO_COLUMN
@app.route('/data/get/<database_name>/<table1>/<table2>/<key>/<column1>/<column2>',methods=['GET'])
def COMBINED_TABLE_WITH_CONFIGURABLE_TWO_COLUMN(database_name,table1,table2,key,column1,column2):
    conn = sqlite3.connect('data/' + str(database_name))
    # Lakukan parsing pada table 1, table 2,key, column1,column2 kedalam SQL Query
    sql_command = "SELECT "+table1+"."+column1+","+table2+"."+column2+" FROM " +table1+ " LEFT JOIN " \
    +table2+" ON "+table1+"."+key+"="+table2+"."+key
    data6 = pd.read_sql_query(str(sql_command),conn)
    return (data6.to_json())

#COMBINED_TABLE_WITH_CONFIGURABLE_TWO_COLUMN_GET_TOP5
@app.route('/data/get/<database_name>/<table1>/<table2>/<key>/<column1>/<column2>/<command>',methods=['GET'])
def COMBINED_TABLE_WITH_CONFIGURABLE_TWO_COLUMN_GET_TOP5(database_name,table1,table2,key,column1,column2,command):
    conn = sqlite3.connect('data/' + str(database_name))
    sql_command = "SELECT "+table1+"."+column1+","+table2+"."+column2+" FROM " +table1+ " LEFT JOIN " \
    +table2+" ON "+table1+"."+key+"="+table2+"."+key
    if command=="top5":
        value=5
    elif command=="top10":
        value=10
    elif command=="top15":
        value=15
    data7 = pd.read_sql_query(str(sql_command),conn)
    data7 = data7.sort_values(by = 'Total', ascending = False).head(value)
    return (data7.to_json())
    
        
if __name__ == '__main__':
    app.run(debug=True, port=5000)
