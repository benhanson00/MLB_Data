# -*- coding: utf-8 -*-
"""
Created on Tue Apr 19 21:36:45 2022

@author: benja
"""
if __name__ == '__main__':

    import pandas as pd
    import mysql.connector
    from mysql.connector import Error
    from sqlalchemy import create_engine

    class Extractor:
    
        def __init__(self, path):
        
            self.path = path
    
        def load_csv(self, path):
               
            df = pd.read_csv(path)
        
            return df
    
    class Restruct:
    
        def __init__(self, data):
        
            self.data = data
   
        def restructure(self, data):
        
            column = list(data.columns.values)
            column1 = column[0].split(',')

            df_dict = {col:[] for col in column1}

            for r in range(len(data)):
                for j in range(len(column1)):
                    row = data.iloc[r,:][0].split(',')
                    df_dict[column1[j]].append(row[j])
                
                new_df = pd.DataFrame(df_dict)    
    
    
            print('dataframe successfully created')
            return new_df
    
    class Server_Connect:

        def __init__(self, host, username, password):
        
        
            self.host = host
            self.username = username
            self.password = password
        
        def connect_to_server(self, host, username, password):
        
            try:
                cnct = mysql.connector.connect(host = host, username = username, password = password) # creates a connection to sql server
                print("MySQL Server Connection Successful")
            except Error as err:
                    print(f"Error: '{err}'")
        
            return cnct
    
    class Database_Build:
    
        def __init__(self, host, username, password, database_name):
        
            self.host = host
            self.username = username
            self.password = password
            self.database_name = database_name
    
        def create_db(self, database_name):
        
            try:
                cnct = mysql.connector.connect(host = host, username = username, password = password) # creates a connection to sql server
                mycursor = cnct.cursor()                              # allows us to write sql queries
                mycursor.execute(f"CREATE DATABASE {database_name}")  # executes query to create data of the name that we specified
                print("Database Created Successfully")
            except Error as err:
                    print(f"Error: '{err}'")
            
            return mycursor
    
        def connect_to_db(self, host, username, password, database_name):
        
            try:
                mydb = mysql.connector.connect(host = host, username = username, password = password, database = database_name)  # connects to specified sql db
                print("MySQL Database Connection Successful")
            except Error as err:
                    print(f"Error: '{err}'")
        
            return mydb
    
    class Table_Make:
    
        def __init__(self, host, username, password, database_name, table_name):
        
            self.host = host
            self.username = username 
            self.password = password
            self.database_name = database_name
            self.table_name = table_name
    
        def make_table(self, df, table_name):
        
            column = list(df.columns.values)
        
            try:
                table = f"CREATE TABLE IF NOT EXISTS {table_name}("        
                for col in range(len(column)):
                    table += '\n' + column[col] + ' ' + input(f"Enter data type for {column[col]}: ") + ','  
                table = table[:-1] + ')'
        
                mydb = mysql.connector.connect(host = host, username = username, password = password, database = database_name)  # connects to specified sql db
                mycursor = mydb.cursor()
                mycursor.execute(table)
                print("Table Created Successfully or Already Exists")
            except Error as err:
                    print(f"Error: '{err}'")
        
            return table
    
    class Table_Populate:
    
        def __init__(self, host, username, password, database_name, table_name):
        
            self.host = host
            self.username = username 
            self.password = password
            self.database_name = database_name
            self.table_name = table_name
    
        def pop_table(self, df, table_name, host, username, password, database_name):
        
            engine = create_engine(f'mysql://{username}:{password}@{host}/{database_name}') # creates a connection to the sql db
            
            fill_table = df.to_sql(con = engine, name = table_name, if_exists = 'replace')
        
            return fill_table
    
    path = r'C:\Users\benja\OneDrive\Documents\python\MLB_Data\batting_1960.csv'

    df = Extractor(path)
    df = df.load_csv(path)

    dataf = Restruct(df)
    d_frame = dataf.restructure(df)

    d_frame = d_frame.rename(columns = {'OPS+':'OPS_Plus', 'Name-additional':'Name_ID'})

    host = 'localhost'
    username = 'root'
    password = 'password'
    database_name = 'MLB_Data'
    table_name = 'mlb_batting_1960'


    cnct = Server_Connect(host, username, password)

    cnct.connect_to_server(host, username, password)

    mydb = Database_Build(host, username, password, database_name)

    mydb = mydb.create_db(database_name)

    conn = Database_Build(host, username, password, database_name)

    conn = conn.connect_to_db(host, username, password, database_name)

    table = Table_Make(host, username, password, database_name, table_name)

    table = table.make_table(d_frame, table_name)

    fill_table = Table_Populate(host, username, password, database_name, table_name)

    fill_table = fill_table.pop_table(d_frame, table_name, host, username, password, database_name)
