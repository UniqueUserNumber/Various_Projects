import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import sqlalchemy
import urllib.request
import time
from datetime import date
from datetime import timedelta 
import requests
from io import StringIO
import pysftp
import os
import numpy as np

tic = time.clock()
toc = time.clock()

print('Test2')
# This will transform to have the datatypes. This is needed in SQLAlchemy for the create engine to work. An example is below
# dfReadyNode.to_sql('RevRecRawPricingHourly', engine, if_exists = 'append', index = False, dtype = dtypesRaw)
#==============================================================================
# This server is dead now.
server = ''
user = ''
pwd = ''

params = 'DRIVER={SQL Server Native Client 11.0};SERVER=%s;PORT=52808;DATABASE=GlobalTrading;UID=%s;PWD=%s;' % (server, user, pwd)
params = urllib.parse.quote_plus(params)
engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
#==============================================================================

#==============================================================================
def db_connect():
    """Pass nothing. Returns Pyodbc connection object. Call this function to connect to hardcoded database."""
    print('Connecting to GlobalTrading database...')
    con = None
    try:
        #  Hardcoded details for SQL server connection including username and password
        con = pyodbc.connect(
            r'DRIVER={SQL Server};'
            r'SERVER=;'
            r'DATABASE=;'
            r'UID=;'
            r'PWD=')
    except Exception as e:
        print(e)
        input('Connection Error, please close console')
        exit()
    print('Connection Established')
    return con


#==============================================================================
#==============================================================================
#==============================================================================
################# This can run custom sql code::::
#==============================================================================
#==============================================================================
#==============================================================================
def db_exec(con, command='', commit=0):
    """
    Provide Pyodbc connection to first parameter, a string containing the SQL command to the second parameter, and a
        0 or 1  for the commit parameter.
    Returns cursor if commit=0, otherwise it returns nothing.
    ====
    Call this function to execute command on SQL database.
    ====
    """
    # Initializes cursor object
    print('Initializing Cursor...')
    cursor = None
    try:
        cursor = con.cursor()
    except Exception as e:
        print(e)
        input('Cursor Initialization Error, please close console')
        exit()
    print('Cursor Initialized')

    # Executes command passed from the command parameter
    print('Executing Command...', command)
    tic = time.clock()
    try:
        cursor.execute(command)
    except Exception as e:
        cursor.close()
        print(e)
        input('Command Execution Error, please close console')
        exit()
    toc = time.clock()
    print('Command Executed \n Runtime: ', toc-tic)

    # Commits or does not commit changes to database
    if commit == 0:
        return cursor
    else:
        print('Committing to DB...')
        try:
            cursor.commit()
        except Exception as e:
            cursor.close()
            print(e)
            input('Error Committing to DB, please close console')
            exit()
        print('Changes Committed')

        print('Disconnecting from Server...')
        cursor.close()
        print('Disconnected')
        return

#==============================================================================