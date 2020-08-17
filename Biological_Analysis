# -*- coding: utf-8 -*-
"""
How this code works:
Pulls information from the sample Measurements table.
Uses that information to build:
    1) Create Table statement
    2) Insert Statement Using Unions
Executes:
    1)Delete Table If Exists
    2)Create Table designed from parameters
    3)Activate the Union command
"""
#Import Section
import pandas as pd
import urllib.request
from sqlalchemy import create_engine
import pyodbc
import time

# Here is some code that creates the connection to the server so that the 
# code can actually run. The user and password are defined below
# This can be modified so that the username and password are considered variables if you want multiple usernames.
###############################################################################
def db_connect():
    con = None
    try:
        #Hardcoded details for SQL server connection including username and password
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

# below is the section of the code that allows customized queries
# As long as the user has the permissions, the code should run as if that user
# is running the query
#==============================================================================
def db_exec(con, command='', commit=0):
    cursor = None
    try:
        cursor = con.cursor()
    except Exception as e:
        print(e)
        input('Cursor Initialization Error, please close console')
        exit()
    print('Cursor Initialized')
    
    print('Executing Command...')
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

# Setting up the engine. This can be stored in a separate file later
# A lot of times there is a password protected file that doesn't load into github
###############################################################################
server = ''
user = ''
pwd = ''
database = 'TestEnvir'

params = 'DRIVER={SQL Server Native Client 11.0};SERVER=%s;PORT=52808;DATABASE=%s;UID=%s;PWD=%s;' % (server,database, user, pwd)
params = urllib.parse.quote_plus(params)
engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)

# Please note that the Driver will have to change to match the system
connection = pyodbc.connect(
            r'DRIVER={SQL Server};'
            r'SERVER=-;'
            r'DATABASE=;'
            r'UID=;'
            r'PWD=')

# Below are the Sql Statements
###############################################################################

# If there is a date that should be looked into
# query for the distinct values that will be used in the dataframe
# for setting up the tables 
distinctValues = """
select distinct measurement_Type from Testenvir.dbo.[sample_measurements]
-- where clause can go in here for the time stamp
"""

# Creating the table section.
# All of the values are of the decimal(16,6) format
CreateFinalTable = """
create table Testenvir.dbo.experiment_measurements(
experiment_id int
, top_parent_id int
, sample_id int
, {}
);
"""

DeleteTable = """
IF OBJECT_ID('TestEnvir.dbo.experiment_measurements', 'U') IS NOT NULL 
drop table TestEnvir.dbo.experiment_measurements;
"""

###############################################################################
# Section for creating the strings that will be used in the query for creating the table
# I created what the query is.
TotalUnion = ''
OutsideSelect = ''
TableCreation = ''

### Different Parts of the Union
Step1Union= """
insert into testenvir.dbo.experiment_measurements
select experiment_id, top_parent_id, sample_id {}

from (
select s.experiment_id experiment_id, s.parent_id top_parent_id, sm.sample_id sample_id
"""

Step2Union= """
{}
"""

Step3Union = """
from [TestEnvir].[dbo].[samples] s
join [TestEnvir].[dbo].[sample_measurements] sm on s.[id] = sm.sample_id
group by s.experiment_id,s.parent_id,sm.sample_id, measurement_type) a
group by experiment_id,top_parent_id,sample_id
"""


# This pulls the information from the database into a python script
def update():
    global CreateFinalTable, TotalUnion
    TableCreation = ''
    #Creates the dataframe with the unique measurements so that it can recreate the table.
    df = pd.read_sql(distinctValues, connection)
    #Tests if there are under 1000 unique Values
    if len(df) > 1000:
        Variables = 1000
        print('There are over 1000 measurement types so this has been limited to the first 1000')
    else:
        Variables = len(df)
    
    # This part is to make it so that the code can figure out how many columns there should be
    # in the table. If there are 20 measurements, there will be 20 + 3 Columns
    for x in range(Variables):
        if x == 0:
            TableCreation = 'measurement_'+df['measurement_Type'][x]+ ' decimal(16,6)'
        else:
            TableCreation = TableCreation +'\n, measurement_'+df['measurement_Type'][x]+ ' decimal(16,6)'
    
    #This just modifies the string so that the query creates the table correctly
    CreateFinalTable = CreateFinalTable.format(TableCreation)
    # This part will create the outside part of the union so that it names the columns correctly
    for x in range(Variables):
        if x == 0:
            OutsideSelect = ',avg(measurement_'+df['measurement_Type'][x]+') measurement_'+df['measurement_Type'][x]+''
        else:
            OutsideSelect = OutsideSelect + ' , avg(measurement_'+df['measurement_Type'][x]+') measurement_'+df['measurement_Type'][x]+''
    
    # This starts off the process so that the insert statement works.
    TotalUnion = TotalUnion + Step1Union.format(OutsideSelect)
    insideSelect = ''
    # This part if for setting up the information in the union section so it can combine correctly
    for insideUnions in range(Variables):
        
        Measure = ''      
        # This sets up the union statement so when it condenses, it can collapse correctly
        # This part also makes sure that the values match
        Measure =  ",case when measurement_type = '" + df['measurement_Type'][insideUnions] + "' then sum(value) end  measurement_" + df['measurement_Type'][insideUnions] + ''
        insideSelect = insideSelect + Measure
            
    TotalUnion = TotalUnion + Step2Union.format(insideSelect)
        
    # Finalize the Union so that it can be executed.
    TotalUnion = TotalUnion + Step3Union
    
    # execute the queries that were created
    db_exec(db_connect(), DeleteTable, commit=1)
    db_exec(db_connect(), CreateFinalTable, commit=1)
    db_exec(db_connect(), TotalUnion, commit=1)

def __init__():
    update()  # When this program is run immediately run the update() function    

if __name__ == "__main__":
    __init__()  # When run as a script call this function
    
