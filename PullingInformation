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

my_auth_User = []
my_auth_Pass = []

UserPassCombo = 0

STARTDATE = 'today- 2'
ENDDATE = 'today '

print('Test2')
# This will transform to have the datatypes.
#==============================================================================
# This server is dead now.
server = 'ANDVr-DAYZERDB1\DAYZER1'
user = 'YesEnergyUpdater'
pwd = 'Updater'

params = 'DRIVER={SQL Server Native Client 11.0};SERVER=%s;PORT=52808;DATABASE=GlobalTrading;UID=%s;PWD=%s;' % (server, user, pwd)
params = urllib.parse.quote_plus(params)
engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
#==============================================================================


# Clearing up raw information
DeleteFromRaw1 = """
delete from  [GlobalTrading].[dbo].[RevRecRawPricing15Min];
delete from  [GlobalTrading].[dbo].[RevRecRawPricing5Min];
delete from  [GlobalTrading].[dbo].[RevRecRawPricingHourly];
"""

# I can't clear from six lines for some reason.
DeleteFromRaw2 = """
delete from  [GlobalTrading].[dbo].[RevRecPricing15Min];
delete from  [GlobalTrading].[dbo].[RevRecPricing5Min];
delete from  [GlobalTrading].[dbo].[RevRecPricingHourly];
"""

# Here is the mapping
SQLMapping = """
Select *
from [GlobalTrading].[dbo].[RevRecMapping]
"""

Cleaning = """
WITH CTE AS(
   SELECT *,
       RN = ROW_NUMBER()OVER(PARTITION BY Sap_contract, plant, offtaker, Market_day,time ORDER BY Sap_contract)
   FROM [GlobalTrading].dbo.[RevRecFinalPricing]
)
DELETE FROM CTE WHERE RN > 1;

"""

SqlForDataframe = """
select * from (
select 
[SAP_Contract],[Plant],[Offtaker]
      ,cast(replace([Object_ID_Node], '.','') as bigint) [Object_ID_Node]
      ,cast(replace([Object_ID_Hub], '.','') as bigint) [Object_ID_Hub] 
      ,[Market_Day],[Time],[Record_Progressive_ID],[Data_Type],[Agg_Level],
	  lead(node_price,1,0) over (partition by Sap_Contract, Plant, Offtaker order by market_day, Time asc) as Node_Price, 
	  lead([Hub_Price],1,0) over (partition by Sap_Contract, Plant, Offtaker order by market_day, Time asc) as [Hub_Price]
      from [GlobalTrading].dbo.[RevRecFinalPricing] ) as apple
	  where Market_Day = '{}'
order by 1,2,3,4,6,7
"""

##SQL Queries for pulling the 
SQLSetUPHour = """
SELECT  [TimeType]
      ,[NodeID]
  FROM [GlobalTrading].[dbo].[RevenueSetUpRAW]
  where TimeType = 'HOUR';
"""

SQLSetUP5Min = """
SELECT  [TimeType]
      ,[NodeID]
  FROM [GlobalTrading].[dbo].[RevenueSetUpRAW]
  where TimeType = '5MIN';
"""

SQLSetUP15Min = """
SELECT  [TimeType]
      ,[NodeID]
  FROM [GlobalTrading].[dbo].[RevenueSetUpRAW]
  where TimeType = '15MIN';
"""

conGT = pyodbc.connect(
            r'DRIVER={SQL Server};'
            r'SERVER=ANDVr-DAYZERDB1.enelint.global\DAYZER1;'
            r'DATABASE=GlobalTrading;'
            r'UID=YesEnergyUpdater;'
            r'PWD=Updater')

dtypesRaw = {
            'Datetime':sqlalchemy.DateTime
            ,'ObjectID':sqlalchemy.BigInteger	
            ,'dataType':sqlalchemy.NVARCHAR(10)
            ,'Agg_Level':sqlalchemy.NCHAR(10)
            ,'Avg_Value':sqlalchemy.NUMERIC(18,10)
            }

dict = {
        'DATETIME':'Datetime'
        , 'OBJECTID':'ObjectID'
        , 'DATATYPE':'dataType'
        ,'AGG_LEVEL':'Agg_Level'
        , 'AVGVALUE':'Avg_Value'
        }

SQLRawToSetupHourHE = """
MERGE [globaltrading].[dbo].[RevRecPricingHourly] Targ
USING 
(
select [Contract ID] [SAP_Contract],[SAP Plant ID]  [Plant],[SAP Customer ID] [Offtaker], [Object ID Node], [Object ID Hub]
,[Market_Day],[Time],[Time_Position],
[datatype] [Data_Type]	,[Agg_Level],sum([Node_Price]) [Node_Price],sum(Hub_Price) [Hub_Price] 
from (
-- This is the node part.
select  [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID], [Object ID Node] , [Object ID Hub]
,Cast(Cast(FORMAT(DATEADD(minute, -60,DATETIME), 'MM') as int)as varchar(2))+ FORMAT(DATEADD(minute, -60,DATETIME), 'dd') + FORMAT(DATEADD(minute, -60,DATETIME), 'yyyy') as Market_Day
,FORMAT(DATEADD(minute, -60,DATETIME), 'HH')  + FORMAT(DATEADD(minute, -60,DATETIME), 'mm') as Time
,(format(DATEADD(minute, -60,DATETIME), 'HH') + 1) as Time_Position
, [Agg_Level]
, map.[DATATYPE]
,  avg_Value Node_Price
, null Hub_price
from   [GlobalTrading].[dbo].[RevRecMapping] map
join globalTrading.dbo.[RevRecRawPricingHourly] node
on map.[Object ID Node] = node.OBJECTID and  node.[Agg_Level] = 'HOUR' and [Aggregation] = 'Hour' and map.datatype = node.datatype
union
-- this is the hub part
select [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID], [Object ID Node], [Object ID Hub]
,Cast(Cast(FORMAT(DATEADD(minute, -60,DATETIME), 'MM') as int)as varchar(2)) + FORMAT(DATEADD(minute, -60,DATETIME), 'dd') + FORMAT(DATEADD(minute, -60,DATETIME), 'yyyy') as Market_Day
,FORMAT(DATEADD(minute, -60,DATETIME), 'HH')  + FORMAT(DATEADD(minute, -60,DATETIME), 'mm') as Time
,(format(DATEADD(minute, -60,DATETIME), 'HH') + 1) as Time_Position
, [Agg_Level]
, map.[DATATYPE]
, null Node_Price
, [Avg_Value] Hub_price

from   [GlobalTrading].[dbo].[RevRecMapping] map
join globalTrading.dbo.[RevRecRawPricingHourly] Hub
on map.[Object ID Hub] = Hub.OBJECTID and  Hub.[Agg_Level] = 'HOUR' and [Aggregation] = 'Hour' and map.datatype = hub.datatype
) as Test
group by [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID] ,[Object ID Node] ,[Object ID Hub] ,[Market_Day],[Time],[Time_Position],[datatype], agg_level
) Source
ON Targ.[SAP_Contract] = Source.[SAP_Contract]
      and Targ.[Plant] = Source.[Plant]
      and Targ.[Offtaker] = Source.[Offtaker]
      and Targ.[Object_ID_Node] = Source.[Object ID Node]
      and Targ.[Object_ID_Hub] = Source.[Object ID Hub]
      and Targ.[Market_Day] = Source.[Market_Day]
      and Targ.[Time] = Source.[Time]
      and Targ.[Time_Position] = Source.[Time_Position]
      and Targ.[Data_Type] = Source.[Data_Type]
      and Targ.[Agg_Level] = Source.[Agg_Level]
 WHEN NOT MATCHED BY TARGET THEN
                           INSERT 
                           ([SAP_Contract]
								  ,[Plant]
								  ,[Offtaker]
								  ,[Object_ID_Node]
								  ,[Object_ID_Hub]
								  ,[Market_Day]
								  ,[Time]
								  ,[Time_Position]
								  ,[Data_Type]
								  ,[Agg_Level]
								  ,[Node_Price]
								  ,[Hub_Price])
                           VALUES
                          (Source.[SAP_Contract]
								  ,Source.[Plant]
								  ,Source.[Offtaker]
								  ,Source.[Object ID Node]
								  ,Source.[Object ID Hub]
								  ,Source.[Market_Day]
								  ,Source.[Time]
								  ,Source.[Time_Position]
								  ,Source.[Data_Type]
								  ,'Hour'
								  ,Source.[Node_Price]
								  ,Source.[Hub_Price]);

"""

SQLRawToSetup5minHE = """
MERGE [globaltrading].[dbo].[RevRecPricing5min] Targ
USING 
(

select [Contract ID] [SAP_Contract],[SAP Plant ID]  [Plant],[SAP Customer ID] [Offtaker], [Object ID Node], [Object ID Hub]
,[Market_Day],[Time],[Time_Position],
[datatype] [Data_Type]	,[Agg_Level],sum([Node_Price]) [Node_Price],sum(Hub_Price) [Hub_Price] 
from (
-- This is the node part.
select  [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID], [Object ID Node] , [Object ID Hub]
,Cast(Cast(FORMAT(DATEADD(minute, -5,DATETIME), 'MM') as int)as varchar(2)) + FORMAT(DATEADD(minute, -5,DATETIME), 'dd') + FORMAT(DATEADD(minute, -5,DATETIME), 'yyyy') as Market_Day
,FORMAT(DATEADD(minute, -5,DATETIME), 'HH')  + FORMAT(DATEADD(minute, -5,DATETIME), 'mm') as Time
,(format(DATEADD(minute, -5,DATETIME), 'HH')*12 + FORMAT(DATEADD(minute, -5,DATETIME), 'mm')/5) + 1 as Time_Position
, [Agg_Level]
, map.[DATATYPE]
,  avg_Value Node_Price
, null Hub_price
from   [GlobalTrading].[dbo].[RevRecMapping] map
join globalTrading.dbo.[RevRecRawPricing5min] node
on map.[Object ID Node] = node.OBJECTID and  node.[Agg_Level] = '5MIN' and [Aggregation] = '5min' and map.datatype = node.datatype
union
-- this is the hub part
select [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID], [Object ID Node], [Object ID Hub]
,Cast(Cast(FORMAT(DATEADD(minute, -5,DATETIME), 'MM') as int)as varchar(2)) + FORMAT(DATEADD(minute, -5,DATETIME), 'dd') + FORMAT(DATEADD(minute, -5,DATETIME), 'yyyy') as Market_Day
,FORMAT(DATEADD(minute, -5,DATETIME), 'HH')  + FORMAT(DATEADD(minute, -5,DATETIME), 'mm') as Time
,(format(DATEADD(minute, -5,DATETIME), 'HH')*12 + FORMAT(DATEADD(minute, -5,DATETIME), 'mm')/5) + 1 as Time_Position
, [Agg_Level]
, map.[DATATYPE]
, null Node_Price
, [Avg_Value] Hub_price

from   [GlobalTrading].[dbo].[RevRecMapping] map
join globalTrading.dbo.[RevRecRawPricing5min] Hub
on map.[Object ID Hub] = Hub.OBJECTID and  Hub.[Agg_Level] = '5MIN' and [Aggregation] = '5min' and map.datatype = hub.datatype
) as Test
group by [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID] ,[Object ID Node] ,[Object ID Hub] ,[Market_Day],[Time],[Time_Position],[datatype], agg_level
) Source
ON Targ.[SAP_Contract] = Source.[SAP_Contract]
      and Targ.[Plant] = Source.[Plant]
      and Targ.[Offtaker] = Source.[Offtaker]
      and Targ.[Object_ID_Node] = Source.[Object ID Node]
      and Targ.[Object_ID_Hub] = Source.[Object ID Hub]
      and Targ.[Market_Day] = Source.[Market_Day]
      and Targ.[Time] = Source.[Time]
      and Targ.[Time_Position] = Source.[Time_Position]
      and Targ.[Data_Type] = Source.[Data_Type]
      and Targ.[Agg_Level] = Source.[Agg_Level]
 WHEN NOT MATCHED BY TARGET THEN
                           INSERT 
                           ([SAP_Contract]
								  ,[Plant]
								  ,[Offtaker]
								  ,[Object_ID_Node]
								  ,[Object_ID_Hub]
								  ,[Market_Day]
								  ,[Time]
								  ,[Time_Position]
								  ,[Data_Type]
								  ,[Agg_Level]
								  ,[Node_Price]
								  ,[Hub_Price])
                           VALUES
                          (Source.[SAP_Contract]
								  ,Source.[Plant]
								  ,Source.[Offtaker]
								  ,Source.[Object ID Node]
								  ,Source.[Object ID Hub]
								  ,Source.[Market_Day]
								  ,Source.[Time]
								  ,Source.[Time_Position]
								  ,Source.[Data_Type]
								  ,'5Min'
								  ,Source.[Node_Price]
								  ,Source.[Hub_Price]);
"""

SQLRawToSetup15minHEErcot = """
MERGE [globaltrading].[dbo].[RevRecPricing15min] Targ
USING 
(select [Contract ID] [SAP_Contract],[SAP Plant ID]  [Plant],[SAP Customer ID] [Offtaker], [Object ID Node], [Object ID Hub]
,[Market_Day],[Time],[Time_Position],
[datatype] [Data_Type]	,[Agg_Level],sum([Node_Price]) [Node_Price],sum(Hub_Price) [Hub_Price] 
from (
-- This is the node part.
select  [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID], [Object ID Node] , [Object ID Hub]
,Cast(Cast(FORMAT(DATEADD(minute, -15,DATETIME), 'MM') as int)as varchar(2)) + FORMAT(DATEADD(minute, -15,DATETIME), 'dd') + FORMAT(DATEADD(minute, -15,DATETIME), 'yyyy') as Market_Day
,FORMAT(DATEADD(minute, -15,DATETIME), 'HH')  + FORMAT(DATEADD(minute, -15,DATETIME), 'mm') as Time
,(format(DATEADD(minute, -15,DATETIME), 'HH')*12 + FORMAT(DATEADD(minute, -15,DATETIME), 'mm')/15) + 1 as Time_Position
, [Agg_Level]
, map.[DATATYPE]
,  avg_Value Node_Price
, null Hub_price
from   [GlobalTrading].[dbo].[RevRecMapping] map
join globalTrading.dbo.[RevRecRawPricing15min] node
on map.[Object ID Node] = node.OBJECTID and  node.[Agg_Level] = '5MIN' and [Aggregation] = '15min' and map.datatype = node.datatype
union
-- this is the hub part
select [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID], [Object ID Node], [Object ID Hub]
,Cast(Cast(FORMAT(DATEADD(minute, -15,DATETIME), 'MM') as int)as varchar(2)) + FORMAT(DATEADD(minute, -15,DATETIME), 'dd') + FORMAT(DATEADD(minute, -15,DATETIME), 'yyyy') as Market_Day
,FORMAT(DATEADD(minute, -15,DATETIME), 'HH')  + FORMAT(DATEADD(minute, -15,DATETIME), 'mm') as Time
,(format(DATEADD(minute, -15,DATETIME), 'HH')*12 + FORMAT(DATEADD(minute, -15,DATETIME), 'mm')/15) + 1 as Time_Position
, [Agg_Level]
, map.[DATATYPE]
, null Node_Price
, [Avg_Value] Hub_price

from   [GlobalTrading].[dbo].[RevRecMapping] map
join globalTrading.dbo.[RevRecRawPricing15min] Hub
on map.[Object ID Hub] = Hub.OBJECTID and  Hub.[Agg_Level] = '5MIN' and [Aggregation] = '15min' and map.datatype = hub.datatype
) as Test
group by [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID] ,[Object ID Node] ,[Object ID Hub] ,[Market_Day],[Time],[Time_Position],[datatype], agg_level
) Source
ON Targ.[SAP_Contract] = Source.[SAP_Contract]
      and Targ.[Plant] = Source.[Plant]
      and Targ.[Offtaker] = Source.[Offtaker]
      and Targ.[Object_ID_Node] = Source.[Object ID Node]
      and Targ.[Object_ID_Hub] = Source.[Object ID Hub]
      and Targ.[Market_Day] = Source.[Market_Day]
      and Targ.[Time] = Source.[Time]
      and Targ.[Time_Position] = Source.[Time_Position]
      and Targ.[Data_Type] = Source.[Data_Type]
      and Targ.[Agg_Level] = Source.[Agg_Level]
 WHEN NOT MATCHED BY TARGET THEN
                           INSERT 
                           ([SAP_Contract]
								  ,[Plant]
								  ,[Offtaker]
								  ,[Object_ID_Node]
								  ,[Object_ID_Hub]
								  ,[Market_Day]
								  ,[Time]
								  ,[Time_Position]
								  ,[Data_Type]
								  ,[Agg_Level]
								  ,[Node_Price]
								  ,[Hub_Price])
                           VALUES
                          (Source.[SAP_Contract]
								  ,Source.[Plant]
								  ,Source.[Offtaker]
								  ,Source.[Object ID Node]
								  ,Source.[Object ID Hub]
								  ,Source.[Market_Day]
								  ,Source.[Time]
								  ,Source.[Time_Position]
								  ,Source.[Data_Type]
								  ,'15Min'
								  ,Source.[Node_Price]
								  ,Source.[Hub_Price]);
"""


# 15 minute section from raw to set up table
SQLRawToSetup15minHE = """
MERGE [globaltrading].[dbo].[RevRecPricing15min] Targ
USING 
(

select [Contract ID] [SAP_Contract],[SAP Plant ID]  [Plant],[SAP Customer ID] [Offtaker], [Object ID Node], [Object ID Hub]
,[Market_Day],[Time],[Time_Position],
[datatype] [Data_Type]	,[Agg_Level],sum([Node_Price]) [Node_Price],sum(Hub_Price) [Hub_Price] 
from (
-- This is the node part.
select  [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID], [Object ID Node] , [Object ID Hub]
,Cast(Cast(FORMAT(DATEADD(minute, -15,DATETIME), 'MM') as int)as varchar(2)) + FORMAT(DATEADD(minute, -15,DATETIME), 'dd') + FORMAT(DATEADD(minute, -15,DATETIME), 'yyyy') as Market_Day
,FORMAT(DATEADD(minute, -15,DATETIME), 'HH')  + FORMAT(DATEADD(minute, -15,DATETIME), 'mm') as Time
,(format(DATEADD(minute, -15,DATETIME), 'HH')*12 + FORMAT(DATEADD(minute, -15,DATETIME), 'mm')/5) + 1 as Time_Position
, [Agg_Level]
, map.[DATATYPE]
,  avg_Value Node_Price
, null Hub_price
from   [GlobalTrading].[dbo].[RevRecMapping] map
join globalTrading.dbo.[RevRecRawPricing15min] node
on map.[Object ID Node] = node.OBJECTID and  node.[Agg_Level] = '5MIN' and [Aggregation] = '15min' and map.datatype = node.datatype
union
-- this is the hub part
select [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID], [Object ID Node], [Object ID Hub]
,Cast(Cast(FORMAT(DATEADD(minute, -15,DATETIME), 'MM') as int)as varchar(2)) + FORMAT(DATEADD(minute, -15,DATETIME), 'dd') + FORMAT(DATEADD(minute, -15,DATETIME), 'yyyy') as Market_Day
,FORMAT(DATEADD(minute, -15,DATETIME), 'HH')  + FORMAT(DATEADD(minute, -15,DATETIME), 'mm') as Time
,(format(DATEADD(minute, -15,DATETIME), 'HH')*12 + FORMAT(DATEADD(minute, -15,DATETIME), 'mm')/5) + 1 as Time_Position
, [Agg_Level]
, map.[DATATYPE]
, null Node_Price
, [Avg_Value] Hub_price

from   [GlobalTrading].[dbo].[RevRecMapping] map
join globalTrading.dbo.[RevRecRawPricing15min] Hub
on map.[Object ID Hub] = Hub.OBJECTID and  Hub.[Agg_Level] = '5MIN' and [Aggregation] = '15min' and map.datatype = hub.datatype
) as Test
group by [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID] ,[Object ID Node] ,[Object ID Hub] ,[Market_Day],[Time],[Time_Position],[datatype], agg_level
) Source
ON Targ.[SAP_Contract] = Source.[SAP_Contract]
      and Targ.[Plant] = Source.[Plant]
      and Targ.[Offtaker] = Source.[Offtaker]
      and Targ.[Object_ID_Node] = Source.[Object ID Node]
      and Targ.[Object_ID_Hub] = Source.[Object ID Hub]
      and Targ.[Market_Day] = Source.[Market_Day]
      and Targ.[Time] = Source.[Time]
      and Targ.[Time_Position] = Source.[Time_Position]
      and Targ.[Data_Type] = Source.[Data_Type]
      and Targ.[Agg_Level] = Source.[Agg_Level]
 WHEN NOT MATCHED BY TARGET THEN
                           INSERT 
                           ([SAP_Contract]
								  ,[Plant]
								  ,[Offtaker]
								  ,[Object_ID_Node]
								  ,[Object_ID_Hub]
								  ,[Market_Day]
								  ,[Time]
								  ,[Time_Position]
								  ,[Data_Type]
								  ,[Agg_Level]
								  ,[Node_Price]
								  ,[Hub_Price])
                           VALUES
                          (Source.[SAP_Contract]
								  ,Source.[Plant]
								  ,Source.[Offtaker]
								  ,Source.[Object ID Node]
								  ,Source.[Object ID Hub]
								  ,Source.[Market_Day]
								  ,Source.[Time]
								  ,Source.[Time_Position]
								  ,Source.[Data_Type]
								  ,'15Min'
								  ,Source.[Node_Price]
								  ,Source.[Hub_Price]);
"""

SQLRawToSetupHourHB = """
MERGE [globaltrading].[dbo].[RevRecPricingHourly] Targ
USING 
(

select [Contract ID] [SAP_Contract],[SAP Plant ID]  [Plant],[SAP Customer ID] [Offtaker], [Object ID Node], [Object ID Hub]
,[Market_Day],[Time],[Time_Position],
[datatype] [Data_Type]	,[Agg_Level],sum([Node_Price]) [Node_Price],sum(Hub_Price) [Hub_Price] 
from (
-- This is the node part.
select  [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID], [Object ID Node] , [Object ID Hub]
,Cast(Cast(FORMAT(DATEADD(minute, 0,DATETIME), 'MM') as int)as varchar(2)) + FORMAT(DATEADD(minute, 0,DATETIME), 'dd') + FORMAT(DATEADD(minute, 0,DATETIME), 'yyyy') as Market_Day
,FORMAT(DATEADD(minute, 0,DATETIME), 'HH')  + FORMAT(DATEADD(minute, 0,DATETIME), 'mm') as Time
,(format(DATEADD(minute, 0,DATETIME), 'HH') + 1) as Time_Position
, [Agg_Level]
, map.[DATATYPE]
,  avg_Value Node_Price
, null Hub_price
from   [GlobalTrading].[dbo].[RevRecMapping] map
join globalTrading.dbo.[RevRecRawPricingHourly] node
on map.[Object ID Node] = node.OBJECTID and  node.[Agg_Level] = 'HOUR' and [Aggregation] = 'Hour' and map.datatype = node.datatype
union
-- this is the hub part
select [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID], [Object ID Node], [Object ID Hub]
,Cast(Cast(FORMAT(DATEADD(minute, 0,DATETIME), 'MM') as int)as varchar(2)) + FORMAT(DATEADD(minute, 0,DATETIME), 'dd') + FORMAT(DATEADD(minute, 0,DATETIME), 'yyyy') as Market_Day
,FORMAT(DATEADD(minute, 0,DATETIME), 'HH')  + FORMAT(DATEADD(minute, 0,DATETIME), 'mm') as Time
,(format(DATEADD(minute, 0,DATETIME), 'HH') + 1) as Time_Position
, [Agg_Level]
, map.[DATATYPE]
, null Node_Price
, [Avg_Value] Hub_price

from   [GlobalTrading].[dbo].[RevRecMapping] map
join globalTrading.dbo.[RevRecRawPricingHourly] Hub
on map.[Object ID Hub] = Hub.OBJECTID and  Hub.[Agg_Level] = 'HOUR' and [Aggregation] = 'Hour' and map.datatype = hub.datatype
) as Test
group by [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID] ,[Object ID Node] ,[Object ID Hub] ,[Market_Day],[Time],[Time_Position],[datatype], agg_level
) Source
ON Targ.[SAP_Contract] = Source.[SAP_Contract]
      and Targ.[Plant] = Source.[Plant]
      and Targ.[Offtaker] = Source.[Offtaker]
      and Targ.[Object_ID_Node] = Source.[Object ID Node]
      and Targ.[Object_ID_Hub] = Source.[Object ID Hub]
      and Targ.[Market_Day] = Source.[Market_Day]
      and Targ.[Time] = Source.[Time]
      and Targ.[Time_Position] = Source.[Time_Position]
      and Targ.[Data_Type] = Source.[Data_Type]
      and Targ.[Agg_Level] = Source.[Agg_Level]
 WHEN NOT MATCHED BY TARGET THEN
                           INSERT 
                           ([SAP_Contract]
								  ,[Plant]
								  ,[Offtaker]
								  ,[Object_ID_Node]
								  ,[Object_ID_Hub]
								  ,[Market_Day]
								  ,[Time]
								  ,[Time_Position]
								  ,[Data_Type]
								  ,[Agg_Level]
								  ,[Node_Price]
								  ,[Hub_Price])
                           VALUES
                          (Source.[SAP_Contract]
								  ,Source.[Plant]
								  ,Source.[Offtaker]
								  ,Source.[Object ID Node]
								  ,Source.[Object ID Hub]
								  ,Source.[Market_Day]
								  ,Source.[Time]
								  ,Source.[Time_Position]
								  ,Source.[Data_Type]
								  ,'Hour'
								  ,Source.[Node_Price]
								  ,Source.[Hub_Price]);
"""

SQLRawToSetup5minHB = """
MERGE [globaltrading].[dbo].[RevRecPricing5min] Targ
USING 
(

select [Contract ID] [SAP_Contract],[SAP Plant ID]  [Plant],[SAP Customer ID] [Offtaker], [Object ID Node], [Object ID Hub]
,[Market_Day],[Time],[Time_Position],
[datatype] [Data_Type]	,[Agg_Level],sum([Node_Price]) [Node_Price],sum(Hub_Price) [Hub_Price] 
from (
-- This is the node part.
select  [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID], [Object ID Node] , [Object ID Hub]
,Cast(Cast(FORMAT(DATEADD(minute, 0,DATETIME), 'MM') as int)as varchar(2)) + FORMAT(DATEADD(minute, 0,DATETIME), 'dd') + FORMAT(DATEADD(minute, 0,DATETIME), 'yyyy') as Market_Day
,FORMAT(DATEADD(minute, -60,DATETIME), 'HH')  + FORMAT(DATEADD(minute, 0,DATETIME), 'mm') as Time
,(format(DATEADD(minute, 0,DATETIME), 'HH')*12 + FORMAT(DATEADD(minute, 0,DATETIME), 'mm')/5) + 1 as Time_Position
, [Agg_Level]
, map.[DATATYPE]
,  avg_Value Node_Price
, null Hub_price
from   [GlobalTrading].[dbo].[RevRecMapping] map
join globalTrading.dbo.[RevRecRawPricing5min] node
on map.[Object ID Node] = node.OBJECTID and  node.[Agg_Level] = '5MIN' and [Aggregation] = '5min' and map.datatype = node.datatype
union
-- this is the hub part
select [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID], [Object ID Node], [Object ID Hub]
,Cast(Cast(FORMAT(DATEADD(minute, 0,DATETIME), 'MM') as int)as varchar(2)) + FORMAT(DATEADD(minute, 0,DATETIME), 'dd') + FORMAT(DATEADD(minute, 0,DATETIME), 'yyyy') as Market_Day
,FORMAT(DATEADD(minute, 0,DATETIME), 'HH')  + FORMAT(DATEADD(minute, 0,DATETIME), 'mm') as Time
,(format(DATEADD(minute, 0,DATETIME), 'HH')*12 + FORMAT(DATEADD(minute, 0,DATETIME), 'mm')/5) + 1 as Time_Position
, [Agg_Level]
, map.[DATATYPE]
, null Node_Price
, [Avg_Value] Hub_price

from   [GlobalTrading].[dbo].[RevRecMapping] map
join globalTrading.dbo.[RevRecRawPricing5min] Hub
on map.[Object ID Hub] = Hub.OBJECTID and  Hub.[Agg_Level] = '5MIN' and [Aggregation] = '5min' and map.datatype = hub.datatype
) as Test
group by [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID] ,[Object ID Node] ,[Object ID Hub] ,[Market_Day],[Time],[Time_Position],[datatype], agg_level
) Source
ON Targ.[SAP_Contract] = Source.[SAP_Contract]
      and Targ.[Plant] = Source.[Plant]
      and Targ.[Offtaker] = Source.[Offtaker]
      and Targ.[Object_ID_Node] = Source.[Object ID Node]
      and Targ.[Object_ID_Hub] = Source.[Object ID Hub]
      and Targ.[Market_Day] = Source.[Market_Day]
      and Targ.[Time] = Source.[Time]
      and Targ.[Time_Position] = Source.[Time_Position]
      and Targ.[Data_Type] = Source.[Data_Type]
      and Targ.[Agg_Level] = Source.[Agg_Level]
 WHEN NOT MATCHED BY TARGET THEN
                           INSERT 
                           ([SAP_Contract]
								  ,[Plant]
								  ,[Offtaker]
								  ,[Object_ID_Node]
								  ,[Object_ID_Hub]
								  ,[Market_Day]
								  ,[Time]
								  ,[Time_Position]
								  ,[Data_Type]
								  ,[Agg_Level]
								  ,[Node_Price]
								  ,[Hub_Price])
                           VALUES
                          (Source.[SAP_Contract]
								  ,Source.[Plant]
								  ,Source.[Offtaker]
								  ,Source.[Object ID Node]
								  ,Source.[Object ID Hub]
								  ,Source.[Market_Day]
								  ,Source.[Time]
								  ,Source.[Time_Position]
								  ,Source.[Data_Type]
								  ,'5Min'
								  ,Source.[Node_Price]
								  ,Source.[Hub_Price]);
"""
SQLRawToSetup15minHB = """

MERGE [globaltrading].[dbo].[RevRecPricing15min] Targ
USING 
(

select [Contract ID] [SAP_Contract],[SAP Plant ID]  [Plant],[SAP Customer ID] [Offtaker], [Object ID Node], [Object ID Hub]
,[Market_Day],[Time],[Time_Position],
[datatype] [Data_Type]	,[Agg_Level],sum([Node_Price]) [Node_Price],sum(Hub_Price) [Hub_Price] 
from (
-- This is the node part.
select  [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID], [Object ID Node] , [Object ID Hub]
,Cast(Cast(FORMAT(DATEADD(minute, 0,DATETIME), 'MM') as int)as varchar(2)) + FORMAT(DATEADD(minute, 0,DATETIME), 'dd') + FORMAT(DATEADD(minute, 0,DATETIME), 'yyyy') as Market_Day
,FORMAT(DATEADD(minute, 0,DATETIME), 'HH')  + FORMAT(DATEADD(minute, 0,DATETIME), 'mm') as Time
,(format(DATEADD(minute, 0,DATETIME), 'HH')*12 + FORMAT(DATEADD(minute, 0,DATETIME), 'mm')/5) + 1 as Time_Position
, [Agg_Level]
, map.[DATATYPE]
,  avg_Value Node_Price
, null Hub_price
from   [GlobalTrading].[dbo].[RevRecMapping] map
join globalTrading.dbo.[RevRecRawPricing15min] node
on map.[Object ID Node] = node.OBJECTID and  node.[Agg_Level] = '5MIN' and [Aggregation] = '15min' and map.datatype = node.datatype
union
-- this is the hub part
select [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID], [Object ID Node], [Object ID Hub]
,Cast(Cast(FORMAT(DATEADD(minute, 0,DATETIME), 'MM') as int)as varchar(2)) + FORMAT([DATETIME], 'dd') + FORMAT(DATEADD(minute, 0,DATETIME), 'yyyy') as Market_Day
,FORMAT(DATEADD(minute, 0,DATETIME), 'HH')  + FORMAT(DATEADD(minute, 0,DATETIME), 'mm') as Time
,(format([Datetime], 'HH')*12 + FORMAT([DATETIME], 'mm')/5) + 1 as Time_Position
, [Agg_Level]
, map.[DATATYPE]
, null Node_Price
, [Avg_Value] Hub_price

from   [GlobalTrading].[dbo].[RevRecMapping] map
join globalTrading.dbo.[RevRecRawPricing15min] Hub
on map.[Object ID Hub] = Hub.OBJECTID and  Hub.[Agg_Level] = '5MIN' and [Aggregation] = '15min' and map.datatype = hub.datatype
) as Test
group by [Contract ID] ,[SAP Plant ID] ,[SAP Customer ID] ,[Object ID Node] ,[Object ID Hub] ,[Market_Day],[Time],[Time_Position],[datatype], agg_level
) Source
ON Targ.[SAP_Contract] = Source.[SAP_Contract]
      and Targ.[Plant] = Source.[Plant]
      and Targ.[Offtaker] = Source.[Offtaker]
      and Targ.[Object_ID_Node] = Source.[Object ID Node]
      and Targ.[Object_ID_Hub] = Source.[Object ID Hub]
      and Targ.[Market_Day] = Source.[Market_Day]
      and Targ.[Time] = Source.[Time]
      and Targ.[Time_Position] = Source.[Time_Position]
      and Targ.[Data_Type] = Source.[Data_Type]
      and Targ.[Agg_Level] = Source.[Agg_Level]
 WHEN NOT MATCHED BY TARGET THEN
                           INSERT 
                           ([SAP_Contract]
								  ,[Plant]
								  ,[Offtaker]
								  ,[Object_ID_Node]
								  ,[Object_ID_Hub]
								  ,[Market_Day]
								  ,[Time]
								  ,[Time_Position]
								  ,[Data_Type]
								  ,[Agg_Level]
								  ,[Node_Price]
								  ,[Hub_Price])
                           VALUES
                          (Source.[SAP_Contract]
								  ,Source.[Plant]
								  ,Source.[Offtaker]
								  ,Source.[Object ID Node]
								  ,Source.[Object ID Hub]
								  ,Source.[Market_Day]
								  ,Source.[Time]
								  ,Source.[Time_Position]
								  ,Source.[Data_Type]
								  ,'15Min'
								  ,Source.[Node_Price]
								  ,Source.[Hub_Price]);


"""


sql_merge_Final = """
MERGE [globaltrading].[dbo].[RevRecFinalPricing] Targ
Using(
select [SAP_Contract],[Plant],[Offtaker],[Object_ID_Node],[Object_ID_Hub]
      ,min([Market_Day]) Market_Day,min([Time]) Time
      ,[Time_Position],[Data_Type],[Agg_Level]
      ,avg([Node_Price]) Node_Price,avg([Hub_Price]) Hub_Price
	  from
(SELECT  
[SAP_Contract],[Plant],[Offtaker],[Object_ID_Node],[Object_ID_Hub],Market_Day,[Time]
	  ,floor(([Time_Position]-1)/3+1) Time_Position
      ,[Data_Type],[Agg_Level],avg([Node_Price]) Node_Price,avg([Hub_Price]) Hub_Price
FROM [GlobalTrading].[dbo].[RevRecPricing15Min]
group by [SAP_Contract],[Plant],[Offtaker],[Object_ID_Node],[Object_ID_Hub]
      ,Market_Day,[Time_Position],[Time],[Data_Type],[Agg_Level]) A
group by [SAP_Contract],[Plant],[Offtaker],[Object_ID_Node]
      ,[Object_ID_Hub],market_day,[Time_Position],[Data_Type],[Agg_Level]) Source
On
Targ.[SAP_Contract] = Source.[SAP_Contract]
      and Targ.[Market_Day] = Source.Market_Day
      and Targ.[Time] = Source.Time
      and Targ.[Time] = Source.Time

 WHEN NOT MATCHED BY TARGET THEN
INSERT 
(
[SAP_Contract]
      ,[Plant]
      ,[Offtaker]
      ,[Object_ID_Node]
      ,[Object_ID_Hub]
      ,[Market_Day]
      ,[Time]
      ,[Record_Progressive_ID]
      ,[Data_Type]
      ,[Agg_Level]
      ,[Node_Price]
      ,[Hub_Price]
)
Values
(Source.[SAP_Contract]
      ,Source.[Plant]
      ,Source.[Offtaker]
      ,Source.[Object_ID_Node]
      ,Source.[Object_ID_Hub]
      ,Source.[Market_Day]
      ,Source.[Time]
      ,Source.[Time_Position]
      ,Source.[Data_Type]
      ,'15Min'
      ,Source.[Node_Price]
      ,Source.[Hub_Price]);

MERGE [globaltrading].[dbo].[RevRecFinalPricing] Targ
Using(
select * from globalTrading.dbo.revrecpricing5Min) Source
On
Targ.[SAP_Contract] = Source.[SAP_Contract]
      and Targ.[Market_Day] = Source.Market_Day
      and Targ.[Time] = Source.Time
 WHEN NOT MATCHED BY TARGET THEN
INSERT 
(
[SAP_Contract]
      ,[Plant]
      ,[Offtaker]
      ,[Object_ID_Node]
      ,[Object_ID_Hub]
      ,[Market_Day]
      ,[Time]
      ,[Record_Progressive_ID]
      ,[Data_Type]
      ,[Agg_Level]
      ,[Node_Price]
      ,[Hub_Price]
)
Values
(
Source.[SAP_Contract]
      ,Source.[Plant]
      ,Source.[Offtaker]
      ,Source.[Object_ID_Node]
      ,Source.[Object_ID_Hub]
      ,Source.[Market_Day]
      ,Source.[Time]
      ,Source.[Time_Position]
      ,Source.[Data_Type]
      ,Source.[Agg_Level]
      ,Source.[Node_Price]
      ,Source.[Hub_Price]

);

MERGE [globaltrading].[dbo].[RevRecFinalPricing] Targ
Using(
select * from globalTrading.dbo.revrecpricingHourly) Source
On
Targ.[SAP_Contract] = Source.[SAP_Contract]
      and Targ.[Market_Day] = Source.Market_Day
      and Targ.[Time] = Source.Time
 WHEN NOT MATCHED BY TARGET THEN
INSERT 
(
[SAP_Contract]
      ,[Plant]
      ,[Offtaker]
      ,[Object_ID_Node]
      ,[Object_ID_Hub]
      ,[Market_Day]
      ,[Time]
      ,[Record_Progressive_ID]
      ,[Data_Type]
      ,[Agg_Level]
      ,[Node_Price]
      ,[Hub_Price]

)
Values
(
Source.[SAP_Contract]
      ,Source.[Plant]
      ,Source.[Offtaker]
      ,Source.[Object_ID_Node]
      ,Source.[Object_ID_Hub]
      ,Source.[Market_Day]
      ,Source.[Time]
      ,Source.[Time_Position]
      ,Source.[Data_Type]
      ,Source.[Agg_Level]
      ,Source.[Node_Price]
      ,Source.[Hub_Price]

);
"""


#==============================================================================
def db_connect():
    """Pass nothing. Returns Pyodbc connection object. Call this function to connect to hardcoded database."""
    print('Connecting to GlobalTrading database...')
    con = None
    try:
        #  Hardcoded details for SQL server connection including username and password
        con = pyodbc.connect(
            r'DRIVER={SQL Server};'
            r'SERVER=ANDVr-DAYZERDB1\DAYZER1;'
            r'DATABASE=GlobalTrading;'
            r'UID=YesEnergyUpdater;'
            r'PWD=Updater')
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
#==============================================================================
#==============================================================================
def testForSpeed(UserPassCombo):
    global tic
    global toc
    
    if UserPassCombo == 0:
        toc = time.clock()
        TimeToRest = max(9 - (toc -tic),1)
        print('Pause {}s due to limits'.format(TimeToRest))
        time.sleep(TimeToRest)
        tic = time.clock()




UserPassCombo = 0


api5min = pd.read_sql(SQLSetUP5Min, conGT)
apiHour = pd.read_sql(SQLSetUPHour, conGT)
api15min = pd.read_sql(SQLSetUP15Min, conGT)

db_exec(db_connect(), DeleteFromRaw1, commit=1)
db_exec(db_connect(), DeleteFromRaw2, commit=1)

# =============================================================================
# pd.read_sql(DeleteFromRaw, conGT)
# 
# =============================================================================

tic = time.clock()
toc = time.clock()
dfHour = []
df5Min = []
df15Min = []
toc = time.clock()

print('Test2')
SettingUp = pd.read_sql(SQLMapping, conGT)

for SetNumber in range(len(SettingUp)):
    print(SetNumber)
    PhaseQ = SettingUp['Contract ID'][SetNumber]
    DataType = SettingUp['DATATYPE'][SetNumber]
    NodeID = SettingUp['Object ID Node'][SetNumber]
    HubID = SettingUp['Object ID Hub'][SetNumber]
    Aggregation = SettingUp['Aggregation'][SetNumber]
    Interval = SettingUp['Interval'][SetNumber]
    dfToSQLHub = []
    dfToSQLNode = []

    
    #This only works if there is one 4000 object Hub ID
    if np.isnan(HubID) == False:
        if int(HubID)== 4000:
            HubID = int(HubID)
            db_exec(db_connect(), sql_merge_Final, commit=1)
            db_exec(db_connect(), DeleteFromRaw1, commit=1)
            db_exec(db_connect(), DeleteFromRaw2, commit=1)
            TimeFrameForURL = '5min'
            forURL = 'https://services.yesenergy.com/PS/rest/timeseries/'+DataType+'/'+ str(HubID)+'.csv?agglevel='+TimeFrameForURL+'&startdate='+STARTDATE+'&enddate='+ENDDATE+ '&cols=1,3,5,6,9'
            my_auth = (my_auth_User[UserPassCombo],my_auth_Pass[UserPassCombo])
            UserPassCombo = (UserPassCombo + 1) % 5
            url = requests.get(forURL, auth = my_auth)
            df1 = pd.read_csv (StringIO(url.text))
            dfToSQLHub.append(df1)
            dfReadyHub = pd.concat(dfToSQLHub) 
            dfReadyHub.columns = list(dict.values())
            dfReadyHub.to_sql('RevRecRawPricing15Min', engine, if_exists = 'append', index = False, dtype = dtypesRaw)
            db_exec(db_connect(),SQLRawToSetup15minHE , commit=1)
            db_exec(db_connect(), sql_merge_Final, commit=1)
            db_exec(db_connect(), DeleteFromRaw1, commit=1)
            db_exec(db_connect(), DeleteFromRaw2, commit=1)
            print('Test2')
            continue
     
    #https://services.yesenergy.com/PS/rest/timeseries/RT_LMP_ERCOT_BEST/10000698546?startdate=today-71&enddate=today-41&agglevel=5min
    #This checks if Phase 2 is present
    if PhaseQ == 'Phase 2':
        continue
    
    # Hub ID Problem
    if np.isnan(HubID) == False:
        HubID = int(HubID)
        if Aggregation == 'Hour':
            TimeFrameForURL = 'Hour'
            forURL = 'https://services.yesenergy.com/PS/rest/timeseries/'+DataType+'/'+ str(HubID)+'.csv?agglevel='+TimeFrameForURL+'&startdate='+STARTDATE+'&enddate='+ENDDATE+ '&cols=1,3,5,6,9'
        elif Aggregation == '5min':
            TimeFrameForURL = '5min'
            forURL = 'https://services.yesenergy.com/PS/rest/timeseries/'+DataType+'/'+ str(HubID)+'.csv?agglevel='+TimeFrameForURL+'&startdate='+STARTDATE+'&enddate='+ENDDATE+ '&cols=1,3,5,6,9'
        elif Aggregation == '15min':
            TimeFrameForURL = '5min'
            forURL = 'https://services.yesenergy.com/PS/rest/timeseries/RT_LMP_ERCOT_BEST/'+ str(HubID)+'.csv?agglevel='+TimeFrameForURL+'&startdate='+STARTDATE+'&enddate='+ENDDATE+ '&cols=1,3,5,6,9'
        else:
            print('blah'+ Aggregation+'1')
        my_auth = (my_auth_User[UserPassCombo],my_auth_Pass[UserPassCombo])
        UserPassCombo = (UserPassCombo + 1) % 5
        url = requests.get(forURL, auth = my_auth)
        df1 = pd.read_csv (StringIO(url.text))
        dfToSQLHub.append(df1)
        dfReadyHub = pd.concat(dfToSQLHub) 
        
        print('Starting Upload for Real time Information with TimeFrame {} {} '.format(STARTDATE,ENDDATE))
        dfReadyHub.columns = list(dict.values())
        if Aggregation == 'Hour':
            dfReadyHub.to_sql('RevRecRawPricingHourly', engine, if_exists = 'append', index = False, dtype = dtypesRaw)
        if Aggregation == '5min':
            dfReadyHub.to_sql('RevRecRawPricing5Min', engine, if_exists = 'append', index = False, dtype = dtypesRaw)
        if Aggregation == '15min':
            dfReadyHub['dataType'] = 'RTLMP'
            dfReadyHub.to_sql('RevRecRawPricing15Min', engine, if_exists = 'append', index = False, dtype = dtypesRaw)
        testForSpeed(UserPassCombo)
   
    if np.isnan(NodeID) == False:
        NodeID = int(NodeID)
        if Aggregation == 'Hour':
            TimeFrameForURL = 'Hour'
            forURL = 'https://services.yesenergy.com/PS/rest/timeseries/'+DataType+'/'+ str(NodeID)+'.csv?agglevel='+TimeFrameForURL+'&startdate='+STARTDATE+'&enddate='+ENDDATE+ '&cols=1,3,5,6,9'
        if Aggregation == '5min':
            TimeFrameForURL = '5min'
            forURL = 'https://services.yesenergy.com/PS/rest/timeseries/'+DataType+'/'+ str(NodeID)+'.csv?agglevel='+TimeFrameForURL+'&startdate='+STARTDATE+'&enddate='+ENDDATE+ '&cols=1,3,5,6,9'
        if Aggregation == '15min':
            TimeFrameForURL = '5min'
            forURL = 'https://services.yesenergy.com/PS/rest/timeseries/RT_LMP_ERCOT_BEST/'+ str(NodeID)+'.csv?agglevel='+TimeFrameForURL+'&startdate='+STARTDATE+'&enddate='+ENDDATE+ '&cols=1,3,5,6,9'
        my_auth = (my_auth_User[UserPassCombo],my_auth_Pass[UserPassCombo])
        UserPassCombo = (UserPassCombo + 1) % 5
        url = requests.get(forURL, auth = my_auth)
        df2 = pd.read_csv (StringIO(url.text))
        dfToSQLNode.append(df2)
        dfReadyNode = pd.concat(dfToSQLNode) 
        
        print('Starting Upload for Real time Information with TimeFrame {} {} '.format(STARTDATE,ENDDATE))
        
        
        dfReadyNode.columns = list(dict.values())
        if Aggregation == 'Hour':
            dfReadyNode.to_sql('RevRecRawPricingHourly', engine, if_exists = 'append', index = False, dtype = dtypesRaw)
        if Aggregation == '5min':
            dfReadyNode.to_sql('RevRecRawPricing5Min', engine, if_exists = 'append', index = False, dtype = dtypesRaw)
        if Aggregation == '15min':
            dfReadyNode['dataType'] = 'RTLMP'
            dfReadyNode.to_sql('RevRecRawPricing15Min', engine, if_exists = 'append', index = False, dtype = dtypesRaw)
        testForSpeed(UserPassCombo)
    
    print('Run Start.complete')
    #Working on this part and making sure that this all works.



db_exec(db_connect(),SQLRawToSetupHourHE , commit=1)
db_exec(db_connect(), SQLRawToSetup5minHE, commit=1)
db_exec(db_connect(),SQLRawToSetup15minHEErcot , commit=1)
db_exec(db_connect(),SQLRawToSetupHourHB , commit=1)
db_exec(db_connect(),SQLRawToSetup5minHB , commit=1)
db_exec(db_connect(), SQLRawToSetup15minHB, commit=1)

# This part goes from the second step to the final step.
db_exec(db_connect(), sql_merge_Final, commit=1)    

# #==============================================================================
# # 3 Steps
# # Create a date function
# # Sql to Dataframe
# # DataFrame to csv
# #==============================================================================

TimeFrame = 1
TimeShift = date.today() - timedelta(TimeFrame)
forUpload = TimeShift.strftime("%Y%m%d")
ForSqlPull = TimeShift.strftime("%m%d%Y")

MonthPart = str(int(TimeShift.strftime("%m")))
OtherPart = TimeShift.strftime("%d%Y")
FullParts = MonthPart + OtherPart 

db_exec(db_connect(), Cleaning, commit=1)  

SqlForDataframe.format(ForSqlPull)
fileLocation = r"C:\Users\mgold\Python\RevRec\Files\yesenergy_daily_"+forUpload+"0320.txt"
pdSqlToPanda = pd.read_sql(SqlForDataframe.format(FullParts), conGT)
## I need to modify the panda datatype from float to integer. Hmmmm
#pdSqlToPanda['Object_ID_Node'] = pdSqlToPanda['Object_ID_Node'].astype(float)

############
# I need to convert the information in the thing to a str if there is a value and I need to
# return a null value but I need to convert to a str so that the types are the same.
np.where(pdSqlToPanda['Object_ID_Node'] is None,1,0)
pdSqlToPanda['Object_ID_Node'] = pdSqlToPanda['Object_ID_Node'].astype(str)
pdSqlToPanda['Object_ID_Hub'] = pdSqlToPanda['Object_ID_Hub'].astype(str)
pdSqlToPanda['Node_Price'] = pdSqlToPanda['Node_Price'].astype(str)
pdSqlToPanda['Hub_Price'] = pdSqlToPanda['Hub_Price'].astype(str)
pdSqlToPanda['Object_ID_Node'] = pdSqlToPanda['Object_ID_Node'].map(lambda x: x.replace('.0',''))
pdSqlToPanda['Object_ID_Node'] = pdSqlToPanda['Object_ID_Node'].map(lambda x: x.replace('nan',''))
pdSqlToPanda['Object_ID_Hub'] = pdSqlToPanda['Object_ID_Hub'].map(lambda x: x.replace('.0',''))
pdSqlToPanda['Object_ID_Hub'] = pdSqlToPanda['Object_ID_Hub'].map(lambda x: x.replace('nan',''))
pdSqlToPanda['Node_Price'] = pdSqlToPanda['Node_Price'].map(lambda x: '{:.5f}'.format(float(x)) )
pdSqlToPanda['Hub_Price'] = pdSqlToPanda['Hub_Price'].map(lambda x: '{:.5f}'.format(float(x)) )
pdSqlToPanda['Node_Price'] = pdSqlToPanda['Node_Price'].map(lambda x: x.replace('nan',''))
pdSqlToPanda['Hub_Price'] = pdSqlToPanda['Hub_Price'].map(lambda x: x.replace('nan',''))
#####################

os.chdir(r'C:\Users\mgold\Python\RevRec\Files')
pdSqlToPanda.to_csv(fileLocation, sep=';', index = False)


# #==============================================================================
# # df.to_csv('TestFileForPost.csv', sep=',', index = False)
# #==============================================================================
##  Modifacations below will have the local path become a variable so that the
# File name can be changed.
print('File to Server')
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None
srv = pysftp.Connection(host='10.154.78.225',username="ftpalterix2", private_key=r"C:\Users\mgold\Python\RevRec\PrivateKey\YesEnergy.pem", cnopts=cnopts)
srv.put(localpath=fileLocation, remotepath='/dati/YesEnergy/yesenergy_daily_'+forUpload+'0320.txt')
print('Finished uploading File to server')    

# =============================================================================
#Record_Progressive_ID
