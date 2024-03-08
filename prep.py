############
# Packages #
############
import os, urllib.request, json, sys, re
from postgrs import postgres


#############
# Endpoints #
#############
apiendpoint = 'https://data.gov.au/data/api/3/action/datastore_search?resource_id=f75cd60f-c812-4b1f-b796-f25cd8a85020'
fulldata_apiendpoint = r'https://data.gov.au/data/api/3/action/datastore_search_sql?sql=SELECT%20*%20from%20%22f75cd60f-c812-4b1f-b796-f25cd8a85020%22'
apiendpoint = fulldata_apiendpoint

#################
# Call Endpoint #
#################
print("fetching data from api ...")
response = urllib.request.urlopen(apiendpoint)
if str(response.status) != '200':
    print("API Call  failed with status", response.status)
    sys.exit(-1)

##################
# Store Response #
##################
apidata = json.load(response)['result']['records']

#######################
# Store Required Data #
#######################
disaster_assist = {}
for datarow in apidata:
    eachrow = {}
    keys = None
    for key in datarow:
        if key == '_id':
            keys = datarow[key]
        elif str(key).lower() not in ('location_code','location_name','location_type','state','event_name','agrn','metric_name','attribute','unit'): 
            continue
        else:
            eachrow[key] = datarow[key]
    
    disaster_assist[keys] = eachrow

##############
# Clean Data #
##############
for id, event in disaster_assist.items():
    for key,value in event.items():
        if key == 'attribute':
            pre_mod = event[key]
            # Regular expression pattern to match words
            post_mod_words = re.findall(r'\b\w+\b', pre_mod)
            event[key] = " ".join(post_mod_words[:2])

####################
# Group Event Data #
####################
event_data = {}
rowcounter = 0
for id, event in disaster_assist.items():
    event_name = agrn = locationcode = metric_name = attribute = unit = ''
    innerlist = []
    for key,value in event.items():
        if key =='event_name':
            event_name = value
        elif key =='location_code':
            if value == 'NULL':
                locationcode = 'unknown'
            else:
                locationcode = value
        elif key == 'agrn':
            agrn = value
        elif key == 'metric_name':
            metric_name = value
        elif key == 'attribute':
            attribute = value
        elif key == 'unit':
            unit = value
        else:
            pass
    innerlist = [event_name,locationcode,agrn,metric_name,attribute,unit]
    if innerlist in event_data.values():
        continue
    rowcounter+=1
    event_data[rowcounter] = innerlist

#######################
# Group location Data #
#######################     
location_data = {}
rowcounter = 0
for id, event in disaster_assist.items():
    location_code = location_name = location_type = state = ''
    innerlist = []
    for key,value in event.items():
        if key =='location_code':
            if value == 'NULL':
                location_code = 'unknown'
            else:
                location_code = value
        elif key == 'location_name':
            location_name = str(value).replace("'","''")
        elif key == 'location_type':
            location_type = value
        elif key == 'state':
            state = value
        else:
            pass
    innerlist = [location_code, location_name, location_type, state]
    if innerlist in location_data.values():
        continue
    rowcounter+=1
    location_data[rowcounter] = innerlist
    
   
###############
# DDL QUERIES #
###############
drop_location_table = "DROP TABLE IF EXISTS LOC;"
drop_event_table = "DROP TABLE IF EXISTS EVENTS;"
create_location_table = "CREATE TABLE IF NOT EXISTS LOC (LOCATION_CODE VARCHAR(40), LOCATION_NAME VARCHAR(40),LOCATION_TYPE VARCHAR(40), LOCATION_STATE VARCHAR(40), CONSTRAINT PK_LOCATION_CODE PRIMARY KEY (LOCATION_CODE));"
create_event_table = "CREATE TABLE IF NOT EXISTS EVENTS (ID SERIAL, EVENT_NAME VARCHAR(500), LOCATION_CODE VARCHAR(40), ARGN INTEGER, ATTRIBUTE VARCHAR(500), UNIT VARCHAR(40), CONSTRAINT PK_ID PRIMARY KEY (ID));"
connect_event_location = "ALTER TABLE EVENTS ADD CONSTRAINT FK_EVENT_2_LOCATION FOREIGN KEY (LOCATION_CODE) REFERENCES LOC (LOCATION_CODE)"
print("building tables ...")
postgres().execute_query(drop_event_table)
postgres().execute_query(drop_location_table)
postgres().execute_query(create_location_table)
postgres().execute_query(create_event_table)
postgres().execute_query(connect_event_location)
print("executing queries ...")

###############
# INSERT DATA #
###############
batch_query=[]
for row,values in location_data.items():
    stmt = f"INSERT INTO LOC (LOCATION_CODE,LOCATION_NAME,LOCATION_TYPE,LOCATION_STATE) VALUES ('{values[0]}','{values[1]}','{values[2]}','{values[3]}');"
    batch_query.append(stmt)

for eachquery in batch_query:
    try:
        postgres().execute_query(eachquery)
    except Exception as e:
        print("[POSTGRES] [INFO]: ",eachquery)
        print("[POSTGRES] [WARN]: Empty Query")
        continue

batch_query=[]
for row,values in event_data.items():
    stmt = f"INSERT INTO EVENTS (EVENT_NAME,LOCATION_CODE,ARGN,ATTRIBUTE,UNIT) VALUES ('{values[0]}','{values[1]}','{values[2]}','{values[3]}','{values[4]}');"
    batch_query.append(stmt)

for eachquery in batch_query:
    try:
        postgres().execute_query(eachquery)
    except Exception as e:
        print("[POSTGRES] [WARN]: Empty Query")
        continue


print("Data is fetched and stored in database.")