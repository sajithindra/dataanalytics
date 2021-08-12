from pymongo import MongoClient
import pandas as pd
pd.set_option("display.max_rows",None,"display.max_columns",None)
from datetime import time
import activeorg
server = MongoClient('mongodb://localhost:27017')
db=server['salesfokuz_lead']
leadsactivity = db['lead_log']
usercoll=db['users']
#################### pipeline ###################
livepipeline = [
{
        "$addFields": {
            "completed_date": {
                "$toString": "$data.completed_date"
            }}
    },
    {
        "$addFields": {
            "organization_id": {
                "$toString": "$data.organization_id"
            }
        }
    },

{
        '$project': {
            '_id': 0,
            "organization_id":1,
            'lead_id':1,
            'user_id': 1,
            "module": 1,
            "action": 1,
            "completed_date":1,

            }
    }

]
##################   END #######################

################## aggregating data from mongodb using the pipeline #############
leadlogdata = list(leadsactivity.aggregate(livepipeline))
##################   END #######################
df = pd.DataFrame(leadlogdata)
################## taking active organizations by using active module########
leaddf = activeorg.active(df)
################## end #######################
leaddf=leaddf[leaddf.module == 'call']                                #filtering module to take call
leaddf=leaddf[leaddf.action == 'Completed the']                       #filtering action to take completed calls
leaddf['completed_time'] = pd.to_datetime(leaddf['completed_date']).dt.strftime('%H')      #convert time into hour
leaddf['completed_time']=leaddf['completed_time'].astype(int)      #convert time into integer
###################### filtering the completed time by conditions for the classification #################
b = [0,4,8,12,16,20,24]
l = ['Late_Night', 'Early_Morning','Morning','Noon','Eve','Night']
leaddf['session'] = pd.cut(leaddf['completed_time'], bins=b, labels=l, include_lowest=True)
def f(x):
    if (x > 4) and (x < 8):
        return 'Early_Morning'
    elif (x >= 8) and (x < 12 ):
        return 'Morning'
    elif (x >= 12) and (x < 16):
        return'Noon'
    elif (x >= 16) and (x < 20) :
        return 'Eve'
    elif (x >= 20) and (x < 24):
        return'Night'
    elif (x <= 4):
        return'Late_Night'
leaddf['session'] = leaddf['completed_time'].apply(f)
######################### end ###############
leaddf['Early_Morning_calls']=leaddf['session']
leaddf['Morning_calls']=leaddf['session']
leaddf['Noon_calls']=leaddf['session']
leaddf['Eve_calls']=leaddf['session']
leaddf['Night_calls']=leaddf['session']
leaddf['Late_Night_calls']=leaddf['session']
################## replace restpect values to in the column #################
leaddf['Early_Morning_calls'].replace({"Early_Morning": "Early_Morning", "Morning": 0,"Noon":0,"Eve":0,"Night":0,"Late_Night":0},inplace=True)
leaddf['Morning_calls'].replace({"Early_Morning": 0, "Morning":"Morning","Noon":0,"Eve":0,"Night":0,"Late_Night":0},inplace=True)
leaddf['Noon_calls'].replace({"Early_Morning": 0, "Morning": 0,"Noon":"Noon","Eve":0,"Night":0,"Late_Night":0},inplace=True)
leaddf['Eve_calls'].replace({"Early_Morning": 0, "Morning": 0,"Noon":0,"Eve":"Eve","Night":0,"Late_Night":0},inplace=True)
leaddf['Night_calls'].replace({"Early_Morning": 0, "Morning": 0,"Noon":0,"Eve":0,"Night":"Night","Late_Night":0},inplace=True)
leaddf['Late_Night_calls'].replace({"Early_Morning": 0, "Morning": 0,"Noon":0,"Eve":0,"Night":0,"Late_Night":"Late_Night"},inplace=True)
############## end #####################3
print (leaddf)