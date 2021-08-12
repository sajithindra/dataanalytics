from pymongo import MongoClient
import pandas as pd
pd.set_option("display.max_rows",None,"display.max_columns",None)
pd.options.mode.chained_assignment = None
import datetime
from datetime import datetime
server = MongoClient('mongodb://localhost:27017')
db=server['salesfokuz_lead']
leadsactivity = db['lead_log']
dadb = server['da']
meeting_time_users= dadb['meetingtime_user_id']
meeting_time_organizations= dadb['meetingtime_organization_id']

livepipeline = [
{
        "$addFields": {
            "punch_status": {
                "$toString": "$data.punch_status"
            }}
    },
{
        "$addFields": {
            "punch_in": {
                "$toString": "$data.punch_in_datetime"
            }}
    },
{
        "$addFields": {
            "punch_out": {
                "$toString": "$data.punch_out_datetime"
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
            'user_id': 1,
            "module": 1,
            "action": 1,
            "lead_id":1,
            "punch_in":1,
            "organization_id":1,
            "punch_out": 1,
            "punch_status": 1,

        }
    }

]
##################   END #######################

################## aggregating data from mongodb using the pipeline #############
leadlogdata = list(leadsactivity.aggregate(livepipeline))
# print(leadlogdata)
##################   END #######################
leaddf = pd.DataFrame(leadlogdata)
# print(leaddf)
leaddf.fillna(0, inplace = True)
leaddf=leaddf.replace('', 0)
leaddf=leaddf.drop(leaddf[leaddf.punch_in == 0].index)
leaddf=leaddf.drop(leaddf[leaddf.punch_out == 0].index)
leaddf=leaddf.drop(leaddf[leaddf.action == 'Cancelled the'].index)

leaddf=leaddf[leaddf.module == 'meeting']
# print(leaddf)
################################################
leaddf['punch_in']=pd.to_datetime(leaddf['punch_in'])
leaddf['punch_out']=pd.to_datetime(leaddf['punch_out'])
leaddf['meeting_time']=leaddf['punch_out']-leaddf['punch_in']
leaddf['days']=leaddf['meeting_time'].dt.days.astype(int)
leaddf=leaddf.drop(leaddf[leaddf.days != 0].index)

leaddf['seconds']=leaddf['meeting_time'].dt.seconds.astype(int)
leaddf['hours']=leaddf['seconds']//3600
leaddf['minutes']=leaddf['seconds']//60
# print(leaddf)
totaltime_mean_user_id = leaddf.groupby(['user_id']).mean().to_dict()
totaltime_mean_organization_id = leaddf.groupby(['organization_id']).mean().to_dict()
# print(meeting_time_users.insert_one(totaltime_mean_user_id))
# print(meeting_time_organizations.insert_one(totaltime_mean_organization_id))
print(meeting_time_users.update_many({}, {"$set":totaltime_mean_user_id}, upsert=True))
print(meeting_time_organizations.update_many({}, {"$set":totaltime_mean_organization_id}, upsert=True))
