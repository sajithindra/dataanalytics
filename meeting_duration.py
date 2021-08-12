from pymongo import MongoClient
import pandas as pd
pd.set_option("display.max_rows",None,"display.max_columns",None)
pd.options.mode.chained_assignment = None
import activeorg
server = MongoClient('mongodb://localhost:27017')
db=server['salesfokuz_lead']
leadsactivity = db['lead_log']
dacoll= db['da_user']
usercoll=db['users']
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
##################   end #######################
################## Creating dataframe  from active module ###########
df = pd.DataFrame(leadlogdata)
leaddf = activeorg.active(df)
################## end #############
########### Filtering the data with respect to module and action #############
leaddf=leaddf[leaddf.module == 'meeting']
leaddf=leaddf[leaddf.action == 'Completed the']
################### cleaning the data #############
leaddf.dropna()
leaddf=leaddf.drop(leaddf[leaddf.punch_in == ''].index)
leaddf=leaddf.drop(leaddf[leaddf.punch_out == ''].index)
######################### end #########################
leaddf['punch_in']=pd.to_datetime(leaddf['punch_in'])                 #converting the data into datetime
leaddf['punch_out']=pd.to_datetime(leaddf['punch_out'])
leaddf['meeting_time']=leaddf['punch_out']-leaddf['punch_in']
leaddf['total_time'] = leaddf['meeting_time'].astype(str).map(lambda x: x[7:])#get time in HH:MM:SS format
leaddf['seconds']=leaddf['meeting_time'].dt.seconds.astype(int)
newdf=pd.DataFrame()                                                  #creates a new df
newdf['organization_id']=leaddf['organization_id']                    #assign organization_id,user_id,and time into newdf
newdf['user_id']=leaddf['user_id']
newdf['lead_id']=leaddf['lead_id']
newdf['meeting_time']=leaddf['total_time']
meetingtime=newdf.to_dict('records') #grouping the organization_id and user_id
# print(meetingtime)
for i in meetingtime:
    print(dacoll.update_one({'organization_id':i['organization_id'], 'user_id':i['user_id'],'lead_id':i['lead_id']},{'$set':{'meeting_time':i['meeting_time']}}))