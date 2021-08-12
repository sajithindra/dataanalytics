from pymongo import MongoClient
import pandas
import activeorg
from datetime import datetime
server = MongoClient('mongodb://localhost:27017')
db=server['salesfokuz_leadfokuz']
leadcollections = db['lead']
dausercollections = db['da_user']
daorgcollections = db['da_org']

################  Pipeline defined for the data set creation  #######
livepipeline = [
    {
        '$match': {}
    }, {
        '$lookup': {
            'from': 'lead_stage', 
            'localField': 'lead_stage', 
            'foreignField': 'id', 
            'as': 'lead_stage'
        }
    }, {
        '$project': {
            'user_id': 1, 
            'stage': '$lead_stage.stage', 
            'organization_id': 1, 
            'created': 1, 
            'close_date': 1
        }
    }
]
##################   END #######################

################## aggregating data from mongodb using the pipeline #############
leaddata = list(leadcollections.aggregate(livepipeline))
################ END #################

################## deleting the data where stage values is zero #################
for i in leaddata:
    if (len(i["stage"])==0):
        leaddata.remove(i)
    else:
        i["stage"] = i["stage"][0]
################ END #################

################ data processing #################
leaddf = pandas.DataFrame(leaddata)
leaddf = activeorg.active(leaddf)
newdf=leaddf[leaddf["stage"] == "Converted"]
newdf=newdf.drop(newdf[newdf.close_date == ''].index)
newdf=newdf[newdf['close_date'].notna()]
newdf['created']= pandas.to_datetime(newdf['created'])
newdf['close_date']= pandas.to_datetime(newdf['close_date'])
newdf['conversion_time'] = newdf['close_date'] - newdf['created']
################ END #################

################# Data conversion #################
newdf['conversion_time'] = newdf['conversion_time'].dt.days.astype(int)
user_mean =newdf.groupby(['organization_id','user_id']).mean().reset_index().to_dict('records')
organization_mean = newdf.groupby(['organization_id']).mean().reset_index().to_dict('records')
################ END #################

#################### PERIODIC DATA UPDATION #####################

for i in user_mean:
    print(dausercollections.update_one({'organization_id':i['organization_id'], 'user_id':i['user_id']},{'$set':{'conversion_time':i['conversion_time']}}))
for i in organization_mean:
    print(daorgcollections.update_one({'organization_id':i['organization_id']},{'$set':{'conversion_time':i['conversion_time']}}))

################ END #################