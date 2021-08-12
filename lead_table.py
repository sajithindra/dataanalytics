from pymongo import MongoClient
server = MongoClient('mongodb://localhost:27017')
import pandas as pd
db=server['salesfokuz_lead']
leadcollections = db['lead']


################  Pipeline defined for the data set creation  #######
livepipeline = [
    {
    '$match' : {
            
        }
    },
   {
        

        '$lookup': {
            'from': 'lead_stage',
            'localField': 'lead_stage',
            'foreignField': 'id',
            'as': 'lead_stage'
        }
    },
    {
        '$lookup': {
            'from': 'lead_log',
            'localField': 'id',
            'foreignField': 'lead_id',
            'as': 'lead_log'
        }
    },
{
        "$project": {
            "user_id": 1.0,"_id" :0,"status":1,
            "stage": {
                "$arrayElemAt": [
                    "$lead_stage.stage",
                    0.0
                ]
            },
            "module": {
                "$arrayElemAt": [
                    "$lead_log.module",
                    0.0
                ]
            },
            "action": {
                "$arrayElemAt": [
                    "$lead_log.action",
                    0.0
                ]
            },
            "lead_id": {
                "$arrayElemAt": [
                    "$lead_log.lead_id",
                    0.0
                ]
            },
            "organization_id": 1.0,

        }
    }



]


################## aggregating data from mongodb using the pipeline #############
leaddata = list(leadcollections.aggregate(livepipeline))
# print(list(leaddata))
leaddf = pd.DataFrame(leaddata)

##################################### frequency table creation #####################################
df1=leaddf[leaddf['action']== 'Completed the']
dups = df1.pivot_table(index = ['organization_id','user_id','lead_id','status','stage','module'],aggfunc='size')
lead_module =dups.reset_index()
lead_module.columns = ['organization_id','user_id','lead_id','status','stage','module', 'frequency']
res = lead_module.pivot(index='lead_id', columns='module', values='frequency').reset_index()
df4_stage = res.fillna(0)
df7_merge = pd.merge(df4_stage, lead_module, on=['lead_id'])
df_stage = df7_merge[df7_merge['stage'].isin(['Converted', 'Failed'])]
print(df_stage)
df_rep = df_stage.replace({'Converted':1, 'Failed': 0})
print(df_rep)
#################################### CORRELATION ################################################

df_rep['call_corr'] = df_rep['call'].corr(df_rep['stage'])
df_rep['chat_corr'] = df_rep['chat'].corr(df_rep['stage'])
df_rep['meeting_corr'] = df_rep['meeting'].corr(df_rep['stage'])
df_rep['mail_corr'] = df_rep['mail'].corr(df_rep['stage'])
df_rep = df_rep.drop(columns=['frequency'])
print(df_rep)


