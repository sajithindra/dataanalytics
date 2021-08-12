#######Problem statement:Where X is a random variable such that calculate the Probability Mass Function of no.of lead converted per month by each organization##########

from pymongo import MongoClient
import pandas
from datetime import datetime
import calendar
from activeorg import active
server = MongoClient('mongodb://localhost:27017')
db=server['salesfokuz_lead']
usercollections = db['users']
leadcollections = db['lead']
dacoll= db['da_user']


############################ Pipeline ################################

livepipeline = [
     {
        '$lookup': {
            'from': 'lead_stage', 
            'localField': 'lead_stage', 
            'foreignField': 'id', 
            'as': 'lead_stage'
        }
    }, {
        '$project': {
            '_id':0,
            'user_id': 1, 
            'organization_id':1,
            'stage': '$lead_stage.stage', 
            'organization_id': 1, 
            'created': 1, 
            'close_date': 1
        }
    }
]

################## data collection and data cleaning #############

leaddata = list(leadcollections.aggregate(livepipeline))
for i in leaddata:
    if (len(i["stage"])==0):
        leaddata.remove(i)
    else:
        i["stage"] = i["stage"][0]


############################### Extracting Month  ############################

leaddf = pandas.DataFrame(leaddata)
#collecting the converted lead
newdf=leaddf[leaddf["stage"] == "Converted"]
#removing the data's with black space
newdf=newdf.drop(newdf[newdf.close_date == ''].index)
#removing the data's with NaN values
newdf=newdf[newdf['close_date'].notna()]
#converting the created and completed lead data to datetime 
newdf['created']= pandas.to_datetime(newdf['created'])
newdf['close_date']= pandas.to_datetime(newdf['close_date'])
#extracting the date from lead completed date
newdf['date'] = newdf['close_date'].dt.date
#extracting the month from lead completed date
newdf['month'] = pandas.DatetimeIndex(newdf['date']).month
#Matching the month number to month name
newdf['Month'] = newdf['month'].apply(lambda x: calendar.month_abbr[x])
#extracting the year from lead completed date
newdf['year'] = pandas.DatetimeIndex(newdf['date']).year
print(newdf)
# Calculating the no.of lead converted in each month
df_pivot = newdf.pivot_table(index='Month',columns=['organization_id','year'],aggfunc='size', fill_value=0).T
print(df_pivot)
new_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul','Aug', 'Sep', 'Oct', 'Nov', 'Dec']
#Resetting the index
df5 = df_pivot.reindex(new_order, axis=1)
df5_lead = df5.reset_index()


# ###################################### PMF Dataframe ###############################
#Creating new dataframe
df5_lead1 = pandas.DataFrame()
df5_lead1['organization_id'] = df5_lead['organization_id']
df5_lead1['year'] = df5_lead['year']
# #calculating the pmf no.of lead converted in an orgaanization per month

df5_lead1['pmf_jan'] = df5_lead['Jan'].sort_index() / len(df5_lead['Jan']) 
df5_lead1['pmf_feb'] = df5_lead['Feb'].sort_index() / len(df5_lead['Feb'])
df5_lead1['pmf_Mar'] = df5_lead['Mar'].sort_index() / len(df5_lead['Mar'])  
df5_lead1['pmf_Apr'] = df5_lead['Apr'].sort_index() / len(df5_lead['Apr'])
df5_lead1['pmf_May'] = df5_lead['May'].sort_index() / len(df5_lead['May'])
df5_lead1['pmf_June'] = df5_lead['Jun'].sort_index() / len(df5_lead['Jun'])
df5_lead1['pmf_July'] = df5_lead['Jul'].sort_index() / len(df5_lead['Jul']) 
df5_lead1['pmf_Aug'] = df5_lead['Aug'].sort_index() / len(df5_lead['Aug'])  
df5_lead1['pmf_Sep'] = df5_lead['Sep'].sort_index() / len(df5_lead['Sep'])  
df5_lead1['pmf_Oct'] = df5_lead['Oct'].sort_index() / len(df5_lead['Oct']) 
df5_lead1['pmf_Nov'] = df5_lead['Nov'].sort_index() / len(df5_lead['Nov']) 
df5_lead1['pmf_Dec'] = df5_lead['Dec'].sort_index() / len(df5_lead['Dec']) 
df5_lead1 = active(df5_lead1)
print(df5_lead1)
df7_merge_dict = df5_lead1.to_dict("records")
print(dacoll.insert_many(df7_merge_dict))


