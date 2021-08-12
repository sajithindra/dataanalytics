from pymongo import MongoClient
import pandas as pd
pd.set_option("display.max_rows",None,"display.max_columns",None)
import outlier_iqr
server = MongoClient('mongodb://localhost:27017')
db=server['salesfokuz_lead']
leadsactivity = db['da_lead']
usercoll=db['users']
livepipeline = [                                           #pipeline to get data from mongodb
{
    '$project': {
            '_id': 0,
            "organization_id":1,
            'user_id': 1,
            'lazy_factor':1,

        }}]
leadlogdata = list(leadsactivity.aggregate(livepipeline))              #aggregating the data
df = pd.DataFrame(leadlogdata)                                         #create dataframe for the aggregated data
lowerbound,upperbound = outlier_iqr.outlier_treatment(df.lazy_factor)   #find the lowerbound and upperbound
df[(df.lazy_factor < lowerbound) | (df.lazy_factor >upperbound )]       #finding the outlier in the column
df.drop(df[ (df.lazy_factor > upperbound) | (df.lazy_factor < lowerbound) ].index , inplace=True)   #removing  the outlier
print(df)