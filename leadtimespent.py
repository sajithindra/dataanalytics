from pymongo import MongoClient
import pandas as pd
pd.set_option("display.max_rows",None,"display.max_columns",None)

import datetime
from datetime import datetime
server = MongoClient('mongodb://localhost:27017')
db=server['salesfokuz_lead']
usercollection = db['users']
leadcollection = db['lead']
dauser= db['da_user']


################  Data Frame creating  #######

leaddf =  pd.DataFrame(list(leadcollection.find({},{"created":1,"close_date": 1,"organization_id": 1,"user_id":1,"_id": 0})))
##################   end #######################

# ################ data processing for  leads #################
leaddf=leaddf.drop(leaddf[leaddf['user_id'] == ''].index)
currenttime = datetime.now()
date_time =currenttime.strftime("%Y-%m-%dT%H:%M:%S")
leaddf["close_date"].fillna(date_time, inplace = True)
leaddf = leaddf.replace("", date_time)
leaddf['created']=pd.to_datetime(leaddf['created'])
leaddf['close_date']=pd.to_datetime(leaddf['close_date'])
leaddf['lead_time']=leaddf['close_date']-leaddf['created']
leaddf['lazy_factor'] = leaddf['lead_time'].dt.days.astype(int)
leadtime_mean=leaddf.groupby(['organization_id','user_id']).mean().reset_index().to_dict('records')
for i in leadtime_mean:
    print(dauser.update_one({'organization_id':i['organization_id'], 'user_id':i['user_id']},{'$set':{'lazy_factor':i['lazy_factor']}}))
######################### end ############
