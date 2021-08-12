
# module to filter the active organization for the dataset

from pymongo import MongoClient
import pandas
server = MongoClient("mongodb://localhost:27017/")
db=server['salesfokuz_lead']
usercoll = db['users']

### fetching the active organization from the user collections ##############

data=pandas.DataFrame(list(usercoll.find({"status" :1,'user_type':"organization"},{"organization_id":1,"_id":0})))

################ FUNCTION TO FILTER ACTIVE ORG ###########################
def active(x):
    d= pandas.merge(x,data, how='inner',on=['organization_id'])
    return d                # returns the intersection of the two databases wrt organization_id