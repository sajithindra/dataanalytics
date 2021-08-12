######## Aim:performance based on monthly,quaterly and yearly #######
from pymongo import MongoClient
import pandas as pd
from activeorg import active
server = MongoClient("mongodb://localhost:27017/")
db=server['salesfokuz_lead']
import pandas as pd
targetvaluesdb =server['targetvalues']
achievedcollections = db['achieved']
targetcollections = db['target_month']
quartercollections = db['quarter']
usercoll = db['users']
usercollections = db['da_user']
###################################################### Data collection #################################################
asdf  =  pd.DataFrame(list(usercollections.find({},{"organization_id":1,"user_id":1,'target_January': '$target.2021.target_January',
                                                   'target_February': '$target.2021.target_February',
                                                   'target_March': '$target.2021.target_March',
                                                   'target_April': '$target.2021.target_April',
                                                   'target_May': '$target.2021.target_May',
                                                   'target_June': '$target.2021.target_June',
                                                   'target_July': '$target.2021.target_July',
                                                   'target_August': '$target.2021.target_August',
                                                   'target_September': '$target.2021.target_September',
                                                   'target_October': '$target.2021.target_October',
                                                   'target_November': '$target.2021.target_November',
                                                   'target_September': '$target.2021.target_September',
                                                   'target_December': '$target.2021.target_December',
                                                   'target_quarter1': '$target.2021.target_quarter1',
                                                   'target_quarter2': '$target.2021.target_quarter2',
                                                   'target_quarter3': '$target.2021.target_quarter3',
                                                   'target_quarter4': '$target.2021.target_quarter4',
                                                   'target_yearly': '$target.2021.target_yearly',
                                                   
                                                   'achieved_January': '$achieved.2021.achieved_January',
                                                   'achieved_February': '$achieved.2021.achieved_February',
                                                   'achieved_March': '$achieved.2021.achieved_March',
                                                   'achieved_April': '$achieved.2021.achieved_April',
                                                   'achieved_May': '$achieved.2021.achieved_May',
                                                   'achieved_June': '$achieved.2021.achieved_June',
                                                   'achieved_July': '$achieved.2021.achieved_July',
                                                   'achieved_August': '$achieved.2021.achieved_August',
                                                   'achieved_September': '$achieved.2021.achieved_September',
                                                   'achieved_October': '$achieved.2021.achieved_October',
                                                   'achieved_November': '$achieved.2021.achieved_November',
                                                   'achieved_September': '$achieved.2021.achieved_September',
                                                   'achieved_December': '$achieved.2021.achieved_December',
                                                   'achieved_quarter1': '$achieved.2021.achieved_quarter1',
                                                   'achieved_quarter2': '$achieved.2021.achieved_quarter2',
                                                   'achieved_quarter3': '$achieved.2021.achieved_quarter3',
                                                   'achieved_quarter4': '$achieved.2021.achieved_quarter4',
                                                   'achieved_yearly': '$achieved.2021.achieved_yearly',"_id":0})))
df7_merge = asdf.dropna()



################################# PERFORMANCE #####################################
#Calculating the performance based on target and achieved(monthly,quarter1,quarter2,quarter3,quarter4 and yearly)

df7_merge['Jan_sale'] = (df7_merge['achieved_January'] / df7_merge['target_January'])*100
df7_merge['Feb_sale'] = (df7_merge['achieved_February'] / df7_merge['target_February'])*100
df7_merge['Mar_sale'] = (df7_merge['achieved_March'] / df7_merge['target_March'])*100
df7_merge['Apr_sale'] = (df7_merge['achieved_April'] / df7_merge['target_April'])*100
df7_merge['May_sale'] = (df7_merge['achieved_May'] / df7_merge['target_May'])*100
df7_merge['June_sale'] = (df7_merge['achieved_June'] / df7_merge['target_June'])*100
df7_merge['July_sale'] = (df7_merge['achieved_July'] / df7_merge['target_July'])*100
df7_merge['Aug_sale'] = (df7_merge['achieved_August'] / df7_merge['target_August'])*100
df7_merge['Sep_sale'] = (df7_merge['achieved_September'] / df7_merge['target_September'])*100
df7_merge['Oct_sale'] = (df7_merge['achieved_October'] / df7_merge['target_October'])*100
df7_merge['Nov_sale'] = (df7_merge['achieved_November'] / df7_merge['target_November'])*100
df7_merge['Dec_sale'] = (df7_merge['achieved_December'] / df7_merge['target_December'])*100
df7_merge['quarter1_sale'] = (df7_merge['achieved_quarter1'] / df7_merge['target_quarter1'])*100
df7_merge['quarter2_sale'] = (df7_merge['achieved_quarter2'] / df7_merge['target_quarter2'])*100
df7_merge['quarter3_sale'] = (df7_merge['achieved_quarter3'] / df7_merge['target_quarter3'])*100
df7_merge['quarter4_sale'] = (df7_merge['achieved_quarter4'] / df7_merge['target_quarter4'])*100
df7_merge['yearly_sale'] = (df7_merge['achieved_yearly'] / df7_merge['target_yearly'])*100
#To filter out active organization
sale_merge = active(df7_merge)
datad=sale_merge.fillna(0) # replace the Nan with zeros
print(datad)
df7_merge_dict = datad.to_dict("records")
print(usercollections.insert_many(df7_merge_dict))

