
### Problem statement:Calculating the total target,achieved and performance based on monthly,quaterly and yearly #######

from pymongo import MongoClient
import pandas as pd
from activeorg import active
server = MongoClient("mongodb://localhost:27017/")
db=server['salesfokuz_lead']
achievedcollections = db['achieved']
targetcollections = db['target_month']
quartercollections = db['quarter']
usercoll = db['users']
usercollections = db['da_user']

################################################# Data collection using find() #################################################

finaldf =  pd.DataFrame(list(targetcollections.find({},{"organization_id":1,"user_id":1,"target": 1,'year': 1, 'month': 1,"_id": 0})))
achieveddf =  pd.DataFrame(list(achievedcollections.find({},{"organization_id":1,"user_id":1,"amount": 1,'year': 1, 'month': 1,"_id": 0})))


######################################## Target Table Creation #########################################################

#Creating a pivot table for total target with respect to month and transpose the pivot table 
df_pivot = finaldf.pivot_table(index='month',columns=['month'],aggfunc=sum, fill_value=0).T
df_pivot = finaldf.pivot_table(index='month',columns=['organization_id','user_id','year'],aggfunc=sum, fill_value=0).T

#Rearranging the month column
new_order = ['January', 'February', 'March', 'April', 'May', 'June', 'July','August', 'September', 'October', 'November', 'December']
df4_target = df_pivot.reindex(new_order, axis=1)
#Replacing the NaN values with zero
df4_target = df4_target.fillna(0)

# ######################################## Achieved Table Creation #########################################################

#Creating a pivot table for total achieved with respect to month and transpose the pivot table 
df_pivot = achieveddf.pivot_table(index='month',columns=['organization_id','user_id','year'],aggfunc=sum, fill_value=0).T
new_order = ['January', 'February', 'March', 'April', 'May', 'June', 'July','August', 'September', 'October', 'November', 'December']
# df5 = df_pivot.reindex(new_order, axis=1)
#Converting the values to numeric and check whether any blank space exit,if it exit replace with NaN value
df5 = df_pivot.apply(pd.to_numeric, errors='coerce')
df5 = df5.fillna(0) #Replace NaN with zero


#checking whether the month exit in the dataframe:if not, add the month column with zero values
def absolute_value(k):
    if 'January' not in k:
        k['January'] = 0
        

    if 'February' not in k:
        k['February'] = 0

    if 'March' not in k:
        k['March'] = 0

    
    if 'April' not in k:
        k['April'] = 0

    

    if 'May' not in k:
        k['May'] = 0

    
    if 'June' not in k:
        k['June'] = 0

    
    if 'July' not in k:
        k['July'] = 0

    
    if 'August' not in k:
        k['August'] = 0

    
    if 'September' not in k:
        k['September'] = 0

    
    if 'October' not in k:
        k['October'] = 0

    
    if 'November' not in k:
        k['November'] = 0

    
    if 'December' not in k:
        k['December'] = 0
absolute_value(df5) 
df4_target.apply(absolute_value)

###########Calculating the toal target of quarter1,quarter2,quarter3,quarter4 and yearly ##############

df4_target['quarter1'] = df4_target['January'] + df4_target['February']+df4_target['March']
df4_target['quarter2'] = df4_target['April'] + df4_target['May']+df4_target['June']
df4_target['quarter3'] = df4_target['July']+df4_target['August']+df4_target['September']
df4_target['quarter4'] = df4_target['October'] + df4_target['November']+ df4_target['December']
df4_target['yearly']   =  df4_target['January'] + df4_target['February']+df4_target['March']+df4_target['April'] + df4_target['May']+df4_target['June']+df4_target['July']+df4_target['August']+df4_target['September']+df4_target['October'] + df4_target['November']+df4_target['December']
#Resetting the index to rearrange the column
df4_target = df4_target.reset_index()
#Merging the user dataframe and total target dataframe to exact the active users
target_merge = active(df4_target)
#Droping the unwanted column
target_merge = target_merge.drop(columns=['level_0'])
#Remaning the column 
target_merge.columns = ['organization_id','user_id','year','target_January','target_February','target_March','target_April','target_May','target_June','target_July','target_August','target_September','target_October','target_November', 'target_December','target_quarter1','target_quarter2','target_quarter3','target_quarter4','target_yearly']
print(target_merge)

########################## Rearranging the achieved table #################################

#Resetting the index to rearrange the column   
df6 = df5.reset_index()
#reindexing to reduce the multiindexing
df6.reset_index()
#Merging the user dataframe and total achieved dataframe to exact the active users
achieved_merge1 = active(df6)
#Droping the unwanted column
if 'level_0' in achieved_merge1:
    achieved_merge1 = achieved_merge1.drop(columns=['level_0'])
else: 
    
    achieved_merge1.columns = ['organization_id','user_id', 'year','July','June','May','January', 'February', 'March', 'April','August','September','October','November','December']

#Rearranging the month column
new_order = ['organization_id', 'user_id','year','January','February', 'March', 'April', 'May', 'June', 'July','August', 'September', 'October', 'November', 'December']
achieved_merge = achieved_merge1.reindex(new_order, axis=1)
###########Calculating the toal target of quarter1,quarter2,quarter3,quarter4 and yearly#################
achieved_merge['quarter1'] = achieved_merge['January'] + achieved_merge['February']+achieved_merge['March']
achieved_merge['quarter2'] = achieved_merge['April'] + achieved_merge['May']+achieved_merge['June']
achieved_merge['quarter3'] = achieved_merge['July']+achieved_merge['August']+achieved_merge['September']
achieved_merge['quarter4'] = achieved_merge['October'] + achieved_merge['November']+ achieved_merge['December']
achieved_merge['yearly']   =  achieved_merge['January'] + achieved_merge['February']+achieved_merge['March']+achieved_merge['April'] + achieved_merge['May']+achieved_merge['June']+achieved_merge['July']+achieved_merge['August']+achieved_merge['September']+achieved_merge['October'] + achieved_merge['November']+ achieved_merge['December']
#Remaning the column 
achieved_merge.columns = ['organization_id', 'user_id', 'year','achieved_January','achieved_February','achieved_March','achieved_April','achieved_May','achieved_June','achieved_July','achieved_August','achieved_September','achieved_October','achieved_November', 'achieved_December','achieved_quarter1','achieved_quarter2','achieved_quarter3','achieved_quarter4','achieved_yearly']
print(achieved_merge)
################################# PERFORMANCE #####################################
#Calculating the performance based on target and achieved(monthly,quarter1,quarter2,quarter3,quarter4 and yearly)
df7_merge = pd.merge(achieved_merge, target_merge, on=['organization_id', 'user_id','year'])
df7_merge['Jan_percentage'] = (df7_merge['achieved_January'] / df7_merge['target_January'])*100
df7_merge['Feb_percentage'] = (df7_merge['achieved_February'] / df7_merge['target_February'])*100
df7_merge['Mar_percentage'] = (df7_merge['achieved_March'] / df7_merge['target_March'])*100
df7_merge['Apr_percentage'] = (df7_merge['achieved_April'] / df7_merge['target_April'])*100
df7_merge['May_percentage'] = (df7_merge['achieved_May'] / df7_merge['target_May'])*100
df7_merge['June_percentage'] = (df7_merge['achieved_June'] / df7_merge['target_June'])*100
df7_merge['July_percentage'] = (df7_merge['achieved_July'] / df7_merge['target_July'])*100
df7_merge['Aug_percentage'] = (df7_merge['achieved_August'] / df7_merge['target_August'])*100
df7_merge['Sep_percentage'] = (df7_merge['achieved_September'] / df7_merge['target_September'])*100
df7_merge['Oct_percentage'] = (df7_merge['achieved_October'] / df7_merge['target_October'])*100
df7_merge['Nov_percentage'] = (df7_merge['achieved_November'] / df7_merge['target_November'])*100
df7_merge['Dec_percentage'] = (df7_merge['achieved_December'] / df7_merge['target_December'])*100
df7_merge['Quar1_percentage'] = (df7_merge['achieved_quarter1'] / df7_merge['target_quarter1'])*100
df7_merge['Quar2_percentage'] = (df7_merge['achieved_quarter2'] / df7_merge['target_quarter2'])*100
df7_merge['Quar3_percentage'] = (df7_merge['achieved_quarter3'] / df7_merge['target_quarter3'])*100
df7_merge['Quar4_percentage'] = (df7_merge['achieved_quarter4'] / df7_merge['target_quarter4'])*100

# #*************************** Yearly *****************************************************
df7_merge['Yearly_percentage'] = (df7_merge['achieved_yearly'] / df7_merge['target_yearly'])*100
datad=df7_merge.fillna(0) # replace the Nan with zeros
print(datad)
############################## converting the dataframe to dictionary and updating to database ###############


df7_merge_dict = datad.to_dict("records")
keya=['achieved_January', 'achieved_February', 'achieved_March', 'achieved_April', 'achieved_May', 'achieved_June', 'achieved_July', 'achieved_August', 'achieved_September', 'achieved_October', 'achieved_November', 'achieved_December', 'achieved_quarter1', 'achieved_quarter2', 'achieved_quarter3', 'achieved_quarter4', 'achieved_yearly']
keyp = [ 'Jan_percentage', 'Feb_percentage', 'Mar_percentage', 'Apr_percentage', 'May_percentage', 'June_percentage', 'July_percentage', 'Aug_percentage', 'Sep_percentage', 'Oct_percentage', 'Nov_percentage', 'Dec_percentage', 'Quar1_percentage', 'Quar2_percentage', 'Quar3_percentage', 'Quar4_percentage', 'Yearly_percentage']
keyt = ['target_January', 'target_February', 'target_March', 'target_April', 'target_May', 'target_June', 'target_July', 'target_August', 'target_September', 'target_October', 'target_November', 'target_December', 'target_quarter1', 'target_quarter2', 'target_quarter3', 'target_quarter4', 'target_yearly']

for i in df7_merge_dict:
    i['data_a']={x : i[x] for x in keya}
    for x in keya: del i[x]
for i in df7_merge_dict:
    i['data_t']={x : i[x] for x in keyt}
    for x in keyt: del i[x]
for i in df7_merge_dict:
    i['data_p']={x : i[x] for x in keyp}
    for x in keyp: del i[x]
for i in df7_merge_dict:
    print(usercollections.update_one({'organization_id':i['organization_id'],'user_id':i['user_id']},{'$set':{'target':{str(i['year']):i['data_t']}}}))
    print(usercollections.update_one({'organization_id':i['organization_id'],'user_id':i['user_id']},{'$set':{'achieved':{str(i['year']):i['data_a']}}}))
    print(usercollections.update_one({'organization_id':i['organization_id'],'user_id':i['user_id']},{'$set':{'sales_percent':{str(i['year']):i['data_p']}}}))
    #print(performancetablecollect.insert_many(df7_merge_dict))