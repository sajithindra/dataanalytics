from pymongo import MongoClient
import pandas as pd
server = MongoClient("mongodb://localhost:27017/")
db=server['salesfokuz_lead']
import pandas as pd0
import seaborn as sns
import matplotlib.pyplot as plt
targetvaluesdb =server['targetvalues']
achievedcollections = db['achieved']
targetcollections = db['target_month']
quartercollections = db['quarter']
usercoll = db['users']
correlationcollect = targetvaluesdb['correlations']

###################################################### Data collection #################################################

finaldf =  pd.DataFrame(list(targetcollections.find({},{"organization_id":1,"user_id":1,"target": 1,'year': 1, 'month': 1,"_id": 0})))
userdf = pd.DataFrame(list(usercoll.find({"status" :{"$eq" : 1}},{"user_type" : "organization","organization_id":1,"user_id":1,"_id":0})))
achieveddf =  pd.DataFrame(list(achievedcollections.find({},{"organization_id":1,"user_id":1,"amount": 1,'year': 1, 'month': 1,"_id": 0})))

######################################## Target Table Creation #########################################################

df_pivot = finaldf.pivot_table(index='month',columns=['organization_id','user_id','year'],aggfunc=sum, fill_value=0).T
new_order = ['January', 'February', 'March', 'April', 'May', 'June', 'July','August', 'September', 'October', 'November', 'December']
df4_target = df_pivot.reindex(new_order, axis=1)
df4_target = df4_target.fillna(0)
df4_target['quarter1'] = df4_target['January'] + df4_target['February']+df4_target['March']
df4_target['quarter2'] = df4_target['April'] + df4_target['May']+df4_target['June']
df4_target['quarter3'] = df4_target['July']+df4_target['August']+df4_target['September']
df4_target['quarter4'] = df4_target['October'] + df4_target['November']+ df4_target['December']
df4_target['yearly']   =  df4_target['January'] + df4_target['February']+df4_target['March']+df4_target['April'] + df4_target['May']+df4_target['June']+df4_target['July']+df4_target['August']+df4_target['September']+df4_target['October'] + df4_target['November']+df4_target['December']
df4_target = df4_target.reset_index()
target_merge = pd.merge(userdf, df4_target, how='inner',on=['organization_id', 'user_id']) 
target_merge = target_merge.drop(columns=['level_0','user_type'])
target_merge.columns = ['organization_id','user_id','year','target_January','target_February','target_March','target_April','target_May','target_June','target_July','target_August','target_September','target_October','target_November', 'target_December','target_quarter1','target_quarter2','target_quarter3','target_quarter4','target_yearly']
# print(finaldf)
print(target_merge)

######################################## Achieved Table Creation #########################################################

df_pivot = achieveddf.pivot_table(index='month',columns=['organization_id','user_id','year'],aggfunc=sum, fill_value=0).T
new_order = ['January', 'February', 'March', 'April', 'May', 'June', 'July','August', 'September', 'October', 'November', 'December']
df5 = df_pivot.reindex(new_order, axis=1)
df5 = df5.apply(pd.to_numeric, errors='coerce')
df5 = df5.fillna(0)
df5['quarter1'] = df5['January'] + df5['February']+ df5['March']
df5['quarter2'] = df5['April'] + df5['May']+df5['June']
df5['quarter3'] =  df5['July']+df5['August']+df5['September']
df5['quarter4'] = df5['October'] + df5['November'] + df5['December']
df5['yearly']   =  df5['January'] + df5['February']+df5['March']+df5['April'] + df5['May']+df5['June']+df5['July']+df5['August']+df5['September']+df5['October'] + df5['November'] + df5['December']
df5_achieved = df5.reset_index()
achieved_merge = pd.merge(userdf, df5_achieved, how='inner',on=['organization_id', 'user_id']) 
achieved_merge = achieved_merge.drop(columns=['level_0','user_type'])
achieved_merge.columns = ['organization_id', 'user_id', 'year','achieved_January','achieved_February','achieved_March','achieved_April','achieved_May','achieved_June','achieved_July','achieved_August','achieved_September','achieved_October','achieved_November', 'achieved_December','achieved_quarter1','achieved_quarter2','achieved_quarter3','achieved_quarter4','achieved_yearly']
print(achieved_merge)

#################################### CORRELATION ################################################

df7_merge = pd.merge(achieved_merge, target_merge, on=['organization_id', 'user_id','year']) 
print(df7_merge)
df7_merge['jan_corr'] = df7_merge['target_January'].corr(df7_merge['achieved_January'])
df7_merge['feb_corr'] = df7_merge['target_February'].corr(df7_merge['achieved_February'])
df7_merge['March_corr'] = df7_merge['target_March'].corr(df7_merge['achieved_March'])
df7_merge['April_corr'] = df7_merge['target_April'].corr(df7_merge['achieved_April'])
df7_merge['May_corr'] = df7_merge['target_May'].corr(df7_merge['achieved_May'])
df7_merge['June_corr'] = df7_merge['target_June'].corr(df7_merge['achieved_June'])
df7_merge['July_corr'] = df7_merge['target_July'].corr(df7_merge['achieved_July'])
df7_merge['August_corr'] = df7_merge['target_August'].corr(df7_merge['achieved_August'])
df7_merge['September_corr'] = df7_merge['target_September'].corr(df7_merge['achieved_September'])
df7_merge['October_corr'] = df7_merge['target_October'].corr(df7_merge['achieved_October'])
df7_merge['November_corr'] = df7_merge['target_November'].corr(df7_merge['achieved_November'])
df7_merge['December_corr'] = df7_merge['target_December'].corr(df7_merge['achieved_December'])
df7_merge['quarter1_corr'] = df7_merge['target_quarter1'].corr(df7_merge['achieved_quarter1'])
df7_merge['quarter2_corr'] = df7_merge['target_quarter2'].corr(df7_merge['achieved_quarter2'])
df7_merge['quarter3_corr'] = df7_merge['target_quarter3'].corr(df7_merge['achieved_quarter3'])
df7_merge['quarter4_corr'] = df7_merge['target_quarter4'].corr(df7_merge['achieved_quarter4'])
df7_merge['yearly_corr'] = df7_merge['target_yearly'].corr(df7_merge['achieved_yearly'])

df8_merge = df7_merge.drop(columns=['achieved_January','achieved_February','achieved_March','achieved_April','achieved_May','achieved_June','achieved_July',
                                   'achieved_August','achieved_September','achieved_October','achieved_November', 'achieved_December','achieved_quarter1','achieved_quarter2',
                                   'achieved_quarter3','achieved_quarter4','achieved_yearly','target_January','target_February','target_March','target_April','target_May','target_June',
                                   'target_July','target_August','target_September','target_October','target_November', 'target_December','target_quarter1','target_quarter2','target_quarter3','target_quarter4','target_yearly'])

print(df8_merge)

data_dict = df8_merge.to_dict("records")
print(correlationcollect.insert(data_dict))


