import pandas as pd
import pymongo
from pymongo import cursor
import sys,re,os
import config as cg
mongoClient = pymongo.MongoClient(cg.MONGODB_URI)
summaryCollection =  mongoClient['global_summary_details']['myntra_summary_temp']
mongoClient = pymongo.MongoClient(cg.MONGODB_URI)
yesterdaydb=mongoClient["sg_shortlink_backup"]
		#print(yesterdaydb.list_collection_names())
camplist_dates=[]
newcollist1=[] ##List with Repetitive Dates
result1=summaryCollection.find({"campaigndate":{"$gte":"2022-06-01","$lte":"2022-06-15"}})
# result1=summaryCollection.aggregate(
#     [
#         {
#             "$group":{
#                 "_id":{"campaigndate":"$campaigndate"},
#                 "count":{"$sum":1}
#             }
#         }
#      ],
# )
# for rec in result1:
#     print(rec['_id']['campaigndate'])
# sys.exit()

for rec in result1:
    s=rec['campaigndate'].split("-")
    #print(s)
    b="d"+"".join(s)
    #print(b)
    newcollist1.append(str(rec['campaigndate']))
print(newcollist1)
print()
newcollist=[] #List with unique dates
for nrec in newcollist1:
    if nrec not in newcollist:
        newcollist.append(nrec)
print(newcollist)
#sys.exit()
count=0
for date in newcollist:
    #print(date)
    #date = "2022-06-01"
    cursor1=summaryCollection.find({"campaigndate":{"$eq":(date)}})
    #print(cursor1)
    for rec in cursor1:
        #print(rec['campaigndate'])
        mongoClient = pymongo.MongoClient(cg.MONGODB_URI)
        #converting the date into database collection format
        s=rec["campaigndate"].split("-")
        #print(s)
        b="d"+"".join(s)
        print(b)
        shortcodeCollection = yesterdaydb[b]	
        distinctclicks = int(rec['distinctclicks'])
        #print("Connection SG backup for clicks")
        distinctClicksCur = shortcodeCollection.aggregate( [{"$match" : {"$and" : [{"shortcode":rec['shortcode']},{"user_agent" : {"$regex": "Mobile" } }]}},{"$group": {"_id": "$mobileno"}},{"$limit":distinctclicks}], allowDiskUse = True )
        #print("Closing SG Backup for clicks")
        #Closing Mongo Connection
        mongoClient.close()
        clickDF = pd.DataFrame(distinctClicksCur)
        print(clickDF)
        clickDF.to_csv("/home/charanreddy/Documents/pymongo_practice/Myntra Project/automateAll.csv",sep=",",index=None, header=False,encoding='utf-8',mode='a')
        print("------------------------------")

