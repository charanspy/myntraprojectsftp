import pandas as pd
import pymongo
from pymongo import cursor
import sys,re,os
import config as cg


#To Run Manually
yesterday = '2022-04-30'
yesterdayCollection = "d"+str('20220430')

def fnGenerateLogsInNewFormat(row):
	try:
		#Mongo DB Connection for clicks
		mongoClient = pymongo.MongoClient(cg.MONGODB_URI)
		shortcodeCollection = mongoClient['sg_shortlink_backup'][yesterdayCollection]

		distinctclicks = int(row['distinctclicks'])
		#print("Connection SG backup for clicks")
		distinctClicksCur = shortcodeCollection.aggregate( [{"$match" : {"$and" : [{"shortcode":row['shortcode']},{"user_agent" : {"$regex": "Mobile" } }]}},{"$group": {"_id": "$mobileno"}},{"$limit":distinctclicks}], allowDiskUse = True )
		#print("Closing SG Backup for clicks")
		#Closing Mongo Connection
		#mongoClient.close()

		clickDF = pd.DataFrame(distinctClicksCur)
		print(clickDF)
		clickDF.to_csv("/home/charanreddy/Documents/pymongo_practice/Myntra Project/newAll.csv",sep=",",index=None, header=False,encoding='utf-8',mode='a')
		print("------------------------------")

	except pymongo.errors.AutoReconnect as e:
		print("PyMongo auto-reconnecting...",str(e))

if __name__ == "__main__":
	if len(sys.argv) > 1  and str(sys.argv[1]) == 'GENERATE':
		results = ""
		mongoClient = pymongo.MongoClient(cg.MONGODB_URI)
		summaryCollection =  mongoClient['global_summary_details']['myntra_summary_temp']

		cursor = summaryCollection.find({"campaigndate":yesterday})
		results = list(cursor)
		print(results)
		

		mongoClient.close()
		if len(results) != 0:
			#generatedSummaryDetails = []
			for row in results:
				print(row)
				if row['shortcode'] !="NA":
					data = fnGenerateLogsInNewFormat(row)
			print("Execution Completed")
		else:
			print("No Campaigns")
	else:
		print("Access Denied")