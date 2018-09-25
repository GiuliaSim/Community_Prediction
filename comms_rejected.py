from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
bigDataDB = client['BigData']
clique = bigDataDB['clique']
commsValid = bigDataDB['communities']
commsRejected = bigDataDB['rejected_comms']

for document in clique.find():
	documentId = document['_id']
	if commsValid.find({'_id': documentId}).count() == 0:
		commsRejected.insert(document)

print('DONE')