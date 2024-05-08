from pymongo import MongoClient

mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['db_ecommerce']
orders_collection = mongo_db['Orders']

order_id = 'B-25601'

search_doc = {'_id': order_id}
result = orders_collection.find_one(search_doc)

print(result)

mongo_client.close()
