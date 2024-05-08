from pymongo import MongoClient

# Connect to MongoDB
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['db_ecommerce']
orders_collection = mongo_db['Orders']

# Define the order ID to search for
order_id = 'B-25601'

# Search for the document by order ID
search_doc = {'_id': order_id}
result = orders_collection.find_one(search_doc)

# Print the result
print(result)

# Close the MongoDB connection
mongo_client.close()
