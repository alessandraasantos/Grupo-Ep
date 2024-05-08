from pymongo import MongoClient

mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['db_ecommerce']
orders_collection = mongo_db['Orders']
order_details_collection = mongo_db['OrderDetails']
target_sales_collection = mongo_db['target_sales']

order_id = 'B-25601'

pipeline = [
    {"$match": {"_id": order_id}},
    {"$lookup": {
        "from": "OrderDetails",
        "localField": "_id",
        "foreignField": "order_id",
        "as": "order_details"
    }},
    {"$unwind": "$order_details"},
    {"$addFields": {
        "order_month": {"$month": "$order_date"},
        "order_year": {"$year": "$order_date"}
    }},
    {"$lookup": {
        "from": "target_sales",
        "let": {
            "category": "$order_details.category",
            "order_month": "$order_month",
            "order_year": "$order_year"
        },
        "pipeline": [
            {"$match": {
                "$expr": {
                    "$and": [
                        {"$eq": ["$$category", "$category"]},
                        {"$eq": ["$$order_month", {"$month": {"$dateFromString": {
                            "dateString": {"$concat": ["01-", "$month_of_order_date"]},
                            "format": "%d-%b-%Y"
                        }}}]},
                        {"$eq": ["$$order_year", {"$year": {"$dateFromString": {
                            "dateString": {"$concat": ["01-", "$month_of_order_date"]},
                            "format": "%d-%b-%Y"
                        }}}]}
                    ]
                }
            }},
            {"$project": {"target": 1}}
        ],
        "as": "target_sales"
    }},
    {"$unwind": {"path": "$target_sales", "preserveNullAndEmptyArrays": True}},
    {"$project": {
        "_id": 1,
        "order_date": 1,
        "customer_name": 1,
        "state": 1,
        "city": 1,
        "amount": "$order_details.amount",
        "profit": "$order_details.profit",
        "quantity": "$order_details.quantity",
        "category": "$order_details.category",
        "sub_category": "$order_details.sub_category",
        "target": "$target_sales.target"  
    }}
]

result = orders_collection.aggregate(pipeline)

for doc in result:
    print(doc)
