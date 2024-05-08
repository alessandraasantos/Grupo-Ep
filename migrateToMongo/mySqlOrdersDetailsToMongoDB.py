from pymongo import MongoClient
import mysql.connector
from decimal import Decimal
from datetime import datetime

# Connect to MongoDB
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['db_ecommerce']
orders_collection = mongo_db['Orders']

# Connect to MySQL
mysql_connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='db_ecommerce'
)

# Fetch data from MySQL table 'Orders' and 'OrderDetails'
cursor = mysql_connection.cursor(dictionary=True)
cursor.execute("SELECT o.*, od.* FROM Orders o JOIN OrderDetails od ON o.order_id = od.order_id")
mysql_data = cursor.fetchall()

# Prepare data for MongoDB insertion
for row in mysql_data:
    order_id = row['order_id']
    order_date = row['order_date']  # Assuming 'order_date' is already in datetime format
    if not isinstance(order_date, datetime):  # Ensure 'order_date' is datetime
        order_date = datetime.combine(order_date, datetime.min.time())  # Convert to datetime if it's not

    order_data = {
        "_id": order_id,
        "order_date": order_date,
        "customer_name": row['customer_name'],
        "state": row['state'],
        "city": row['city'],
        "order_details": []
    }
    
    amount = float(row['amount'])
    profit = float(row['profit'])

    order_details_data = {
        "_id": row['order_detail_id'],
        "amount": amount,
        "profit": profit,
        "quantity": row['quantity'],
        "category": row['category'],
        "sub_category": row['sub_category']
    }

    order_data["order_details"].append(order_details_data)

    # Check if the order already exists in the collection
    existing_order = orders_collection.find_one({"_id": order_id})
    if existing_order:
        # If the order already exists, append the order details to it
        existing_order["order_details"].append(order_details_data)
        # Update the order in the collection
        orders_collection.update_one({"_id": order_id}, {"$set": existing_order})
    else:
        # If the order doesn't exist, insert the entire document
        orders_collection.insert_one(order_data)

# Close connections
mysql_connection.close()
mongo_client.close()
