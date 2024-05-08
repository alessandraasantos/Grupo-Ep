from pymongo import MongoClient
import mysql.connector
from decimal import Decimal
from datetime import datetime


mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['db_ecommerce']  
order_details_collection = mongo_db['OrderDetails']  


mysql_connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='db_ecommerce'
)  


cursor = mysql_connection.cursor(dictionary=True)
cursor.execute("SELECT od.*, o.order_date, o.customer_name, o.state, o.city FROM OrderDetails od JOIN Orders o ON od.order_id = o.order_id")
mysql_order_details_data = cursor.fetchall()

order_details_data = []
for order_detail in mysql_order_details_data:
  
    amount = float(order_detail['amount'])
    profit = float(order_detail['profit'])


    order_date = datetime.combine(order_detail['order_date'], datetime.min.time())

    order_detail_data = {
        '_id': order_detail['order_detail_id'],  
        'order_id': order_detail['order_id'],
        'amount': amount,
        'profit': profit,
        'quantity': order_detail['quantity'],
        'category': order_detail['category'],
        'sub_category': order_detail['sub_category'],
        'order_date': order_date,
        'customer_name': order_detail['customer_name'],
        'state': order_detail['state'],
        'city': order_detail['city']
        
    }
    order_details_data.append(order_detail_data)


order_details_collection.insert_many(order_details_data)


mysql_connection.close()
mongo_client.close()
