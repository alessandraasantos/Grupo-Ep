from pymongo import MongoClient
import mysql.connector
from datetime import datetime

try:
    mongo_client = MongoClient('mongodb://localhost:27017/')
    mongo_db = mongo_client['db_ecommerce']
    orders_collection = mongo_db['Orders']


    mysql_connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='root',
        database='db_ecommerce'
    )


    cursor = mysql_connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM Orders")
    mysql_orders_data = cursor.fetchall()


    orders_data = []
    for order in mysql_orders_data:
        order_date = datetime.combine(order['order_date'], datetime.min.time())

        order_data = {
            '_id': order['order_id'],
            'order_date': order_date,
            'customer_name': order['customer_name'],
            'state': order['state'],
            'city': order['city']
        }
        orders_data.append(order_data)

    orders_collection.insert_many(orders_data)
    print("Data inserted successfully into MongoDB Orders collection.")

except Exception as e:
    print("An error occurred:", str(e))

finally:
    if 'mongo_client' in locals():
        mongo_client.close()
    if 'mysql_connection' in locals():
        mysql_connection.close()
