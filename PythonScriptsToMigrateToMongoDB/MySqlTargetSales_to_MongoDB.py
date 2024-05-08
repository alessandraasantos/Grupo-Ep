from pymongo import MongoClient
import mysql.connector


mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['db_ecommerce']  
mongo_collection = mongo_db['target_sales']  


mysql_connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='db_ecommerce'
)  


cursor = mysql_connection.cursor(dictionary=True)


cursor.execute("SELECT * FROM target_sales")
mysql_targetSaless_data = cursor.fetchall()


mongo_targetSales_data = []
for targetSales in mysql_targetSaless_data:
    order_date_iso = targetSales['order_date'].isoformat()
    mongo_targetSales = {
        '_id': targetSales['id_target_sales'], 
        'month_of_order_date': targetSales['month_of_order_date'],
        'category': targetSales['category'],
        'target': targetSales['target'],
        'order_date': order_date_iso, 
        
    }
    mongo_targetSales_data.append(mongo_targetSales)


mongo_collection.insert_many(mongo_targetSales_data)


mongo_client.close()
mysql_connection.close()
