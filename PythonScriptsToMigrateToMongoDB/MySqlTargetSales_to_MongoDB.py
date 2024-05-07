from pymongo import MongoClient
import mysql.connector

# Connect to MongoDB
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['db_ecommerce']  # Replace 'your_mongodb_database' with your MongoDB database name
mongo_collection = mongo_db['target_sales']  # Replace 'your_mongodb_collection' with your MongoDB collection name

# Connect to MySQL
mysql_connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='db_ecommerce'
)  # Replace the placeholders with your MySQL connection details

# Fetch data from MySQL tables
cursor = mysql_connection.cursor(dictionary=True)

# Example: Fetch data from MySQL table 'users'
cursor.execute("SELECT * FROM target_sales")
mysql_targetSaless_data = cursor.fetchall()

# Transform MySQL data into MongoDB documents (this step may involve restructuring the data)

# Example: Transform 'users' data
mongo_targetSales_data = []
for targetSales in mysql_targetSaless_data:
    order_date_iso = targetSales['order_date'].isoformat()
    mongo_targetSales = {
        '_id': targetSales['id_target_sales'],  # Assuming 'id' is the primary key
        'month_of_order_date': targetSales['month_of_order_date'],
        'category': targetSales['category'],
        'target': targetSales['target'],
        'order_date': order_date_iso, 
        # Add more fields as needed
    }
    mongo_targetSales_data.append(mongo_targetSales)

# Insert data into MongoDB
mongo_collection.insert_many(mongo_targetSales_data)


# Close connections
mongo_client.close()
mysql_connection.close()
