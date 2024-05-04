import csv
import mysql.connector
from datetime import datetime

# Connect to MySQL database
cnx = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='db_ecommerce'
)
cursor = cnx.cursor()

# Open Orders CSV file
with open(r'C:\Users\marcu\Downloads\archive\List of Orders.csv', newline='') as csvfile:
    reader = csv.reader(csvfile)

    # Skip header row
    next(reader)

    # Iterate over rows and insert into Orders table
    for row in reader:
        # Check if the row is empty (contains only empty strings)
        if not any(row):
            continue  # Skip empty row
        
        order_id = row[0]
        order_date_str = row[1]
        
        # Check if the order ID is empty
        if not order_id:
            continue  # Skip row if order ID is empty
        
        # Parse and format the date
        order_date = datetime.strptime(order_date_str, '%d-%m-%Y').date()
        
        customer_name = row[2]
        state = row[3]
        city = row[4]
        query = "INSERT INTO Orders (order_id, order_date, customer_name, state, city) VALUES (%s, %s, %s, %s, %s)"
        cursor.execute(query, (order_id, order_date, customer_name, state, city))

# Commit changes and close connection
cnx.commit()
cursor.close()
cnx.close()
