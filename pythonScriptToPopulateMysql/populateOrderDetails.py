import csv
import mysql.connector

# Connect to MySQL database
cnx = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='db_ecommerce'
)
cursor = cnx.cursor()

# Open OrderDetails CSV file
with open(r'C:\Users\marcu\Downloads\archive\Order Details.csv', newline='') as csvfile:
    reader = csv.reader(csvfile)

    # Skip header row
    next(reader)

    # Iterate over rows and insert into OrderDetails table
    for row in reader:
        # Check if the row is empty (contains only empty strings)
        if not any(row):
            continue  # Skip empty row
        
        order_id = row[0]
        amount = float(row[1])
        profit = float(row[2])
        quantity = int(row[3])
        category = row[4]
        sub_category = row[5]
        
        query = "INSERT INTO OrderDetails (order_id, amount, profit, quantity, category, sub_category) VALUES (%s, %s, %s, %s, %s, %s)"
        cursor.execute(query, (order_id, amount, profit, quantity, category, sub_category))

# Commit changes and close connection
cnx.commit()
cursor.close()
cnx.close()
