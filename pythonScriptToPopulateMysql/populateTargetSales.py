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

# Open CSV file with raw string or double backslashes
with open(r'C:\Users\marcu\Downloads\archive\Sales target.csv', newline='') as csvfile:
    reader = csv.reader(csvfile)

    # Skip header row
    next(reader)

    # Iterate over rows and insert into database
    for row in reader:
        month_of_order_date = row[0]
        category = row[1]
        target = float(row[2])  # Convert target to float
        query = "INSERT INTO target_sales (month_of_order_date, category, target) VALUES (%s, %s, %s)"
        cursor.execute(query, (month_of_order_date, category, target))

# Commit changes and close connection
cnx.commit()
cursor.close()
cnx.close()
