import os
import csv
import mysql.connector

current_dir = os.path.dirname(os.path.abspath(__file__))
grupo_ep_dir = os.path.dirname(current_dir)
csv_file_path = os.path.join(grupo_ep_dir, 'dataset', 'Order_Details.csv')

cnx = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='db_ecommerce'
)
cursor = cnx.cursor()

with open(csv_file_path, newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)

    for row in reader:
        if not any(row):
            continue
        
        order_id = row[0]
        amount = float(row[1])
        profit = float(row[2])
        quantity = int(row[3])
        category = row[4]
        sub_category = row[5]
        
        query = "INSERT INTO OrderDetails (order_id, amount, profit, quantity, category, sub_category) VALUES (%s, %s, %s, %s, %s, %s)"
        cursor.execute(query, (order_id, amount, profit, quantity, category, sub_category))

cnx.commit()
cursor.close()
cnx.close()
