import csv
import mysql.connector
from datetime import datetime


cnx = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='db_ecommerce'
)
cursor = cnx.cursor()

with open(r'C:\Users\marcu\Downloads\archive\List of Orders.csv', newline='') as csvfile:
    reader = csv.reader(csvfile)


    next(reader)


    for row in reader:
        
        if not any(row):
            continue  
        
        order_id = row[0]
        order_date_str = row[1]
        

        if not order_id:
            continue  
        

        order_date = datetime.strptime(order_date_str, '%d-%m-%Y').date()
        
        customer_name = row[2]
        state = row[3]
        city = row[4]
        query = "INSERT INTO Orders (order_id, order_date, customer_name, state, city) VALUES (%s, %s, %s, %s, %s)"
        cursor.execute(query, (order_id, order_date, customer_name, state, city))


cnx.commit()
cursor.close()
cnx.close()
