import csv
import mysql.connector


cnx = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='db_ecommerce'
)
cursor = cnx.cursor()


with open(r'C:\Users\marcu\Downloads\archive\Sales target.csv', newline='') as csvfile:
    reader = csv.reader(csvfile)


    next(reader)

    for row in reader:
        month_of_order_date = row[0]
        category = row[1]
        target = float(row[2])  
        query = "INSERT INTO target_sales (month_of_order_date, category, target) VALUES (%s, %s, %s)"
        cursor.execute(query, (month_of_order_date, category, target))


cnx.commit()
cursor.close()
cnx.close()
