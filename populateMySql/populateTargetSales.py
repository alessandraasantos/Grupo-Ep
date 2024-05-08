import csv
import mysql.connector
import os


current_dir = os.path.dirname(os.path.abspath(__file__))
grupo_ep_dir = os.path.dirname(current_dir)
csv_file_path = os.path.join(grupo_ep_dir, 'Excell', 'Sales_target.csv')


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
        month_of_order_date = row[0]
        category = row[1]
        target = float(row[2])  
        query = "INSERT INTO target_sales (month_of_order_date, category, target) VALUES (%s, %s, %s)"
        cursor.execute(query, (month_of_order_date, category, target))


cnx.commit()
cursor.close()
cnx.close()
