CREATE SCHEMA `db_ecommerce` ;
use db_ecommerce;


CREATE TABLE target_sales (
    id_target_sales INT AUTO_INCREMENT PRIMARY KEY,
    month_of_order_date VARCHAR(10),
    category VARCHAR(50),
    target INT,
    order_date DATE
);

select * from target_sales;


SET SQL_SAFE_UPDATES = 0;


UPDATE target_sales
SET order_date = STR_TO_DATE(CONCAT('01-', month_of_order_date), '%d-%b-%y')
WHERE id_target_sales > 0;

CREATE TABLE Orders (
    order_id VARCHAR(255) PRIMARY KEY,
    order_date DATE,
    customer_name VARCHAR(255),
    state VARCHAR(255),
    city VARCHAR(255)
);


CREATE TABLE OrderDetails (
    order_detail_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id VARCHAR(255),
    amount DECIMAL(10, 2),
    profit DECIMAL(10, 2),
    quantity INT,
    category VARCHAR(255),
    sub_category VARCHAR(255),
    FOREIGN KEY (order_id) REFERENCES Orders(order_id)
);








select * from Orders;

select * from OrderDetails;

-- DQLs to validate DATA INTEGRITY 
-- Check for duplicate entries in Orders table:
SELECT order_id, COUNT(*)
FROM Orders
GROUP BY order_id
HAVING COUNT(*) > 1;

SELECT *
FROM OrderDetails
WHERE order_id IS NULL OR amount IS NULL OR profit IS NULL OR quantity IS NULL OR category IS NULL OR sub_category IS NULL;

SELECT o.order_id
FROM Orders o
LEFT JOIN OrderDetails od ON o.order_id = od.order_id
WHERE od.order_id IS NULL;

SELECT od.order_id
FROM OrderDetails od
LEFT JOIN Orders o ON o.order_id = od.order_id
WHERE o.order_id IS NULL;

SELECT order_id, SUM(amount) AS total_amount
FROM OrderDetails
GROUP BY order_id
HAVING total_amount != (SELECT SUM(amount) FROM Orders WHERE Orders.order_id = OrderDetails.order_id);


SELECT   -- Select to validate if its retrieving order details as expected
    o.order_id,
    o.order_date,
    o.customer_name,
    o.state,
    o.city,
    od.amount,
    od.profit,
    od.quantity,
    od.category,
    od.sub_category
FROM 
    Orders o
INNER JOIN 
    OrderDetails od ON o.order_id = od.order_id
WHERE 
    o.order_id = 'B-25601'; 


SELECT 
    o.order_id,
    o.order_date,
    o.customer_name,
    o.state,
    o.city,
    od.amount,
    od.profit,
    od.quantity,
    od.category,
    od.sub_category,
    ts.target
FROM 
    Orders o
INNER JOIN 
    OrderDetails od ON o.order_id = od.order_id
LEFT JOIN 
    target_sales ts ON od.category = ts.category AND
                           MONTH(o.order_date) = MONTH(STR_TO_DATE(CONCAT('01-', ts.month_of_order_date), '%d-%b-%y')) AND 
                           YEAR(o.order_date) = YEAR(STR_TO_DATE(CONCAT('01-', ts.month_of_order_date), '%d-%b-%y'))
WHERE 
    o.order_id = 'B-25601';



