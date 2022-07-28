/* Creen 3 */
-- Table Descriptions 
SELECT 'Customers' AS table_name, 
       13 AS number_of_attribute,
       COUNT(*) AS number_of_row
  FROM Customers
UNION ALL
SELECT 'products' as table_name,
        9 as number_of_attribute,
		count(*) as number_of_row
  FROM products
UNION ALL

SELECT 'ProductLines' AS table_name, 
       4 AS number_of_attribute,
       COUNT(*) AS number_of_row
  FROM ProductLines

UNION ALL

SELECT 'Orders' AS table_name, 
       7 AS number_of_attribute,
       COUNT(*) AS number_of_row
  FROM Orders

UNION ALL

SELECT 'OrderDetails' AS table_name, 
       5 AS number_of_attribute,
       COUNT(*) AS number_of_row
  FROM OrderDetails

UNION ALL

SELECT 'Payments' AS table_name, 
       4 AS number_of_attribute,
       COUNT(*) AS number_of_row
  FROM Payments

UNION ALL

SELECT 'Employees' AS table_name, 
       8 AS number_of_attribute,
       COUNT(*) AS number_of_row
  FROM Employees

UNION ALL

SELECT 'Offices' AS table_name, 
       9 AS number_of_attribute,
       COUNT(*) AS number_of_row
  FROM Offices;
 
 /* Creen 4 */ 
 -- low stock 
 -- truy vấn sản phẩm tồn kho nhiều nhất 
 SELECT productCode , round( sum(quantityOrdered) * 1.0 / ( SELECT quantityInStock
                                                              FROM products p
													         WHERE p.productCode = od.productCode) , 2 ) as low_stock
   FROM orderdetails od 
  GROUP by productCode
  ORDER	by low_stock 
  LIMIT 10 ; 
 --- low stock 2 : 
 SELECT t.productCode , round( sum(t.quantityOrdered)*1.0/t.quantityInStock , 2 ) as low_stock
   FROM	(
         SELECT od.*, p.quantityInStock 
		   FROM orderdetails od
		   JOIN products p
			 on od.productCode = p.productCode ) t 
  GROUP by t.productCode
  ORDER by low_stock 
  LIMIT 10 ;   
-- Product Performance 
WITH ngocphuong as ( 
SELECT t.productCode
  FROM (
		SELECT productCode , sum(quantityOrdered * priceEach) as prod_perf
		  FROM orderdetails od 
		 GROUP by productCode
		 ORDER by prod_perf desc
		 LIMIT 10 
		 ) t
 )
SELECT productCode , productName
  FROM products 
 WHERE productCode in ( SELECT * 
                          FROM ngocphuong)
-- Products Performance 
-- 10 sản phẩm đem lại doanh thu lớn nhất
SELECT productCode, 
       SUM(quantityOrdered * priceEach) AS prod_perf
  FROM orderdetails od
 GROUP BY productCode 
 ORDER BY prod_perf DESC
 LIMIT 10;
-- 10 khách hàng đem lại lợi nhuận lớn nhất 
SELECT o.customerNumber , sum(quantityOrdered * ( priceEach - buyPrice)) as profit 
  FROM products p
  JOIN orderdetails od 
    on od.productCode = p.productCode
  JOIN orders o 
    on o.orderNumber = od.orderNumber
 GROUP by o.customerNumber
 ORDER by profit DESC
 LIMIT 10 ; 
 /* Creen 6 */ 
 -- Truy vấn số lượng khách hàng mới theo tháng và doanh thu khách hàng mới đem lại theo từng tháng ? 
--  lượng khách hàng mới theo chiếm tỉ lệ bao nhiêu trong tổng số khách hàng trong tháng , và doanh thu khách hàng mới/ tổng doanh thu theo tháng ?
-- -- -- ngocphuongwithlove
WITH 
payment_with_YearMonth_table as ( 
SELECT p.* , CAST(substr(paymentDate ,1,4) as INTEGER )*100 + 
       CAST(substr(paymentDate ,6,7) as INTEGER ) as year_month
  FROM payments p 
  ), 
customers_by_month_table as ( 
SELECT p1.year_month , count(*) as sokhachhang , sum(p1.amount) as total 
  FROM payment_with_YearMonth_table p1 
 GROUP by p1.year_month
 ),
 new_customers_by_month_table AS (
 SELECT p1.year_month , count(*) as sokhachhangmoi , sum(p1.amount) as new_total ,
        (SELECT c.sokhachhang
		   FROM customers_by_month_table c
		  WHERE c.year_month = p1.year_month ) as sokhachhang , 
		(SELECT c.total
		   FROM customers_by_month_table c
		  WHERE c.year_month = p1.year_month ) as total
		  
   FROM payment_with_YearMonth_table p1 
  WHERE p1.customerNumber NOT in ( SELECT customerNumber
                                     FROM payment_with_YearMonth_table p2 
								    WHERE p2.year_month < p1.year_month )
  GROUP by p1.year_month
  )
 
  SELECT year_month , round(sokhachhangmoi*100/sokhachhang ,1) as ptram_k_hang_moi , 
         round(new_total*100/total,1) as ptram_total_moi
    FROM new_customers_by_month_table
  /* Creen 6 */ 
  -- Priority Products for restocking ( những sản phẩm có đem lại doanh thu lớn nhất trong những sản phẩm tồn kho nhiều nhất thì sẽ ưu tiên lưu kho  ) 
  WITH 
  low_stock_table as ( 
  SELECT productCode, 
       ROUND(SUM(quantityOrdered) * 1.0/(SELECT quantityInStock
                                           FROM products p
                                          WHERE od.productCode = p.productCode), 2) AS low_stock
  FROM orderdetails od
 GROUP BY productCode
 ORDER BY low_stock
 LIMIT 10
 )
 SELECT productCode , 
        sum(quantityOrdered * priceEach) as prod_perf 
   FROM orderdetails od 
  WHERE productCode in ( SELECT productCode
                           FROM low_stock_table ) 
  
  GROUP by productCode 
  ORDER by prod_perf DESC
  LIMIT 10 ; 
 --- Top 5 khách hàng đem lại lợi nhuận cao nhất 
 WITH 

money_in_by_customer_table AS (
SELECT o.customerNumber, SUM(quantityOrdered * (priceEach - buyPrice)) AS revenue
  FROM products p
  JOIN orderdetails od
    ON p.productCode = od.productCode
  JOIN orders o
    ON o.orderNumber = od.orderNumber
 GROUP BY o.customerNumber
)

SELECT contactLastName, contactFirstName, city, country, mc.revenue
  FROM customers c
  JOIN money_in_by_customer_table mc
    ON mc.customerNumber = c.customerNumber
 ORDER BY mc.revenue DESC
 LIMIT 5;
 -- Top 5 khách hàng đem lại lợi nhuận thấp nhất 
 WITH 

money_in_by_customer_table AS (
SELECT o.customerNumber, SUM(quantityOrdered * (priceEach - buyPrice)) AS revenue
  FROM products p
  JOIN orderdetails od
    ON p.productCode = od.productCode
  JOIN orders o
    ON o.orderNumber = od.orderNumber
 GROUP BY o.customerNumber
)

SELECT contactLastName, contactFirstName, city, country, mc.revenue
  FROM customers c
  JOIN money_in_by_customer_table mc
    ON mc.customerNumber = c.customerNumber
 ORDER BY mc.revenue
 LIMIT 5;
 -- tính lợi nhuận trung bình của khách hàng đem lại 
 WITH money_in_by_customer_table as ( 
 SELECT o.customerNumber , sum( quantityOrdered * ( priceEach - buyPrice)) as revenue 
   FROM products p 
   JOIN orderdetails od 
     on od.productCode =p.productCode 
   JOIN orders o 
     on o.orderNumber = od.orderNumber
  GROUP by o.customerNumber
  ) 
 SELECT avg(mc.revenue) as 'lợi nhuận trung bình' 
   FROM money_in_by_customer_table mc ; 
