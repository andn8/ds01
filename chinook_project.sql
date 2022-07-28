-- tỉ lệ số hóa đơn bán được trên tổng số hóa đơn , theo từng quốc gia 
SELECT BillingCountry , round ( count(*)*100.0/ (
                                         SELECT count(*) 
										   FROM invoices ) ,2 ) as sales_pop 
  FROM invoices
 GROUP by BillingCountry
 ORDER by sales_pop 
 LIMIT 5 ; 
 -- tỉ lệ số lượng khách hàng trên tổng số khách hàng , theo từng quốc gia 
 SELECT CustomerId , round( count(*)*100.0 / 
                                            ( SELECT count(*) 
									            FROM invoices ) , 2 ) as cus_count_percentage                                
   FROM invoices
  GROUP by CustomerId 
  ORDER by cus_count_percentage desc
  LIMIT 5 ;
 -- phần trăm doanh thu bán được theo từng quốc gia 
 SELECT BillingCountry , round( sum(total)*100.0 / 
                                           ( select sum(total) 
										       from invoices ) , 2) as  total_amount
   FROM invoices
  GROUP by BillingCountry
  ORDER by total_amount desc 
  LIMIT 5 ; 
-- số lượng hóa đơn có giá trị lớn hơn giá trị trung bình của các hóa đơn
SELECT count(*) 
  FROM invoices
 WHERE total > ( SELECT avg(total)
                    FROM invoices ) 

		
 -- truy vấn những khách hàng có giá trị lớn hơn 0.75 giá trị hóa đơn lớn nhất 
SELECT c.CustomerId , "FirstName" || " " || "LastName" as fullname , total
  FROM customers c  
  JOIN invoices i 
    on i.CustomerId = c.CustomerId
 WHERE total > (SELECT max(total)*0.75 as ngocphuong from invoices) 
 -- khach hang co avg doanh thu >= avg customers_id = 6 ( khach hang dem lai doanh thu lon nhat ) 
 select CustomerId , avg(total) as customer_avg
  FROM invoices
 GROUP by CustomerId
HAVING avg(total) >=(
					 SELECT avg(total) as ngocphuong
					   from invoices 
					  WHERE CustomerId = 6 )
-- đếm số tracks có MediaTypeId = %MPEG%
select count(*) as ds01  
  from tracks
 where MediaTypeId in ( 
						select MediaTypeId
						  from media_types
						 where name like '%MPEG%' ) 
--- Truy vấn invoices của khách hàng có tên A% ( bắt đầu bằng A ) 
SELECT * 
  FROM invoices 
 WHERE CustomerId IN ( 
                        SELECT CustomerId
                          FROM customers 
                         WHERE FirstName LIKE 'A%') 
-- truy vấn khách hàng mua hàng trên 30 USD
SELECT FirstName , LastName 
  FROM customers 
 WHERE CustomerId NOT in ( 
                           SELECT CustomerId 
						     FROM invoices
							GROUP by CustomerId 
						   HAVING sum(total) < 30.0 ) 
-- Doanh số mỗi khách hàng đem lại 
SELECT FirstName , LastName , sum(total)
  FROM customers c 
  JOIN invoices i 
    on i.CustomerId = c.CustomerId
 GROUP by c.CustomerId 
 -- Doanh thu mỗi khách hàng đem lại 
select FirstName , LastName , total_amount
  FROM Customers c 
  join ( SELECT CustomerId , sum(total) as total_amount
           FROM invoices
		  GROUP by CustomerId ) i
	on i.CustomerId = c.CustomerId 
-- tính số đơn hàng / số khách hàng , theo từng quốc gia 
SELECT Country , sodonhang/sokhachhang as ds01 
  FROM ( 
		SELECT BillingCountry , count(*) as sodonhang
		  FROM invoices
		 GROUP by BillingCountry ) i 
  JOIN (
		SELECT Country , count(*) as sokhachhang
		  FROM customers
		 GROUP by Country ) c 
	on c.Country = i.BillingCountry 
 ORDER by ds01 desc
 -- truy vấn những nhân viên bán được trên 20 usd 
SELECT LastName , FirstName 
  FROM employees
 WHERE EmployeeId in (  
						 SELECT SupportRepId 
						   FROM customers
						  WHERE CustomerId in ( 
												 SELECT CustomerId
												   FROM invoices
												  GROUP by CustomerId 
												 HAVING sum(total) > 20  ) ) 
--- tính tổng doanh thu theo hóa đơn đối với những sản phầm 'Metal' và của những khách hàng 'USD' , tổng số phút của các bài hát ( sản phẩm ) trong hóa đơn 
SELECT tr.InvoiceId as InvoiceId ,
       sum(tr.quantity*unitprice) as total ,
	   sum(tr.Milliseconds)/1000.0/60 as sophut
  FROM (
		SELECT i.* , t.Milliseconds , t.GenreId
		  FROM invoice_items i 
		  JOIN tracks t 
			on	t.TrackId = i.TrackId 
		 WHERE i.InvoiceId in ( SELECT InvoiceId
								  FROM invoices
								 WHERE BillingCountry = 'USA' ) 
		) tr
 WHERE tr.GenreId  in ( SELECT GenreId
                          FROM genres
						 WHERE name like '%Metal%')
 GROUP by tr.InvoiceId	
--- test đệ qui 
WITH RECURSIVE 
messages_table (id, message) AS (
 SELECT 1, 'ngoc phuong'
  UNION
 SELECT 2, 'with love !'
  UNION
 SELECT 3, 'ds01<3tiaaha.'
),

for_you_table (id, message, next_id) AS (
 SELECT id, message, id + 1
   FROM messages_table
  WHERE id = 1

  UNION ALL

 SELECT mt.id, fyt.message || ' ' || mt.message, mt.id + 1
   FROM messages_table mt
   JOIN for_you_table fyt
     ON mt.id = fyt.next_id
)

SELECT message
  FROM for_you_table
  WHERE id = 3; 
