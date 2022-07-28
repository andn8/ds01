-- danh sach 10 san pham bat dau tu dong thu 2ten (A-Z) , gia san pham theo thu tu giam dan 

select Product_name ,list_price 
  from production.products 
 order by list_price desc , product_name 
 offset 2 rows
 fetch first 10 rows only 
 
  -- 10 san pham dat nhat
 
 select top 10 
         product_name ,list_price  
    from production.products 
   order by list_price desc ;
   
   -- danh sach 3 san pham gia dat nhat ( su dung WITH TIES ) , se tra ve nhieu san pham hon neu co gia tuong tu 3 san pham dat nhat
   
   select top 3 with ties 
         product_name,list_price
	 from production.products 
	order by list_price desc 
	
	-- danh sach khach hang bo trong sdt 
	
	select customer_id, first_name , last_name , phone 
	  from sales.customers
	 where phone is null 
	 order by first_name 
	 
	 -- danh sach khach hang co sdt 
	 
	 select customer_id, first_name , last_name , phone 
	  from sales.customers
	 where phone is not null 
	 order by first_name
	
	-- danh sach 3 khach hang co ki tu dau tien cua last_name la Y or Z 
	 select customer_id,first_name ,last_name , phone 
	  from sales.customers
	 where last_name like '[YZ]%'
	 order by first_name 
	 offset 0 rows
	 fetch next 3 rows only 
	 -- danh sach 3 khach hang khong co ki tu dau tien cua last_name la Y or Z 
	 
	 select customer_id,first_name ,last_name , phone 
	  from sales.customers
	 where last_name like '[^Y-X]%'
	 order by first_name 
	 offset 0 rows 
	 fetch next 3 rows only 
	 
	 --tao 2 bang hr.candidates va hr.employees 
	 
	 CREATE SCHEMA hr;
GO
CREATE TABLE hr.candidates(
    id INT PRIMARY KEY IDENTITY,
    fullname VARCHAR(100) NOT NULL
);

CREATE TABLE hr.employees(
    id INT PRIMARY KEY IDENTITY,
    fullname VARCHAR(100) NOT NULL
);
INSERT INTO 
    hr.candidates(fullname)
VALUES
    ('John Doe'),
    ('Lily Bush'),
    ('Peter Drucker'),
    ('Jane Doe');


INSERT INTO 
    hr.employees(fullname)
VALUES
    ('John Doe'),
    ('Jane Doe'),
    ('Michael Scott'),
    ('Jack Sparrow');

-- tim ung tien va nhan vien co fullname giong nhau 

SELECT  
    c.id candidate_id,
    c.fullname candidate_name,
    e.id employee_id,
    e.fullname employee_name
FROM 
    hr.candidates c
    INNER JOIN hr.employees e 
        ON e.fullname = c.fullname;
 
-- khach hang dat tren 2 san pham trong nam 
SELECT
    customer_id,
    YEAR (order_date),
    COUNT (order_id) order_count
FROM
    BikeStore.sales.orders
GROUP BY
    customer_id,
    YEAR (order_date)
HAVING
    COUNT (order_id) >= 2
ORDER BY
    customer_id;
-- tim kiem  san pham (item_id) ban thu ve lon hon 20000 vnd 
select order_id, sum(quantity*list_price*(1-discount)) net_value
  from BikeStore.sales.order_items
 group by order_id 
having sum(quantity*list_price*(1-discount))>20000 
 order by net_value
 --Subquery 
-- don hang cua nhung khac hang o 'new york' 
select order_id,month(order_date) as order_month,customer_id 
  from BikeStore.sales.orders
 where customer_id in ( select customer_id
                          from BikeStore.sales.customers
						 where city = 'New York')
 order by order_date desc;
 -- truy van brand_id co brand_name = 'Strider' or 'Trek' 
 select brand_id from BikeStore.production.brands
  where brand_name in( 'Strider', 'Trek')
-- tinh average list_price cua brand_name ='Strider' or 'Trek' 
select AVG(list_price) as list_price_avg 
  from BikeStore.production.products
 where brand_id in (select brand_id 
                      from BikeStore.production.brands
                     where brand_name in( 'Strider', 'Trek')
-- truy van sam pham co list_price hon lon gia trung binh cua nhung brand_name ='Strider' or 'Trek' 
select product_name , list_price 
  from BikeStore.production.products
 where list_price > ( select AVG(list_price) as list_price_avg 
                        from BikeStore.production.products
                       where brand_id in (select brand_id 
                                            from BikeStore.production.brands
                                           where brand_name in( 'Strider', 'Trek')))
 order by list_price desc ; 
 -- truy van thoi gian(order_date) , order_id , max_list_price trong tung  order_id 
 select order_id , order_date , ( select max(list_price ) 
                                    from BikeStore.sales.order_items i 
                                   where i.order_id=o.order_id) max_list_price 
   from BikeStore.sales.orders o 
  order by order_date desc 
  -- tim tat ca san pham  'mountain bikes' or 'road bikes' duoc ban 
  select product_id,product_name
    from BikeStore.production.products
   where category_id in ( 
                          select category_id
                            from BikeStore.production.categories
                           where category_name in ( 'Mountain Bikes','Road Bikes'))
--truy van nhung san pham ban nhieu hon 2 cai 
select product_name , list_price 
  from BikeStore.production.products
 where product_id = any ( select product_id 
                           from BikeStore.sales.order_items
						  where quantity >= 2 ) 
 order by product_name; 
 -- cach khac : truy van nhung san pham ban nhieu hon 2 cai 
 select product_name , list_price 
  from BikeStore.production.products
 where product_id in ( select product_id 
                           from BikeStore.sales.order_items
						  where quantity >= 2 ) 
 order by product_name;
 -- danh sach san pham co list_price lon hon bat ki avg (list_price) cua tung brand
 SELECT
    product_name,
    list_price
FROM
    bikeStore.production.products
WHERE
    list_price >= ANY (
        SELECT
             AVG (list_price) avg_list_price 
        FROM
            BikeStore.production.products
        GROUP BY
            brand_id 
    ) 
 -- danh sach san pham lon hon tat ca avg(list_price ) cua cac brand 
 SELECT
    product_name,
    list_price
FROM
    BikeStore.production.products
WHERE
    list_price >= ALL (
        SELECT
            AVG (list_price)
        FROM
            Bikestore.production.products
        GROUP BY
            brand_id
    )
-- danh sach khach hang mua hang nam 2017 
select customer_id,first_name+' '+last_name as full_name
  from BikeStore.sales.customers c 
 where exists ( select customer_id
                  from BikeStore.sales.orders o
				 where o.customer_id =c.customer_id and year(order_date) ='2017')
order by full_name
 -- so luong don hang cua nhan vien ban hang 
select staff_id, count(order_id) as order_count
  from BikeStore.sales.orders
 group by staff_id
 -- tinh trung binh so don hang cua tat ca nhan vien 
 select AVG(order_count) as order_avg 
   from ( select staff_id, count(order_id) as order_count
            from BikeStore.sales.orders
           group by staff_id) t 
   -- so luong khach hang ( id = 1;2) , dat hang theo customer_id, year 
  select customer_id , year(order_date) order_year , COUNT(order_id) order_placed 
    from BikeStore.sales.orders
   where customer_id in (1,2 ) 
   group by customer_id, YEAR(order_date) 
   order by customer_id
   /* khi muốn tham chiếu 1 cột hoặc biểu thức mà không có trong danh sách của Group by thì phải sử dụng đầu vào là 1 Aggregate Function . nếu không , 
    sẽ xảy ra lỗi vì không đảm bảo cột , biểu thức sẽ trả lại giá trị đơn duy nhất cho mỗi Group */ 
--sai
SELECT
    customer_id , 
    YEAR (order_date) order_year,
    order_status
FROM
    BikeStore.sales.orders
WHERE
    customer_id IN (259, 2)
GROUP BY
    customer_id,
    YEAR (order_date)
	
ORDER BY
    customer_id;
-- dung 
SELECT
    customer_id,
    YEAR (order_date) order_year,
    order_status
FROM
    BikeStore.sales.orders
WHERE
    customer_id IN (259, 2)
GROUP BY
    customer_id,
    YEAR (order_date),
	order_status
ORDER BY
    customer_id;
-- trung binh gia cua cac thuong hieu san pham nam 2018 , theo thu tu list_price_avg giam dan 
select b.brand_name, AVG(list_price ) list_price_avg  
  from BikeStore.production.products p
 inner join BikeStore.production.brands b on b.brand_id=p.brand_id
 where model_year = 2018
 group by b.brand_name
 order by list_price_avg desc
-- tinh tong so tien theo order_id 
SELECT
    order_id,
    SUM (
        quantity * list_price * (1 - discount)
    ) net_value
FROM
   BikeStore.sales.order_items
GROUP BY
    order_id;
-- khach hang co it nhat 2 order tren nam 
select customer_id , YEAR(order_date) , COUNT(order_id) order_count 
  from BikeStore.sales.orders
 group by customer_id , YEAR(order_date) 
 having count(order_id)>=2 
  order by customer_id
-- truy van nhung order_id co gia tong gia tri don hang > 20000
select order_id, round(SUM(
                     quantity*list_price*(1-discount)
					),2)net_value
  from BikeStore.sales.order_items
 group by order_id 
 having SUM(quantity*list_price*(1-discount)) > 20000
  order by net_value desc 
  -- truy van nhung danh muc san pham ( category_id ) co list_price_max > 4000 hoac list_price_min < 500
select category_id, MAX(list_price) as price_max , MIN(list_price) as price_min
  from BikeStore.production.products
 group by category_id 
 having MAX(list_price)> 4000 or MIN(list_price)<500 ;
 -- truy van danh muc san pham ( category_id ) co list_price_avg thuoc khoang ( 500;1000) 
select category_id,AVG(list_price) as price_avg 
  from BikeStore.production.products
 group by category_id 
having AVG(list_price) between 500 and 1000
 order by price_avg desc 
-- tao new table co ten la ngocphuongngaongao 
select b.brand_name as brand , c.category_name as category , p.model_year , 
       ROUND(sum(quantity*i.list_price *(1-discount)),0) as total_sales into ngocphuongngaongao
  from BikeStore.sales.order_items i
 inner join BikeStore.production.products p on p.product_id = i.product_id
 inner join BikeStore.production.brands b on b.brand_id=p.brand_id
 inner join BikeStore.production.categories c on c.category_id=p.category_id
 group by b.brand_name , c.category_name, p.model_year
 order by b.brand_name , c.category_name ,p.model_year 
-- goi table ngocphuongngaongao , voi dieu kien total_sales > 100.000
select * from ngocphuongngaongao
 where total_sales > 100000
 order by total_sales desc
-- truy van brand , category co tong total_sales lon hon 100.000
select brand,category,sum(total_sales) as sales
  from ngocphuongngaongao
 group by brand , category
having sum(total_sales)>100000
 order by sales desc
 --truy van tong total_sales  > 150.000 theo brand 
select brand, round(SUM(total_sales),4) as ngocphuongxinhgai
  from ngocphuongngaongao
 group by brand 
having SUM(total_sales) > 150000
 order by ngocphuongxinhgai desc 
 -- truy van tong total_sales > 100.000 theo category
select category , SUM(total_sales) as ngocphuonghamhap 
  from ngocphuongngaongao
 group by category
having SUM(total_sales)>100000
 order by ngocphuonghamhap asc 
 -- tinh tong so tien 
 select sum(total_sales) as ds01thichngocphuong
   from ngocphuongngaongao
-- su dung union all de nhom cac truy van con phia tren 
SELECT              -- nhom theo brand , category
    brand,
    category,
    SUM (total_sales) sales
FROM
   ngocphuongngaongao
GROUP BY
    brand,
    category
UNION ALL --- cau lenh gop cac cau lenh 
SELECT    -- nhom theo brand
    brand,
    NULL,
    SUM (total_sales) sales
FROM
  ngocphuongngaongao
GROUP BY
    brand
UNION ALL
SELECT  ---- nhom theo category 
    NULL,
    category,
    SUM (total_sales) sales
FROM
   ngocphuongngaongao
GROUP BY
    category
UNION ALL
SELECT --- khong nhom 
    NULL,
    NULL,
    SUM (total_sales)
FROM
   ngocphuongngaongao
ORDER BY brand, category;

-- su dung group set de toi uu truy van 
SELECT
	brand,
	category,
	SUM (total_sales) sales
FROM
	ngocphuongngaongao
GROUP BY
	GROUPING SETS (
	     (),
		(brand, category),
		(brand),
		(category)
		
	)
ORDER BY
	brand,
	category;
-- Grouping fucntion 
select GROUPING(brand) as brand_group ,
       GROUPING(category) as cat_group,
	   brand,category, SUM(total_sales) 
  from ngocphuongngaongao
 group by 
    grouping sets ( 
	    ( brand , category) ,
		(category),
		(brand),
		()
	)
  order by brand , category;
  /* giai tich : Grouping fucntion trả về 1 tức là ô đó đã aggregated ( được tổng hợp để tính toán hàm Sum) , trả về 0 tức là ô đó chưa được 
    aggregated */ 
-- su dung cube thay the cho Group Set 
select brand , category ,SUM(total_sales) as phuongham 
  from ngocphuongngaongao
 group by 
   cube(brand,category); /* same query : 4 group ser : (brand, category) , (brand ) , (category) , () */
-- truy van chi su dung 1 phan cube de giam so luong Group set duoc tao ra "truy van [01]"
select brand , category , SUM(total_sales) as tiaaha 
  from ngocphuongngaongao
 group by 
  brand , cube(category)
-- truy van tra dung ket qua giong truy van tren "@truy van [01] "
select brand , category, SUM(total_sales) as ds01 
  from ngocphuongngaongao
 group by 
   GROUPING sets (
       (brand , category) ,
	   (brand)
	   )
-- truy van su dung rollup trong Group By 
select brand , category , SUM(total_sales) as "ds01 <3 tiaaha"
  from ngocphuongngaongao
 group by 
  rollup ( category,brand) /* giai thich : cau lenh phan cap : brand > category . tinh doanh thu theo category va category theo tung brand cu the */
-- truy van group theo brand va brand theo tung category cu the 
select brand , category, SUM(total_sales) as cogang
  from ngocphuongngaongao
 group by 
  brand, rollup ( category)
-- Introduction to SQL Server subquery
*********************************************
-- truy van tim customer_id ='New York'
select customer_id
  from BikeStore.sales.customers
 where city = 'New York'
-- truy van nhung order_id , 
select order_id , day(order_date) as 'ngay dat hang', customer_id 
  from BikeStore.sales.orders
 where customer_id in ( select customer_id
                          from BikeStore.sales.customers
                         where city = 'New York') 
 order by order_date
 *******
 ---Nesting subquery
 select product_name,list_price
   from BikeStore.production.products
  where list_price > (
						select AVG(list_price)
						  from BikeStore.production.products
						 where brand_id in ( select brand_id 
											   from BikeStore.production.brands
											  where brand_name = 'Trek' or brand_name = 'Strider' ) )
  order by list_price asc
-- truy van tim san pham co gia dat nhat theo don hang ( order_id) 
select order_id , order_date, 
       ( select MAX(list_price) 
	       from BikeStore.sales.order_items i 
		  where i.order_id = o.order_id) as price_max 
  from BikeStore.sales.orders o 
 order by order_date desc 
-- truy van san pham thuoc category = ( ' Mountain Bike' , 'Road Bike') 
select product_id , product_name
  from BikeStore.production.products
 where category_id in ( select category_id
                          from BikeStore.production.categories
						 where category_name in ('Mountain Bikes' , 'Road Bikes') )
-- truy van nhung khach hang da mua hang nam = 2017 

select customer_id, first_name +' '+last_name as fullname, email
  from BikeStore.sales.customers c
 where exists ( select customer_id
			      from BikeStore.sales.orders o
			     where o.customer_id=c.customer_id and year(order_date) = '2017') 
--tinh trung binh so order_id cua nhan vien ban hang 
select AVG(order_count) as avg_order_id_staff
  from (
		select staff_id , COUNT(order_id) as order_count
		  from BikeStore.sales.orders
		 group by staff_id ) t
--- danh sach san pham co gia lon hon gia avg cua tat ca brand 
select product_name , list_price
  from BikeStore.production.products
 where list_price >= all(
						select AVG(list_price) as price_avg
						  from BikeStore.production.products
						 group by brand_id)
 order by list_price 
--- danh sach san pham co gia lon hon gia avg cua 1 trong tat ca brand      
select product_name , list_price
  from BikeStore.production.products
 where list_price >= any(
						select AVG(list_price) as price_avg
						  from BikeStore.production.products
						 group by brand_id)
  order by list_price;
****************************************************************************


/* Introduction to the SQL Server correlated subquery */
--truy van san pham theo tung danh muc ( category) co gia lon nhat 
select product_name , category_id , list_price 
  from BikeStore.Production.products p1 
 where list_price in ( 
						select max(list_price)
						  from BikeStore.production.products p2 
						 where p2.category_id = p1.category_id
						 group by category_id )
 order by product_name , category_id ;
 --- danh sach khach hang order lon hon 2 san pham 
 select customer_id , last_name + ' '+ first_name as fullname , email
   from bikestore.sales.customers c
  where exists (
				 select count(*) as order_count
				   from bikestore.sales.orders o
				  where o.customer_id = c.customer_id 
				  group by customer_id
				 having count(*)> 2 )
  order by customer_id , fullname;
  -- danh sach khach hang order > 2 san pham ( cach khac ) 
select customer_id , last_name + ' '+ first_name as fullname , email
from bikestore.sales.customers
where customer_id in ( 
						select customer_id 
							from bikestore.sales.orders
						group by customer_id 
						having count(order_id) > 2 )
order by customer_id , fullname;
-- danh sach nhung san pham khong duoc ban 
select product_id
  from bikestore.production.products
except
select product_id 
  from bikestore.sales.order_items
-- Sử dụng CTE để truy vấn doanh thu bán hàng của nhân viên trong năm 2018 

with cte_saleamount_ngocphuongxinhgai ( staff,sales,year ) as ( 
     select first_name + ' ' + last_name as fullname , 
	       sum(quantity*list_price*(1-discount)), 
		   year(order_date)
	   from bikestore.sales.orders o
	  inner join bikestore.sales.order_items i on i.order_id = o.order_id 
	  inner join bikestore.sales.staffs s on s.staff_id = o.staff_id
	  group by first_name + ' ' + last_name  ,
                year(order_date)
		)
     select staff , sales
	   from cte_saleamount_ngocphuongxinhgai
	  where year = 2018
	  order by sales desc
-- tinh trung binh so don dat hang duoc tao boi cac nhan vien su dung CTE 
with ngocphuongwithlove as ( 
     select staff_id , count(*)  order_count 
	   from bikestore.sales.orders
	  where year(order_date) = 2018 
	  group by staff_id 
	  )
select avg(order_count) as count_staff_avg 
  from ngocphuongwithlove
-- su dung CTE de tra ve doanh thu va so luong hang ban theo product category 
with ngocphuongwithlove ( category_id , category_name , product_count )  -- CTE 1 : tinh so luong order theo product category 
  as ( 

		select c.category_id , c.category_name , count(product_id) 
		  from bikestore.production.products p 
		 inner join bikestore.production.categories c 
			on c.category_id = p.category_id 
		 group by c.category_id , c.category_name
 ) ,
  ngocphuongngaongo ( category_id ,sales ) --- CTE 2 : so doanh so theo product category
   as ( 
         select p.category_id , 
		        sum(i.quantity*i.list_price*(1-i.discount))
		   from bikestore.sales.order_items i 
		  inner join bikestore.production.products p 
		     on p.product_id = i.product_id 
		  inner join bikestore.sales.orders o
		     on o.order_id = i.order_id 
		  where order_status =4 -- completed 
		  group by p.category_id 
		 )
select p1.category_id , p1.category_name ,p1.product_count ,p2.sales 
  from ngocphuongwithlove p1
 inner join ngocphuongngaongo p2
    on p2.category_id =p1.category_id
 order by p1.category_name 
************************************************************
-- sử dụng đệ quy trả lại các thứ trong tuần 
with cte_numbers( n, weekday ) 
  as ( 
      select 0, datename(dw,0)
	  union all
	  select n+1 , datename(dw,n+1) 
	    from cte_numbers
	   where n<6
	   )
select weekday 
  from cte_numbers;
-- Recursive CTE lấy id của nhân viên và quản lý của họ , quản lý cấp cao nhất thì không có quản lý ( giá trị NULL ) 
with cte_org as ( 
 select staff_id , first_name , manager_id 
   from bikestore.sales.staffs
  where manager_id is null
  union all 
 select e.staff_id , e.first_name , e.manager_id 
   from bikestore.sales.staffs e 
  inner join cte_org o
     on o.staff_id=e.manager_id 
	 )
 select * 
   from cte_org

**********************************
-- su dung pivot 
-- so luong san luong theo danh muc san pham , theo nam 
select category_name ,count(product_id) product_count , model_year
  from bikestore.production.categories c 
 inner join bikestore.production.products p
    on p.category_id=c.category_id 
 group by category_name , model_year
-- su dung pivot cho query tren 
select * from 
( 
  select category_name ,product_id , model_year
    from bikestore.production.categories c 
   inner join bikestore.production.products p
      on p.category_id=c.category_id
) t 
pivot ( 
      count(product_id ) 
	  for category_name in ( 
	   [Children Bicycles],
		[Comfort Bicycles],
		[Cruisers Bicycles],
		[Cyclocross Bicycles],
		[Electric Bikes],
		[Mountain Bikes],
		[Road Bikes])
		) as pivot_table ;
----- khai bao bien la @comlumn la category_name 
DECLARE 
    @columns NVARCHAR(MAX) = '';

SELECT 
    @columns += QUOTENAME(category_name) + ','
FROM 
    bikestore.production.categories
ORDER BY 
    category_name;

SET @columns = LEFT(@columns, LEN(@columns) - 1);

PRINT @columns;


---Dynamic pivot tables
declare 
  @columns nvarchar(max) = '' , 
  @sql     nvarchar(max) = '' ;
  -- select the category names 
select
  @columns += quotename(category_name) + ','
  from bikestore.production.categories 
 order by category_name ; 
 -- remove the last name 
 set @columns = left(@columns , len(@columns ) -1 );
 -- construct dynamic SQL 
 set @sql = '
              select * from 
							( 
							  select category_name ,product_id , model_year
								from bikestore.production.categories c 
							   inner join bikestore.production.products p
								  on p.category_id=c.category_id
							) t 
							pivot ( count(product_id)  
							        for category_name in ( '+ @columns +')
									) as pivot_table ; ';
--execute the dynamic SQL 
execute sp_executesql @sql ;
        