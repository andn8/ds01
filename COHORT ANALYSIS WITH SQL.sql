-- COHORT RETENTION ANALYSIS WITH SQL - YODY STUDY CASE - DAO NGUYEN AN -  DATA ANANLYST FRESHER
/****** Script for SelectTopNRows command from SSMS  ******/
-- Có tổng cộng 541909 record 
-- Có tổng cộng 397884 record có customer_id # 0 , UnitPrice > 0 , Quantity > 0 
with quantity_unit_price as 
( 
SELECT [InvoiceNo]
      ,[StockCode]
      ,[Description]
      ,[Quantity]
      ,[InvoiceDate]
      ,[UnitPrice]
      ,[CustomerID]
      ,[Country]
  FROM [Cohort Retention Analysis].[dbo].[Online Retail]
 where dbo.[Online Retail].UnitPrice > 0 and dbo.[Online Retail].Quantity > 0 and dbo.[Online Retail].CustomerID != 0 
 ) 
 , dup_check as 
 (
 -- duplicate check 
 select * , ROW_NUMBER() over(partition by InvoiceNo, StockCode, Quantity order by InvoiceDate) dup_flag 
   from quantity_unit_price
   ) 
   -- 5215 duplicate records
   -- 392669 clean data
   select * 
     into #OnlineRetail_main
     from dup_check
	where dup_flag = 1 
--- Clean Data
--- COHORT DATA ANALYSIS
select * from #OnlineRetail_main
--Unique Identifier (CustomerID)
--Initial Start Date (First Invoice Date)
--Revenue Data
select dbo.#OnlineRetail_main.CustomerID , MIN(InvoiceDate) as first_purchase_date,
       DATEFROMPARTS(year(MIN(InvoiceDate)), month( MIN(InvoiceDate)) , 1 )  Cohort_Date
  into #Cohort
  from #OnlineRetail_main
 group by customerID
 -- 4338 record 
 select * 
   from #Cohort
-- Create Cohort index 
select 
      pp.* , 
	  cohort_index = year_diff * 12 + month_diff + 1
  into #cohort_retention
  from 
      (
		select p.* , 
			   year_diff = invoice_year - cohort_year,
			   month_diff = invoice_month - cohort_month
		  from (
				select m.* , 
						c.Cohort_Date , 
						YEAR(m.InvoiceDate) as invoice_year , 
						MONTH(m.InvoiceDate) as invoice_month,
						YEAR(c.Cohort_Date) as cohort_year, 
						MONTH(c.Cohort_date) as cohort_month
				  from #OnlineRetail_main m
				  left join #Cohort c
					on m.CustomerID = c.CustomerID
					) p
      ) pp
 --where pp.CustomerID = 14733
 select * 
   from #cohort_retention

---Pivot Data to see the cohort table
select * 
  into #cohort_pivot
  from ( 
		select distinct
				CustomerID , 
				Cohort_Date , 
				cohort_index
		  from #cohort_retention 
		  ) tb1
pivot(
	Count(CustomerID)
	for Cohort_Index In 
		(
		[1], 
        [2], 
        [3], 
        [4], 
        [5], 
        [6], 
        [7],
		[8], 
        [9], 
        [10], 
        [11], 
        [12],
		[13])

)as pivot_table
--- test 
select * 
  from #cohort_pivot
 order by Cohort_Date 
--- cohort analysis rate
 select Cohort_Date ,
	(1.0 * [1]/[1] * 100) as [1], 
    1.0 * [2]/[1] * 100 as [2], 
    1.0 * [3]/[1] * 100 as [3],  
    1.0 * [4]/[1] * 100 as [4],  
    1.0 * [5]/[1] * 100 as [5], 
    1.0 * [6]/[1] * 100 as [6], 
    1.0 * [7]/[1] * 100 as [7], 
	1.0 * [8]/[1] * 100 as [8], 
    1.0 * [9]/[1] * 100 as [9], 
    1.0 * [10]/[1] * 100 as [10],   
    1.0 * [11]/[1] * 100 as [11],  
    1.0 * [12]/[1] * 100 as [12],  
	1.0 * [13]/[1] * 100 as [13]
from #cohort_pivot
order by Cohort_Date

---DYNAMIC SQL TO CREATE PIVOT TABLE

DECLARE 
    @columns NVARCHAR(MAX) = '',
	@sql     NVARCHAR(MAX) = '';

SELECT 
    @columns += QUOTENAME(cohort_index) + ','
FROM 
    (select distinct cohort_index from #cohort_retention) m
ORDER BY 
    cohort_index;

SET @columns = LEFT(@columns, LEN(@columns) - 1);

PRINT @columns;


-- construct dynamic SQL
SET @sql ='

---# Return number of unique elements in the object
SELECT * 
FROM   
(
	  select distinct
		Cohort_Date,
		cohort_index,
		CustomerID 
	  from #cohort_retention
) t 
PIVOT(
    COUNT(CustomerID) 
    FOR cohort_index IN ('+ @columns +')
) AS pivot_table
order by Cohort_Date


';

-- execute the dynamic SQL
EXECUTE sp_executesql @sql;

   