---Advanced DataBase project 

-- Step one, create tables with partitions by age range
CREATE TABLE Customers
    (
    Customer_ID int not null primary key,
    age NUMBER not null,
    job_code int not null,
    marital_status_code int not null,
    edu_level_code int not null,
    balance int not null,
    housing_status_code int not null,
    loan_status_code int not null,
    contact int not null,
    month char(10) not null,
    duration int not null,
    campain int not null,
    pdays int not null,
    previous int not null,
    poutcome int not null,
    results char(3) not null
    )
PARTITION BY RANGE (age)
    (
    PARTITION p1 VALUES less than (30),
    PARTITION p2 VALUES less than (35),
    PARTITION p3 VALUES less than (40),
    PARTITION p4 VALUES less than (45),
    PARTITION p5 VALUES less than (50),
    PARTITION p6 VALUES less than (95)
    );
    

-- this table has no partition or index, used to compare the performances
CREATE TABLE Customers_to_compare
    (
    Customer_ID int not null primary key,
    age NUMBER not null,
    job_code int not null,
    marital_status_code int not null,
    edu_level_code int not null,
    balance int not null,
    housing_status_code int not null,
    loan_status_code int not null,
    contact int not null,
    month char(10) not null,
    duration int not null,
    campain int not null,
    pdays int not null,
    previous int not null,
    poutcome int not null,
    results char(3) not null
    );

-- also used to compare the query performance of partitions. see the diff with customers_to_compare   
CREATE TABLE compare
    (
    Customer_ID int not null primary key,
    age NUMBER not null,
    job_code int not null,
    marital_status_code int not null,
    edu_level_code int not null,
    balance int not null,
    housing_status_code int not null,
    loan_status_code int not null,
    contact int not null,
    month char(10) not null,
    duration int not null,
    campain int not null,
    pdays int not null,
    previous int not null,
    poutcome int not null,
    results char(3) not null
    )
    PARTITION BY RANGE (age)
    (
    PARTITION p1 VALUES less than (30),
    PARTITION p2 VALUES less than (35),
    PARTITION p3 VALUES less than (40),
    PARTITION p4 VALUES less than (45),
    PARTITION p5 VALUES less than (50),
    PARTITION p6 VALUES less than (95)
    );
-- the data excel is provided(ADB customers.csv), import data into sql developer 
    
-- instead of the complicated process of import excel into sql, below is to copy data from customers table to tables customers_to_compare and compare
insert into compare
select * from customers;

INSERT INTO customers_to_compare 
SELECT
*
FROM customers
;

-- Below we create all the necessary tables and insert values (except import jobs excel into table)
CREATE TABLE Jobs
    (
    job_code int not null primary key,
    job_name char(20) not null,
    job_type_code int not null
    );

CREATE TABLE Job_type
    (
    job_type_code int not null primary key,
    job_type char(20) not null
    );
    
CREATE TABLE marital_status
    (
    marital_status_code int not null primary key,
    marital_status char(20) not null
    );


CREATE TABLE edu_level
    (
    edu_level_code int not null primary key,
    edu_level char(20) not null
    ); 

CREATE TABLE housing_status
    (
    housing_status_code int not null primary key,
    housing_status char(20) not null
    ); 
    
CREATE TABLE loan_status
    (
    loan_status_code int not null primary key,
    loan_status char(20) not null
    );     
    
insert all
    into Job_type  values(0,'business')   
    into Job_type values (1,'technician')
    into Job_type values (2,'none')
select * from dual;

insert all
    into marital_status  values(0,'divorced')   
    into marital_status values (1,'single')
    into marital_status values (2,'married')
select * from dual;

insert all
    into edu_level values(0,'primary')   
    into edu_level values (1,'secondary')
    into edu_level values (2,'tertiary')
    into edu_level values (3,'unkown')
select * from dual;
        
insert all
    into loan_status values(0,'no')   
    into loan_status values (1,'yes')
select * from dual;

insert all
    into housing_status values(0,'no')   
    into housing_status values (1,'yes')
select * from dual;

commit;
--DB creation done, below is a test showing customer_id with business as job type
select customer_id,job_type
from customers
    join jobs
        using (job_code)
    join job_type
        using (job_type_code)
where job_type = 'business';
----------------------------------------------------------------------------------------------------------
ANALYZE TABLE customers COMPUTE STATISTICS; -- go customers table and check the partition attribute
--------------------INDEX
-- This is the range query comparision between indexing and reguler
-- before creating index
select * 
from customers 
where month like '%ju%';
-- create index and re-run above query to check consistent gets and cost
CREATE INDEX month_btree
ON customers(month);

-- point query 
select max(balance) from customers;
select * from customers
where balance = '102127';
-- create index, and re-run the query to compare the gets and costs
Create index balance_btree
on customers (balance);

--scan query. the customers_to_compare has no index or partition
select month, count(*) as customers_contacted
from customers_to_compare
group by month;

select month, count(*) as customers_contacted
from customers
group by month;
--- above is the three types of query comparisions with index
-------------------------------------------------------------------------------------------------------------------
password; --- to change password
-- exeution without parallel
select customer_id, balance,results,job_type
from customers
    join jobs
        using (job_code)
    join job_type
        using (job_type_code)
where (age between 30 and 50)
    and balance >= 10000;
-- parallel with degree of 4 on and re-run the query, speed did increase
alter table customers parallel 4;
select customer_id, balance,results,job_type
from customers
    join jobs
        using (job_code)
    join job_type
        using (job_type_code)
where (age between 30 and 50)
    and balance >= 10000;

------------------PARTITION
-- Point query, customers_to_compare has no index or partition, table compare has partition only
select age, results
from customers_to_compare
where
    age = 33
    and 
    balance >= 18800;
-- Point query, table compare has partition only
select age, results
from compare
where
    age = 33
    and 
    balance >= 18800;

-- scan query without partition
select count(*)
from customers_to_compare
where age in (33,34,35);    
-- scan query with partition
select count(*)
from compare
where age in (33,34,35);    


-- Range query, customers_to_compare has no index or partition, table compare has partition only
select age, results
from customers_to_compare
where
    age = 33
    and 
    results = 'yes';
-- Range query. table compare has same data as customers_to_compare, with partition of ages   
select age, results
from compare
where
    age = 33
    and 
    results = 'yes';

--------------------data visualization
-- Data visualization with histgram to group ages with count
select age_group, count(*) as count
  from (
    SELECT
         CASE
            WHEN AGE < 30 THEN '0-30'   WHEN AGE < 35 THEN '30-35'
            WHEN AGE < 40 THEN '35-40'  WHEN AGE < 45 THEN '40-45'
            WHEN AGE < 50 THEN '45-50'  ELSE '50 or older'
        END AS age_group
    FROM CUSTOMERS
    )
group by age_group
order by age_group desc;

-- outlier lookup in data mining
select round(stddev(balance),2),round(avg(balance),2) as mean
from customers;
select 1352.12+3022.95*3 as uo, 1352.12-3022.95*3 as lo
from dual;
select customer_id, balance, results
from customers
where (balance >=10420.97)
    or (balance <=-7716.73)
order by balance asc;
    