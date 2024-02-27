Create database Project3;
show databases;
use Project3;

# table-1 Users 
create table users (
user_id int,
created_at varchar(100),
company_id int,
language varchar(50),
activated_at varchar(100),
state varchar(50)) ;

Show variables like 'secure_file_priv';

Load data Infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
into Table users
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from users;

alter table users add column temp_created_at datetime;

set SQL_SAFE_UPDATES = 0; 

UPDATE users 
SET temp_created_at = STR_TO_DATE(created_at, '%d-%m-%Y %H:%i');

alter table users drop column created_at;

alter table users change column temp_created_at created_at datetime;

# Table-2 Events
create table events (
 user_id int,
 occurred_at varchar(100),
 event_type varchar(50),
 event_name varchar(100),
 location varchar(50),
 device varchar(50),
 user_type int null
);

Load data Infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
into Table events
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

desc events;

select * from events;

alter table events add column temp_occurred_at datetime;

set SQL_SAFE_UPDATES = 0; 

UPDATE events 
SET temp_occurred_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');

alter table events drop column occurred_at;

alter table events change column temp_occurred_at occurred_at datetime;

# Table-3 Email-events
create table email_events (
 user_id int,
 occurred_at varchar(100),
 action varchar(100),
 user_type int
);

Load data Infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
into Table email_events
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from email_events;

# table-4 job_data       	time_spent	org
create table Job_data (
job_id int,
ds varchar(50),
actor_id int,
events varchar(50),
language varchar(100),
time_spent int,
org varchar(50)
) ;

select * from job_data;

-- Case Study 1: Job Data Analysis
-- (A) Jobs Reviewed Over Time: Calculate the number of jobs reviewed per hour for each day in November 2020.
select * from job_data;
select ds as date, round((count(job_id)/sum(time_spent))*3600) as "Jobs Reviewed per hourper day"
from job_data
where ds between '2020-11-01' and '2020-11-30'
group by ds; 

-- (B) Throughput Analysis: calculate the 7-day rolling average of throughput. Additionally, explain whether you prefer using the daily metric or the 7-day rolling average for throughput, and why.
SELECT ROUND(COUNT(events)/sum(time_spent),2) as "Weekly Throughout"
FROM job_data; 

Select ds as dates, round(count(events)/sum(time_spent),2) as "Daily Throughout"
from job_data
group by ds
order by ds;

-- Explain- Metrics will always go up and down on a weekly and daily basis. you will get numbers faster every day or minute if you want. As a result, rolling metrics are superb at showing if your metrics are trending up or down on a daily level.

-- (C) Language Share Analysis: calculate the percentage share of each language over the last 30 days.
SELECT language AS Languages, ROUND(100 * COUNT(*) / (SELECT COUNT(*) FROM job_data), 2) AS Percentage
FROM job_data
GROUP BY language;

-- (D) Duplicate Rows Detection: Identify duplicate rows in the data.
SELECT actor_id, COUNT(*) as Duplicates
FROM job_data 
GROUP BY actor_id 
HAVING COUNT(*) > 1;

-- Case Study 2: Investigating Metric Spike
-- (A) Weekly User Engagement: calculate the weekly user engagement.
SELECT EXTRACT(WEEK FROM occurred_at) AS "Week Numbers", Count(distinct
user_id) AS "Weekly Active Users"
FROM events 
WHERE event_type = 'engagement'
GROUP BY 1; 

-- (B) User Growth Analysis:  calculate the user growth for the product.
SELECT Months, Users, ROUND(((Users/LAG(Users, 1) OVER (ORDER by Months) -
1)*100), 2) AS "Growth in %"
FROM 
( 
SELECT EXTRACT(MONTH FROM created_at) AS Months, COUNT(activated_at) as Users
FROM users 
WHERE activated_at NOT IN ("")
GROUP BY 1
ORDER BY 1
) sub;

-- (C) Weekly Retention Analysis: calculate the weekly retention of users based on their sign-up cohort.
SELECT sub.first AS "Week Numbers",
       SUM(CASE WHEN week_number = 0 THEN 1 ELSE 0 END) AS "Week 0",
       SUM(CASE WHEN week_number = 1 THEN 1 ELSE 0 END) AS "Week 1",
       SUM(CASE WHEN week_number = 2 THEN 1 ELSE 0 END) AS "Week 2",
       SUM(CASE WHEN week_number = 3 THEN 1 ELSE 0 END) AS "Week 3",
       SUM(CASE WHEN week_number = 4 THEN 1 ELSE 0 END) AS "Week 4",
       SUM(CASE WHEN week_number = 5 THEN 1 ELSE 0 END) AS "Week 5",
       SUM(CASE WHEN week_number = 6 THEN 1 ELSE 0 END) AS "Week 6",
       SUM(CASE WHEN week_number = 7 THEN 1 ELSE 0 END) AS "Week 7",
       SUM(CASE WHEN week_number = 8 THEN 1 ELSE 0 END) AS "Week 8", 
       SUM(CASE WHEN week_number = 9 THEN 1 ELSE 0 END) AS "Week 9",
       SUM(CASE WHEN week_number = 10 THEN 1 ELSE 0 END) AS "Week 10",
       SUM(CASE WHEN week_number = 11 THEN 1 ELSE 0 END) AS "Week 11",
       SUM(CASE WHEN week_number = 12 THEN 1 ELSE 0 END) AS "Week 12",
       SUM(CASE WHEN week_number = 13 THEN 1 ELSE 0 END) AS "Week 13",
       SUM(CASE WHEN week_number = 14 THEN 1 ELSE 0 END) AS "Week 14",
       SUM(CASE WHEN week_number = 15 THEN 1 ELSE 0 END) AS "Week 15",
       SUM(CASE WHEN week_number = 16 THEN 1 ELSE 0 END) AS "Week 16",
       SUM(CASE WHEN week_number = 17 THEN 1 ELSE 0 END) AS "Week 17",
       SUM(CASE WHEN week_number = 18 THEN 1 ELSE 0 END) AS "Week 18"
FROM
( 
    SELECT m.user_id, 
           m.login_week, 
           n.first, 
           m.login_week - n.first AS week_number
    FROM 
    (
        SELECT user_id, EXTRACT(WEEK FROM occurred_at) AS login_week 
        FROM events
        GROUP BY 1, 2
    ) m
    JOIN 
    (
        SELECT user_id, MIN(EXTRACT(WEEK FROM occurred_at)) AS first 
        FROM events 
        GROUP BY 1
    ) n ON m.user_id = n.user_id
) sub 
GROUP BY sub.first 
ORDER BY sub.first;

-- (D) Weekly Engagement Per Device: calculate the weekly engagement per device.
SELECT EXTRACT(WEEK FROM occurred_at) AS "Week Numbers",
    COUNT(DISTINCT CASE WHEN device IN ('dell inspiron notebook') THEN user_id ELSE NULL END) AS "Dell Inspiron Notebook",
    COUNT(DISTINCT CASE WHEN device IN ('iphone5') THEN user_id ELSE NULL END) AS "iPhone 5",
    COUNT(DISTINCT CASE WHEN device IN ('iphone 4s') THEN user_id ELSE NULL END) AS "iPhone 4S",
    COUNT(DISTINCT CASE WHEN device IN ('windows surface') THEN user_id ELSE NULL END) AS "Windows Surface",
    COUNT(DISTINCT CASE WHEN device IN ('macbook air') THEN user_id ELSE NULL END) AS "Macbook Air",
    COUNT(DISTINCT CASE WHEN device IN ('iphone 5s') THEN user_id ELSE NULL END) AS "iPhone 5S",
    COUNT(DISTINCT CASE WHEN device IN ('macbook pro') THEN user_id ELSE NULL END) AS "Macbook Pro",
    COUNT(DISTINCT CASE WHEN device IN ('kindle fire') THEN user_id ELSE NULL END) AS "Kindle Fire",
    COUNT(DISTINCT CASE WHEN device IN ('ipad mini') THEN user_id ELSE NULL END) AS "iPad Mini",
    COUNT(DISTINCT CASE WHEN device IN ('nexus 7') THEN user_id ELSE NULL END) AS "Nexus 7",
    COUNT(DISTINCT CASE WHEN device IN ('nexus 5') THEN user_id ELSE NULL END) AS "Nexus 5",
    COUNT(DISTINCT CASE WHEN device IN ('samsung galaxy s4') THEN user_id ELSE NULL END) AS "Samsung Galaxy S4",
    COUNT(DISTINCT CASE WHEN device IN ('lenovo thinkpad') THEN user_id ELSE NULL END) AS "Lenovo Thinkpad",
    COUNT(DISTINCT CASE WHEN device IN ('samsung galaxy tablet') THEN user_id ELSE NULL END) AS "Samsung Galaxy Tablet",
    COUNT(DISTINCT CASE WHEN device IN ('acer aspire notebook') THEN user_id ELSE NULL END) AS "Acer Aspire Notebook",
    COUNT(DISTINCT CASE WHEN device IN ('asus chromebook') THEN user_id ELSE NULL END) AS "Asus Chromebook",
    COUNT(DISTINCT CASE WHEN device IN ('htc one') THEN user_id ELSE NULL END) AS "HTC One",
    COUNT(DISTINCT CASE WHEN device IN ('nokia lumia 635') THEN user_id ELSE NULL END) AS "Nokia Lumia 635",
    COUNT(DISTINCT CASE WHEN device IN ('samsung galaxy note') THEN user_id ELSE NULL END) AS "Samsung Galaxy Note",
    COUNT(DISTINCT CASE WHEN device IN ('acer aspire desktop') THEN user_id ELSE NULL END) AS "Acer Aspire Desktop",
    COUNT(DISTINCT CASE WHEN device IN ('mac mini') THEN user_id ELSE NULL END) AS "Mac Mini",
    COUNT(DISTINCT CASE WHEN device IN ('hp pavilion desktop') THEN user_id ELSE NULL END) AS "HP Pavilion Desktop",
    COUNT(DISTINCT CASE WHEN device IN ('dell inspiron desktop') THEN user_id ELSE NULL END) AS "Dell Inspiron Desktop",
    COUNT(DISTINCT CASE WHEN device IN ('ipad air') THEN user_id ELSE NULL END) AS "iPad Air",
    COUNT(DISTINCT CASE WHEN device IN ('amazon fire phone') THEN user_id ELSE NULL END) AS "Amazon Fire Phone",
    COUNT(DISTINCT CASE WHEN device IN ('nexus 10') THEN user_id ELSE NULL END) AS "Nexus 10"
FROM events 
WHERE event_type = 'engagement'
GROUP BY 1
ORDER BY 1;

-- (E) Email Engagement Analysis: calculate the email engagement metrics.
SELECT Week,
ROUND((weekly_digest/total*100),2) AS "Weekly Digest Rate",
ROUND((email_opens/total*100),2) AS "Email Open Rate",
ROUND((email_clickthroughs/total*100),2) AS "Email Clickthrough Rate",
ROUND((reengagement_emails/total*100),2) AS "Reengagement Email Rate"
FROM
( 
    SELECT EXTRACT(WEEK FROM occurred_at) AS Week,
    COUNT(CASE WHEN action = 'sent_weekly_digest' THEN user_id ELSE NULL END) AS weekly_digest, 
    COUNT(CASE WHEN action = 'email_open' THEN user_id ELSE NULL END) AS email_opens, 
    COUNT(CASE WHEN action = 'email_clickthrough' THEN user_id ELSE NULL END) AS email_clickthroughs,
    COUNT(CASE WHEN action = 'sent_reengagement_email' THEN user_id ELSE NULL END) AS reengagement_emails,
    COUNT(user_id) AS total
    FROM email_events
    GROUP BY 1
) sub
GROUP BY 1
ORDER BY 1;
