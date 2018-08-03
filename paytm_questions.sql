/*created by nikunj maheshwari*/

-- step 1,2 is related to the table creation and loading of data from log file into table.RegexSerDe has been used to read the data

step 1:--
create db hdpcps;

CREATE TABLE hdpcps.log_file(
  time string,
  host string,
  real_ip STRING,
  forwarded_ip STRING,
  request_time STRING,
  backend_time STRING,
  response_time STRING,
  status STRING,
  return_status STRING,
  variable string,
  size string,
  request string,
  agent STRING,
  rsa string,
  protocol string)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = "([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ \"]*|\"[^\"]*\") ([^ \"]*|\"[^\"]*\") ([^ ]*) ([^ ]*)",
  "output.format.string" = "%1$s %2$s %3$s %4$s %5$s %6$s %7$s %8$s %9$s %10$s %11$s %12$s %13$s %14$s %15$s")
  STORED AS TEXTFILE;

step 2:--
load data local inpath '/users/nikumahe/2015_07_22_mktplace_shop_web_log_sample.log' into table hdpcps.log_file


--after the thorough analysis of data. i have created one intermediate table that will store the session changes for each ip address and will calculate the time differencr between each record.session change will happen if the difference between the two records is greater than 900seconds i.e.15mins. so this intermediate table contains all the things related to what we need in task.

--sum_flag means how many times the sessions has changed.

-- diff_timeis the difference between the two consecutive records.

step 3:--


create table hdpcps.session_values as 
select
real_ip,url,time,diff_time,
sum(session_change)over(partition by real_ip order by time) as sum_flag
from(
select 
real_ip,time,url,abs(diff_time) as diff_time,
case when abs(diff_time)>900
then 1
else 0
end as session_change
from
(
select 
real_ip,time,url,prev_time,
(DATEDIFF(CAST(from_unixtime(unix_timestamp(time, "yyyy-MM-dd'T'hh:mm:ss.SSS'Z'")) AS TIMESTAMP),CAST(from_unixtime(unix_timestamp(prev_time, "yyyy-MM-dd'T'hh:mm:ss.SSS'Z'")) AS TIMESTAMP))*24*60*60)+
(UNIX_TIMESTAMP(SUBSTR(CAST(from_unixtime(unix_timestamp(time, "yyyy-MM-dd'T'hh:mm:ss.SSSSSS'Z'")) AS TIMESTAMP),12),'hh:mm:ss')-
UNIX_TIMESTAMP(SUBSTR(CAST(from_unixtime(unix_timestamp(prev_time, "yyyy-MM-dd'T'hh:mm:ss.SSSSSS'Z'")) AS TIMESTAMP),12),'hh:mm:ss')) as diff_time
from
(
select real_ip,time,
regexp_extract(request,'([^ ]*) ([^ ]*) ([^\"]*)',2) AS URL,
lag(time) over (partition by real_ip order by time) as prev_time
from hdpcps.log_file
group by real_ip,time,request
order by time
)extract_data
)difference_in_time
)session_changes
group by real_ip,time,url,diff_time,session_change
order by time

-- question 1 was to aggrefate the page hits in each session. soo i have taken the count of all distinct ip address grouped in each session.

step 4: - select real_ip,count(distinct real_ip),count(distinct url),
case when sum_flag>=0
then concat('session number -',sum_flag)
else NULL 
end as session_name
from hdpcps.session_values
group by real_ip,sum_flag

--question 2 was to calculate the average session time. i.e the sum of all the differences between each consecutive records for a particular ip divided by the total number of session changes. if session changed value is 0 then take the sum of all times.

step 5:- 
select real_ip,
case when session_changes>0
then total_time_spent/session_changes
else
total_time_spent
end as avg_session_time
from(
select real_ip,
sum(diff_time) as total_time_spent,count(distinct sum_flag) as session_changes
from  hdpcps.session_values
group by real_ip
)x

--to find the unique url visits per sesson. i have taken the count of distinct url and the distinct url grouped by each session for a ip address.

step 6:- 

select real_ip,url,count(distinct url),
case when sum_flag>=0
then concat('session number -',sum_flag)
else NULL 
end as session_name
from hdpcps.session_values
group by url,sum_flag,real_ip

-- most engaged user. i have little doubt in this question what is expected.. i have taken out the max session time for each ip address.

step 7:
select real_ip,max(session_time)as max_time_in_session
from
(
select real_ip,sum(diff_time) as session_time,sum_flag
from hdpcps.session_values
group by real_ip,sum_flag
)x
group by real_ip

