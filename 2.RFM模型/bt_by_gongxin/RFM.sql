----------------RFM模型底表
select 
 identity(int, 1,1) id ,
 a.user_pin user_pin,
 a.last_loan_time last_loan_time ,
 a.loan_times loan_times,
 a.amount amount,
 a.loan_term_amount loan_term_amount,
 a.loan_term_rate loan_term_rate,
 b.min_loan_time min_loan_time 
 from
(select 
user_pin,
max(loan_time) last_loan_time,
count(distinct ordr_id) loan_times,
sum(loan_prin-refund_prin) amount,
sum(case when loan_term > 1 then loan_prin-refund_prin else 1 end) loan_term_amount,
nvl(sum(recvbl_stag_fee)/sum(case when loan_term > 1 then loan_prin-refund_prin else 0 end),0) loan_term_rate
from dwb.dwb_bt_xbt_ordr_det_s_d
where dt = '2018-06-01'
and substr(loan_time,1,10)>='2018-03-01' 
and substr(loan_time,1,10)<='2018-05-31'
and biz_id <> '32'  
and loan_prin-refund_prin > 0
group by user_pin
) a 
left join 
(
select distinct 
user_pin,
loan_time min_loan_time 
from dwb.dwb_bt_xbt_ordr_det_s_d
where dt = '2018-06-01' 
and is_first <> '0'
and biz_id <> '32' 
) b 
on a.user_pin = b.user_pin

--------------用户平均消费间隔时间分布
select b.num,count(distinct b.user_pin) num1 from 
(select a.user_pin user_pin,avg(datediff(a.lead_loan_time,a.loan_time)) num from 
(select 
user_pin,loan_time
,lead(loan_time,1,'2018-06-01') OVER (PARTITION BY user_pin ORDER BY loan_time ) lead_loan_time
from dwb.dwb_bt_xbt_ordr_det_s_d
where dt = '2018-06-01'
and substr(loan_time,1,10)>='2017-06-01' 
and substr(loan_time,1,10)<='2018-05-31'
and biz_id <> '32'  
and loan_prin-refund_prin > 0
) a 
where a.lead_loan_time <> '2018-06-01'
group by user_pin
) b
group by b.num

-----------------------用户最后两次消费间隔时间分布
select b.num,count(distinct b.user_pin) num1 from 
(
select a.user_pin user_pin,abs(datediff(a.lead_loan_time,a.loan_time)) num from 
(select 
user_pin,loan_time
,lead(loan_time,1,'2018-06-01') OVER (PARTITION BY user_pin ORDER BY loan_time desc) lead_loan_time,
row_number() OVER (PARTITION BY user_pin ORDER BY loan_time desc) rn
from dwb.dwb_bt_xbt_ordr_det_s_d
where dt = '2018-06-01'
and substr(loan_time,1,10)>='2017-06-01' 
and substr(loan_time,1,10)<='2018-05-31'
and biz_id <> '32'  
and loan_prin-refund_prin > 0
)  a 
where a.rn=1 and a.lead_loan_time <> '2018-06-01'
) b
group by b.num