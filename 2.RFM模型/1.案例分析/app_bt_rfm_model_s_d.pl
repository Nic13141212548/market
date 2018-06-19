#!/usr/bin/perl
########################################################################################################################
#  Creater        :yelei12
#  Creation Time  :20180607
#  Description    :白条用户价值分析
#  Modify By      :
#  Modify Time    :
#  Modify Content :
#  Script Version :1.0.3
########################################################################################################################
use strict;
use jrjtcommon;
use un_pswd;
use Common::Hive;
use zjcommon;

##############################################
#默认STINGER运行，失败后HIVE运行，可更改Runner和Retry_Runner
#修改最终生成表库名和表名
##############################################

my $Runner = "STINGER";
my $Retry_Runner = "HIVE";
my $DB = "";
my $TABLE = "";
##############################################

if ( $#ARGV < 0 ) { exit(1); }
my $CONTROL_FILE = $ARGV[0];
my $JOB = substr(${CONTROL_FILE}, 4, length(${CONTROL_FILE})-17);

#当日 yyyy-mm-dd
my $TX_DATE = substr(${CONTROL_FILE},length(${CONTROL_FILE})-12, 4).'-'.substr(${CONTROL_FILE},length(${CONTROL_FILE})-8, 2).'-'.substr(${CONTROL_FILE},length(${CONTROL_FILE})-6, 2);

my $TXDATE = substr($TX_DATE, 0, 4).substr($TX_DATE, 5, 2).substr($TX_DATE, 8, 2);                        #当日 yyyymmdd
my $TX_MONTH = substr($TX_DATE, 0, 4).'-'.substr($TX_DATE, 5, 2);                                          #当日所在月 yyyy-mm
my $TXMONTH = substr($TX_DATE, 0, 4).substr($TX_DATE, 5, 2);                                               #当日所在月 yyyymm
my $TX_PREV_DATE = getPreviousDate($TX_DATE);                                                               #前一天 yyyy-mm-dd
my $TX_NEXT_DATE = getNextDate($TX_DATE);                                                                   #下一天 yyyy-mm-dd
my $TXPDATE = substr(${TX_PREV_DATE},0,4).substr(${TX_PREV_DATE},5,2).substr(${TX_PREV_DATE},8,2);        #前一天 yyyymmdd
my $TXNDATE = substr(${TX_NEXT_DATE},0,4).substr(${TX_NEXT_DATE},5,2).substr(${TX_NEXT_DATE},8,2);        #下一天 yyyymmdd
my $CURRENT_TIME = getNowTime();
my $TX_YEAR = substr($TX_DATE, 0, 4);#当年 yyyy

########################################################################################################################
# Write SQL For Your APP
sub getsql
{
    my @SQL_BUFF=();
    #########################################################################################
    ####################################以下为SQL编辑区######################################
    #########################################################################################
$SQL_BUFF[0]=qq(
set mapreduce.job.name=a_bt_rfm_model_s_d_00;
use app;
create table if not exists app.a_bt_rfm_model_s_d
(
 etl_dt                  string                  
,user_pin                string                  
,last_loan_time          string                  
,loan_times              bigint                  
,amount                  decimal(18,2)                  
,loan_term_amount        decimal(18,2)                  
,loan_term_rate          decimal(18,4)              
,min_loan_time           string
)comment 'RFM模型底表'
PARTITIONED BY (dt string comment'分区时间')
ROW FORMAT SERDE'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS ORC
;
);

$SQL_BUFF[1]=qq(
set mapreduce.job.name=dwb_bt_rfm_model_s_d_01;
use app;
insert overwrite table app.a_bt_rfm_model_s_d partition (dt = '$TX_DATE')
select
 from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'),
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
sum(case when loan_term > 0 then loan_prin-refund_prin else 0 end) loan_term_amount,
nvl(sum(recvbl_stag_fee)/sum(case when loan_term > 0 then loan_prin-refund_prin else 0 end),0) loan_term_rate
from dwb.dwb_bt_xbt_ordr_det_s_d
where dt = '$TX_DATE'
and substr(loan_time,1,10)>=date_sub('$TX_DATE' , 90)
and substr(loan_time,1,10)<='$TX_DATE'
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
where dt = '$TX_DATE' 
and is_first <> '0'
and biz_id <> '32' 
) b 
on a.user_pin = b.user_pin
;
);


    #############################################################################################
    ########################################以上为SQL编辑区######################################
    #############################################################################################

    return @SQL_BUFF;
}

########################################################################################################################

sub main
{
    my $ret;

    my @sql_buff = getsql();

    for (my $i = 0; $i <= $#sql_buff; $i++) {
        $ret = Common::Hive::run_hive_sql($sql_buff[$i], ${Runner}, ${Retry_Runner});

        if ($ret != 0) {
            print getCurrentDateTime("SQL_BUFF[$i] Execute Failed");
            return $ret;
        }
        else {
            print getCurrentDateTime("SQL_BUFF[$i] Execute Success");
        }
    }

    return $ret;
}

########################################################################################################################
# program section
# To see if there is one parameter,
print getCurrentDateTime(" Startup Success ..");
print "JOB          : $JOB\n";
print "TX_DATE      : $TX_DATE\n";
print "TXDATE       : $TXDATE\n";
print "Target TABLE : $TABLE\n";

my $rc = main();
if ( $rc != 0 ) {
    print getCurrentDateTime("Task Execution Failed"),"\n";
} else{
    print getCurrentDateTime("Task Execution Success"),"\n";
}
exit($rc);

