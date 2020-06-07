--Natural Key _No Duplicate

select  acct, stl_dt, cusip, stmt_sort, time_seq, seq, broker_code, count(*)  ct
from    inv_aum_typed_qa.dpl_actran 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date) 
group by acct, stl_dt, cusip, stmt_sort, time_seq, seq, broker_code 
having count(*) >1
limit 1;