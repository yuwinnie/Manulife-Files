--Natural Key _No Duplicate

select  rr, broker_code,count(*) ct
from    inv_aum_typed_qa.dpl_mfrr 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date) 
group by rr, broker_code 
having count(*) >1
limit 1;