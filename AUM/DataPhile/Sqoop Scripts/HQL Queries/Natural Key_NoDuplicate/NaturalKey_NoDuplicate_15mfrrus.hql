--Natural Key _No Duplicate

select  usr, rr, broker_code,count(*) ct
from    inv_aum_typed_qa.dpl_mfrrus 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date) 
group by usr, rr, broker_code 
having count(*) >1
limit 1;