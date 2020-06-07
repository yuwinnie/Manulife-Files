--Natural Key _No null values
select  proc_dt, trd_num, client_side
from    inv_aum_typed_qa.dpl_tpcont 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     proc_dt     is null
and     trd_num     is null
and     client_side is null
limit 10;
