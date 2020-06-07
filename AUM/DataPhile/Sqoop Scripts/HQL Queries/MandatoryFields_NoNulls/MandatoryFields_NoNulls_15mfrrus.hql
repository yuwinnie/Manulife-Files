--MandatoryFields_No null values
select  rr,last_dt,last_time
from    inv_aum_typed_qa.dpl_mfrrus 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     (rr is null
or      last_dt is null
or      last_time is null)
limit 1;
