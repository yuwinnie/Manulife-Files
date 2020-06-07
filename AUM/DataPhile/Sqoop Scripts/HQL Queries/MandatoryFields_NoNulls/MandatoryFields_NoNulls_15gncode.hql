--MandatoryFields_No null values
select  last_dt,descr,last_time
from    inv_aum_typed_qa.dpl_gncode 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     (last_dt is null
or      descr is null
or      last_time is null)
limit 1;
