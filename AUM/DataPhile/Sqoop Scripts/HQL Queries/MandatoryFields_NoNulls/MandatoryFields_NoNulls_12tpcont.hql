--MandatoryFields_No null values
select  last_dt,last_time,entr_dt,entr_time,ref_num
from    inv_aum_typed_qa.dpl_tpcont 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     (last_dt is null
or      last_time is null
or      entr_dt is null
or      entr_time is null
or      ref_num is null)
limit 1;
