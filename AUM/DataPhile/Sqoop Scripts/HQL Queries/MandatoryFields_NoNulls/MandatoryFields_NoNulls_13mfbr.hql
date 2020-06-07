--MandatoryFields_No null values
select  branch,last_dt,last_time,short_name,ibcode,residence
from    inv_aum_typed_qa.dpl_mfbr 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     (branch is null
or      last_dt is null
or      last_time is null
or      short_name is null
or      ibcode is null
or      residence is null)
limit 1;
