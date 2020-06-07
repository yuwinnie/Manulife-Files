--MandatoryFields_No null values
select  last_dt,nfu_group,epa,french_name
from    inv_aum_typed_qa.dpl_esmgr 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     (last_dt is null
or      nfu_group is null
or      epa is null
or      french_name is null)
limit 1;
