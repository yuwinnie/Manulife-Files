--MandatoryFields_No null values
select  class,last_usr,seg_proc
from    inv_aum_typed_qa.dpl_mfclcl 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     (class is null
or      last_usr is null
or      seg_proc is null)
limit 1;
