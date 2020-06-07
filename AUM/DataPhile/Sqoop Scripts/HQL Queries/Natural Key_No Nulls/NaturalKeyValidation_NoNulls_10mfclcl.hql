--Natural Key _No null values
select  class
from    inv_aum_typed_qa.dpl_mfclcl 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     class  is null
limit 10;
