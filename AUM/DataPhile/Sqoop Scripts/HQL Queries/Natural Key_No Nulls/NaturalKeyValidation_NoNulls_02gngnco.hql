--Natural Key _No null values
select  company
from    inv_aum_typed_qa.dpl_gngnco 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     company is null
limit 10;
