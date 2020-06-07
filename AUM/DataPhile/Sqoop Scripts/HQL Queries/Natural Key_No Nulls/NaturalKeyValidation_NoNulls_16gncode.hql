--Natural Key _No null values
select  table_name,code
from    inv_aum_typed_qa.dpl_gncode 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     table_name  is null
and     code        is null
limit 10;
