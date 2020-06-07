--Natural Key _No null values
select  fund_code, fund_type
from    inv_aum_typed_qa.dpl_esmgr 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     fund_code   is null
and     fund_type   is null
limit 10;
