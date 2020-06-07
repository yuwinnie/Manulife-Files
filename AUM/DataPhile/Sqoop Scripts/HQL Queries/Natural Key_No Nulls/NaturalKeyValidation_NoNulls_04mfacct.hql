--Natural Key _No null values
select  code, fund_type, suspense
from    inv_aum_typed_qa.dpl_mfacct 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     code        is null
and     fund_type   is null
and     suspense    is null
limit 10;
