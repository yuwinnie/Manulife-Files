--Natural Key _No null values
select  usr, rr
from    inv_aum_typed_qa.dpl_mfrrus 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     (usr is null
or      rr  is null)
limit 10;
