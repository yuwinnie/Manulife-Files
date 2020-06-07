--Natural Key _No null values
select  cusip
from    inv_aum_typed_qa.dpl_mfsc 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     cusip  is null
limit 10;
