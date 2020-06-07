--Natural Key _No null values
select  acct, stl_dt, cusip, stmt_sort, time_seq, seq
from    inv_aum_typed_qa.dpl_actran 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     acct        is null
and     stl_dt      is null
and     cusip       is null
and     stmt_sort   is null
and     time_seq    is null
and     seq         is null
limit 10;
