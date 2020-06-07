--Natural Key _No null values
select  tran_type, buy_fds, sell_fds, exch_dt, exch_time, max_amt
from    inv_aum_typed_qa.dpl_gnexch 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     tran_type   is null
and     buy_fds     is null
and     sell_fds    is null
and     exch_dt     is null
and     exch_time   is null
and     max_amt     is null
limit 10;