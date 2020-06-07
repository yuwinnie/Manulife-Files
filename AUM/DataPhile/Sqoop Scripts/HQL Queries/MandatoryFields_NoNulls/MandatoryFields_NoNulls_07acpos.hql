--MandatoryFields_No null values
select  acct,cusip,rr,pend_qty,cur_cost,seg_qty,memo_qty,memo_cost,short,last_dt,last_time,sfk_qty,off_qty,cost_stat,cost_stat_ovrd,cost_stat_ovrd_usr,cost_stat_calc,cost_stat_ovrd_memo
from    inv_aum_typed_qa.dpl_acpos 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     (acct is null
or      cusip is null
or      rr is null
or      pend_qty is null
or      cur_cost is null
or      seg_qty is null
or      memo_qty is null
or      memo_cost is null
or      short is null
or      last_dt is null
or      last_time is null
or      sfk_qty is null
or      off_qty is null
or      cost_stat is null
or      cost_stat_ovrd is null
or      cost_stat_ovrd_usr is null
or      cost_stat_calc is null
or      cost_stat_ovrd_memo is null)
limit 1;
