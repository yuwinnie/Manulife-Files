--MandatoryFields_No null values
select  acct,stat,del_code,comm_type,comm_amt,fund_type,cons_lvl,debit_int,credit_int,class,cr_limit,cr_check,opt_appr,net_rr,client,discr,delinq,rr,trades_ok,residence,np41,np41b,np41c,div_code,ibcode,monthly_stmt,crm2_eligible
from    inv_aum_typed_qa.dpl_mfclac 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     (acct is null
or      stat is null
or      del_code is null
or      comm_type is null
or      comm_amt is null
or      fund_type is null
or      cons_lvl is null
or      debit_int is null
or      credit_int is null
or      class is null
or      cr_limit is null
or      cr_check is null
or      opt_appr is null
or      net_rr is null
or      client is null
or      discr is null
or      delinq is null
or      rr is null
or      trades_ok is null
or      residence is null
or      np41 is null
or      np41b is null
or      np41c is null
or      div_code is null
or      ibcode is null
or      monthly_stmt is null
or      crm2_eligible  is null)
limit 1;
