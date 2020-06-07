--MandatoryFields_No null values
select  cusip,short_name,fund_type,prc_factor,reg_code,dual_pay,bbs_elig,vse_elig,dtc_elig,mdw_elig,euro_elig,name_chg,recommend,div_days,priv_days,class,num_decimals,stat,seg_pri,seg_pri_or,trades_ok,cds_elig,cns_elig,nids_elig,dcs_ecs_elig,seg_fund,drs_elig,prc_stat,days_prc_cur
from    inv_aum_typed_qa.dpl_mfsc 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     (cusip is null
or      short_name is null
or      fund_type is null
or      prc_factor is null
or      reg_code is null
or      dual_pay is null
or      bbs_elig is null
or      vse_elig is null
or      dtc_elig is null
or      mdw_elig is null
or      euro_elig is null
or      name_chg is null
or      recommend is null
or      div_days is null
or      priv_days is null
or      class is null
or      num_decimals is null
or      stat is null
or      seg_pri is null
or      seg_pri_or is null
or      trades_ok is null
or      cds_elig is null
or      cns_elig is null
or      nids_elig is null
or      dcs_ecs_elig is null
or      seg_fund is null
or      drs_elig is null
or      prc_stat is null
or      days_prc_cur is null)
limit 1;
