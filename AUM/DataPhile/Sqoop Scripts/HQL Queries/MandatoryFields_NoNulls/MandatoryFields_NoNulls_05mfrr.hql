--MandatoryFields_No null values
select  rr,branch,trades_ok,short_name,language,trd_name,website,authorized_list_code,under_supervision,rr_type
from    inv_aum_typed_qa.dpl_mfrr 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     (rr is null
or      branch is null
or      trades_ok is null
or      short_name is null
or      language is null
or      trd_name is null
or      website is null
or      authorized_list_code is null
or      under_supervision is null
or      rr_type is null)
limit 1;
