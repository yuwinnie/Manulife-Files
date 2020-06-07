--MandatoryFields_No null values
select  client,rr,residence,nrt,stat,np41,branch,consoldt,np41b,np41c,bank_crlimit,giin,irs_cost_method,consent_to_share,alternate_id
from    inv_aum_typed_qa.dpl_mfcl 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     (client is null
or      rr is null
or      residence is null
or      nrt is null
or      stat is null
or      np41 is null
or      branch is null
or      consoldt is null
or      np41b is null
or      np41c is null
or      bank_crlimit is null
or      giin is null
or      irs_cost_method is null
or      consent_to_share is null
or      alternate_id is null)
limit 1;
