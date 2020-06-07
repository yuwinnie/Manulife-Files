--MandatoryFields_No null values
select  fscl_yrnd,rtnd_earn,fst_bal,lst_bal,fst_pl,lst_pl,short_name,consoldt,stmt_on,fund_type,language
from    inv_aum_typed_qa.dpl_gngnco 
where   process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)
and     (fscl_yrnd is null
or      rtnd_earn is null
or      fst_bal is null
or      lst_bal is null
or      fst_pl is null
or      lst_pl is null
or      short_name is null
or      consoldt is null
or      stmt_on is null
or      fund_type is null
or      language is null)
limit 1;
