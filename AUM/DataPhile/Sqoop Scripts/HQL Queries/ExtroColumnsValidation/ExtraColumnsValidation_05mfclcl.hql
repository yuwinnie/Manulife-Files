--Extra Columns Data Valiation - broker_code, process_date, process_timestamp

select  class,broker_code, process_date, process_timestamp
from    inv_aum_typed_qa.dpl_mfclcl where process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date) 
and     ((trim(broker_code) is NULL or  broker_code <> 'SM') or to_date(process_timestamp) <> process_date) limit 1;
