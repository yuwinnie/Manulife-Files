--Record Count Validation
select  '11.dpl_mfsc' as tableName,  
        ti.ct as typed_msi_ct,      ri.ct as test_msi_ct,       case when ti.ct <> ri.ct    then 'FAIL' else 'PASS' end as msi_result,
        0 as typed_msisi_ct,   0 as test_msisi_ct,    '-' as msisi_result,
        case when t.ct <> r.ct then 'FAIL' else 'PASS' end as result
from    (select count(*) as ct from inv_aum_typed_qa.dpl_mfsc       where broker_code = 'SM'   and process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)) as ti,
        (select count(*) as ct from inv_aum_typed_qa.dpl_mfsc       where process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)) as t,
        (select count(*) as ct from inv_aum_raw_qa.test_dpl_mfsc    where broker_code = 'SM'   and process_date = cast(regexp_replace(${s_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)) as ri,
        (select count(*) as ct from inv_aum_raw_qa.test_dpl_mfsc    where process_date = cast(regexp_replace(${s_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)) as r;