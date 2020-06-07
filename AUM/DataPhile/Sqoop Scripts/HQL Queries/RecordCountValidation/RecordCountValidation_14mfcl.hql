--Record Count Validation
select  '14.dpl_mfcl' as tableName,   
        ti.ct as typed_msi_ct,      ri.ct as test_msi_ct,       case when ti.ct <> ri.ct    then 'FAIL' else 'PASS' end as msi_result,
        tsi.ct as typed_msisi_ct,   rsi.ct as test_msisi_ct,    case when tsi.ct <> rsi.ct  then 'FAIL' else 'PASS' end as msisi_result,
        tii.ct as typed_msii_ct,    rii.ct as test_msii_ct,     case when tii.ct <> rii.ct  then 'FAIL' else 'PASS' end as msii_result,
        t.ct as typed_ct,           r.ct as test_ct,            case when t.ct <> r.ct      then 'FAIL' else 'PASS' end as result
from    (select count(*) as ct from inv_aum_typed_qa.dpl_mfcl       where broker_code = 'MSI'   and process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)) as ti,
        (select count(*) as ct from inv_aum_typed_qa.dpl_mfcl       where broker_code = 'MSISI' and process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)) as tsi,
        (select count(*) as ct from inv_aum_typed_qa.dpl_mfcl       where broker_code = 'MSII' and process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)) as tii,
        (select count(*) as ct from inv_aum_typed_qa.dpl_mfcl       where process_date = cast(regexp_replace(${t_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)) as t,
        (select count(*) as ct from inv_aum_raw_qa.test_dpl_mfcl    where broker_code = 'MSI'   and process_date = cast(regexp_replace(${s_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)) as ri,
        (select count(*) as ct from inv_aum_raw_qa.test_dpl_mfcl    where broker_code = 'MSISI' and process_date = cast(regexp_replace(${s_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)) as rsi,
        (select count(*) as ct from inv_aum_raw_qa.test_dpl_mfcl    where broker_code = 'MSII' and process_date = cast(regexp_replace(${s_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)) as rii,
        (select count(*) as ct from inv_aum_raw_qa.test_dpl_mfcl    where process_date = cast(regexp_replace(${s_process_date},'(\\d{4})(\\d{2})(\\d{2})','$1-$2-$3') as date)) as r;