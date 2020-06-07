set hivevar:day0backup = '';
set hivevar:CurrentCurationDateTime = ''; --utc timestamp
set hivevar:LastCurationTimestamp = ''--utc timestamp  --//use '1980-01-01 00:00:00:00.0' for day0

--Target Table Creation and Schema validation
describe    inv_curated_eod_qa.20200121_bkp_bd_investor_account;

--Registry Log Validation
select  *
from    inv_curated_eod_qa.20200121_bkp_curation_registry as a
where   job_name = 'sdh_curation';

--Natural Keys valiation - NoNull //should return no record
select  count(*)
from    inv_curated_eod_qa.bd_investor_account
where   src = 'JHISDH'
and     src_ids['JHISDH'] is null;

--Natural Keys valiation - NoDuplicate //should return no record
select  src_ids['JHISDH'],
        count(*)
from    inv_curated_eod_qa.bd_investor_account
where   src = 'JHISDH'
and     current_version = 'Y'
group by src_ids['JHISDH']
having  count(*) >1;

--Mandatory Fields Validation //should return no record
select  count(*)
from    inv_curated_eod_qa.bd_investor_account
where   src = 'JHISDH'
and     (account_key is null
or      bus_eff_date_from is null
or      bus_eff_date_from is null
or      bus_eff_date_to is null
or      src_ids['JHISDH'] is null
or      account_uid is null
or      edl_curated_ts is null
or      currency_code is null);

--Null and hard coded Columns Validation //should return no record
select  count(*)
from    inv_curated_eod_qa.bd_investor_account
where    src = 'JHISDH'
and     (entity_key is not null
or      investor_id is not null
or      account_type is not null
or      account_status <> 'ACTIVE'
or      tax_type is not null
or      currency_code <> 'USD'
or      primary_person_uid is not null
or      src_entity_id is not null);


--////////////////////////day1
    
    --day1 Record Count Validation
with    CurrentCurationDateTime as (
        select  start_time,
                end_time,
                row_number () over (partition by job_name order by start_time desc) as rank
        from    inv_curated_eod_qa.curation_registry
        where   job_name = 'sdh_curation'
),
source as (
        select    *
        from     (
                select  process_timestamp,
                        nvl(trim(ta_acct_nbr),'<NULL>')                                                 as ta_acct_nbr,
                        row_number() over (partition by ta_acct_nbr order by process_timestamp desc)    as rn
                from     inv_jhi_typed_qa.sdh_asset_position s,
                        CurrentCurationDateTime             c
                where   c.rank = 1--2
                and     s.process_timestamp > c.end_time
                ) a
        where    a.rn = 1
),
source_flag as (
select  s.ta_acct_nbr           as  src_ta_acct_nbr,
        s.process_timestamp     as  src_process_timestamp,
        
        t.bus_eff_date_from     as  tar_bus_eff_date_from,
        t.bus_eff_date_to       as  tar_bus_eff_date_to,
        nvl(trim(t.src_ids['JHISDH']),'<NULL>')     as  tar_src_ids,
        t.account_uid           as  tar_account_uid,
        t.edl_curated_ts        as  tar_edl_curated_ts,
        t.current_version       as  tar_current_version,
        
        case    when t.src_ids['JHISDH'] is null                                    then   'new'
                when s.ta_acct_nbr is not null and t.src_ids['JHISDH'] is not null  then   'existing'
        end                     as flag
        
from    source s full outer join(
        select  *
        from    inv_curated_eod_qa.20200208_bkp_bd_investor_account
        where   src = 'JHISDH'
        and     current_version = 'Y') t
on      nvl(trim(s.ta_acct_nbr),'<NULL>') = nvl(trim(t.src_ids['JHISDH']),'<NULL>')
where   s.ta_acct_nbr is not null
)select    'bd_investor_account_Source' as tableName, count(*) from source_flag where flag in ('new','updated')
union all
select  'bd_investor_account_Target' as tableName, count(*) 
from    inv_curated_eod_qa.bd_investor_account   t,
        CurrentCurationDateTime        c
where   src = 'JHISDH'
and     c.rank = 2
and     t.edl_curated_ts > c.end_time
and     current_version = 'Y';


--Curation query
with    CurrentCurationDateTime as (
        select  start_time,
                end_time,
                row_number () over (partition by job_name order by start_time desc) as rank
        from    inv_curated_eod_qa.curation_registry
        where   job_name = 'sdh_curation'
),
source as (
        select    *
        from     (
                select  process_timestamp,
                        nvl(trim(ta_acct_nbr),'<NULL>')                                                 as ta_acct_nbr,
                        row_number() over (partition by ta_acct_nbr order by process_timestamp desc)    as rn
                from     inv_jhi_typed_qa.sdh_asset_position s,
                        CurrentCurationDateTime             c
                where   c.rank = 1--2
                and     s.process_timestamp > c.end_time
                ) a
        where    a.rn = 1
),
source_flag as (
select  s.ta_acct_nbr           as  src_ta_acct_nbr,
        s.process_timestamp     as  src_process_timestamp,
        
        t.bus_eff_date_from     as  tar_bus_eff_date_from,
        t.bus_eff_date_to       as  tar_bus_eff_date_to,
        nvl(trim(t.src_ids['JHISDH']),'<NULL>')     as  tar_src_ids,
        t.account_uid           as  tar_account_uid,
        t.edl_curated_ts        as  tar_edl_curated_ts,
        t.current_version       as  tar_current_version,
        
        case    when t.src_ids['JHISDH'] is null                                    then   'new'
                when s.ta_acct_nbr is not null and t.src_ids['JHISDH'] is not null  then   'existing'
        end                     as flag
        
from    source s full outer join(
        select  *
        from    inv_curated_eod_qa.bd_investor_account
        where   src = 'JHISDH'
        and     current_version = 'Y') t
on      nvl(trim(s.ta_acct_nbr),'<NULL>') = nvl(trim(t.src_ids['JHISDH']),'<NULL>')
where   s.ta_acct_nbr is not null
),
source_master   as (
-------------new records-------------
select  cast('1980-01-01 00:00:00.0' as timestamp)      as  bus_eff_date_from,
        cast('2099-12-31 00:00:00.0' as timestamp)      as  bus_eff_date_to,
        cast(src_ta_acct_nbr as string)                 as  src_ids,
        concat('JHISDH_', src_ta_acct_nbr)              as  account_uid,
        src_process_timestamp                           as  edl_curated_ts,
        'Y'                                             as  current_version,
        flag
from    source_flag
where   flag = 'new'
union all
-------------existing records-------------
select  tar_bus_eff_date_from                       as  bus_eff_date_from,
        tar_bus_eff_date_to                         as  bus_eff_date_to,
        tar_src_ids                                 as  src_ids,
        tar_account_uid                             as  account_uid,
        tar_edl_curated_ts                          as  edl_curated_ts,
        tar_current_version                         as  current_version,
        flag
from    source_flag
where    flag = 'existing'
)
select  distinct flag,current_version, 
        count(*) over (partition by flag,current_version ) as flag_cnt
from    source_master;
select  count(*)    
from    source_master   source
where   flag = 'existing'
and     exists (
        select  *
        from    inv_curated_eod_qa.bd_investor_account target
        where   target.src = 'JHISDH'   
        and     source.bus_eff_date_from = target.bus_eff_date_from
        and     source.bus_eff_date_to = target.bus_eff_date_to
        and     cast(source.src_ids as string) = cast(target.src_ids['JHISDH'] as string)
        and     source.account_uid = target.account_uid
        and     source.edl_curated_ts = target.edl_curated_ts
        and     nvl(trim(source.current_version), '<NULL>') = nvl(trim(target.current_version), '<NULL>'));
select  count(*)    
from    source_master   source
where   flag = 'new'
and     exists (
        select  *
        from    inv_curated_eod_qa.20200121_bkp_bd_investor_account target,
                CurrentCurationDateTime c
        where   target.src = 'JHISDH' 
        and     c.rank = 1 
        and     source.bus_eff_date_from = target.bus_eff_date_from
        and     source.bus_eff_date_to = target.bus_eff_date_to
        and     cast(source.src_ids as string) = cast(target.src_ids['JHISDH'] as string)
        and     source.account_uid = target.account_uid
        and     target.edl_curated_ts >= c.start_time 
        and     target.edl_curated_ts <= c.end_time
        and     nvl(trim(source.current_version), '<NULL>') = nvl(trim(target.current_version), '<NULL>'));

        

