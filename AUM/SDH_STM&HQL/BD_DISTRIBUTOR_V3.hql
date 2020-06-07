--bd_distributor Master records are saved in 20200222_bkp_master_bd_distributor;
--rel_distributor Master records are saved in 20200222_bkp_master_rel_distributor;
--Mar 3, change trim(b.bus_unit_name) = 'US Retail'

--day0 SDH records are saved in below table: edl_curated_ts = '2020-02-24 17:15:48.933'
--inv_curated_eod_qa.20200225_bkp_SDH_day0_rel_distributor
--inv_curated_eod_qa.20200225_bkp_SDH_day0_bd_distributor
--Target table creation
use     inv_curated_eod_qa;
show    tables;

--Target Table Creation and Schema validation
describe    inv_curated_eod_qa.20200225_bkp_SDH_day0_bd_distributor;
show partitions inv_curated_eod_qa.20200225_bkp_SDH_day0_bd_distributor;

--Registry Log Validation
select  *
from    inv_curated_eod_qa.curation_registry
where   job_name = 'sdh_curation';

    --day0

+---------------------------------------+-------------------------------+-----------------------------+-------------------------------+-----------------------------+--+
|       curation_registry.job_id        | curation_registry.sub_job_id  | curation_registry.job_name  | curation_registry.start_time  | curation_registry.end_time  |
+---------------------------------------+-------------------------------+-----------------------------+-------------------------------+-----------------------------+--+
| 0003122-200222232418307-oozie-oozi-W  |                               | sdh_curation                | 2020-02-25 01:03:32.895       | 2020-02-25 01:40:13.172     |
+---------------------------------------+-------------------------------+-----------------------------+-------------------------------+-----------------------------+--+
1 row selected (10.291 seconds)

    
--Natural Keys valiation - NoNull //should return no record
select  count(*)
from    inv_curated_eod_qa.20200225_bkp_SDH_day0_bd_distributor
where   src = 'JHISDH'
and     distributor_uid is null;

    --day0
+------+--+
| _c0  |
+------+--+
| 0    |
+------+--+
1 row selected (6.423 seconds)

--Natural Keys valiation - NoDuplicate //should return no record
select  distributor_uid,
        count(*)
from    inv_curated_eod_qa.20200225_bkp_SDH_day0_bd_distributor
where   src = 'JHISDH'
and     current_version = 'Y'
group by distributor_uid 
having  count(*) >1;

    --day0

+------------------+------+--+
| distributor_uid  | _c1  |
+------------------+------+--+
+------------------+------+--+
No rows selected (18.364 seconds)

--Mandatory Fields Validation //should return no record
select  count(*)
from    inv_curated_eod_qa.20200225_bkp_SDH_day0_bd_distributor
where   src = 'JHISDH' 
and     (distributor_key    is null
or      bus_eff_date_from   is null
or      bus_eff_date_to is null
or      bus_unit_key   is null
or      distributor_type   is null
or      current_status   is null
or      edl_curated_ts   is null
or      current_version is null);

    --day0
+------+--+
| _c0  |
+------+--+
| 0    |
+------+--+
1 row selected (20.422 seconds)


--Null and hard coded Columns Validation //should return no record
select  count(*)
from    inv_curated_eod_qa.20200225_bkp_SDH_day0_bd_distributor
where   src = 'JHISDH'
and     (retirement_plan_key    is not null
or      portfolio_group_key     is not null
or      advisor_key             is not null
or      entity_key              is not null
or      registered_date         is not null
or      terminated_date         is not null
or      est_ccy_code            is not null
or      est_y1_contrib_amt      is not null
or      est_transfer_assets     is not null
or      ml_rel_mgr_name         is not null
or      temp_ta_code            is not null
or      can_trade_deriv_ind     is not null
or      inspect_funds_ind       is not null
or      inspect_investors_ind   is not null
or      channel_level_1         is not null
or      channel_level_2         is not null
or      channel_level_3         is not null
or      elimination_ind         is not null
or      ia_code                 is not null
or      ia_type                 is not null
or      non_committed_capital   is not null
or      committed_capital       is not null
or      shariah_compliant_ind   is not null
or      affiliate_ind           is not null
or      upper(distributor_type) <> 'ENTITY'
or      upper(creation_status)  <> 'OPEN'
or      upper(current_status)   <> 'ACTIVE');

    --day0

+------+--+
| _c0  |
+------+--+
| 0    |
+------+--+
1 row selected (10.4 seconds)

--Curation query
--////////////////////////day0

    --Day0 Record count
select 'bd_distributor_Target' as tableName, count(*) from inv_curated_eod_qa.20200225_bkp_SDH_day0_bd_distributor where src = 'JHISDH'
union all
select  'bd_distributor_Source' as tableName, count(*)
from    (
            
            select  s.firm_name,
                    cast(s.firm_party_id as string)    as firm_party_id,
                    s.process_timestamp,
                    row_number() over(partition by s.firm_party_id order by s.process_timestamp desc) as rn
            from    inv_jhi_typed_qa.sdh_firm  s
        ) a  
where   a.rn = 1; 
+-------------------------+----------+--+
|      _u1.tablename      | _u1._c1  |
+-------------------------+----------+--+
| bd_distributor_Target   | 1629     |
| bd_distributor_Source   | 1629     |
+-------------------------+----------+--+
2 rows selected (20.789 seconds)

    --Day0 Data validation
with source as (
select  *
from    ( 
            select  nvl(trim(s.firm_name),'<NULL>')                 as firm_name,
                    nvl(cast(s.firm_party_id as string) ,'<NULL>')  as firm_party_id,
                    b.bus_unit_key,
                    s.process_timestamp,
                    row_number() over(partition by s.firm_party_id order by s.process_timestamp desc) as rn
            from    inv_jhi_typed_qa.sdh_firm      s,
                    inv_curated_eod_qa.bd_business_unit    b
            where   trim(b.bus_unit_name) = 'US Retail'
            and     current_version = 'Y'
        ) a 
where   a.rn = 1
)
select  count(*)
from    source
left outer join   
    (   select  d.src,
                cast(d.bus_eff_date_from as timestamp)  as d_bus_eff_date_from,
                cast(d.bus_eff_date_to as timestamp)    as d_bus_eff_date_to,
                nvl(trim(d.bus_unit_key),'<NULL>')      as d_bus_unit_key,
                d.distributor_uid                       as d_distributor_uid,
                nvl(trim(d.distributor_name),'<NULL>')  as d_distributor_name,
                nvl(trim(d.distributor_desc),'<NULL>')  as d_distributor_desc,
                d.edl_curated_ts                        as d_edl_curated_ts,
                d.current_version,
                nvl(r.src_id,'<NULL>')                  as r_src_id,
                r.distributor_uid                       as r_distributor_uid
        from    inv_curated_eod_qa.20200225_bkp_SDH_day0_bd_distributor   d inner join inv_curated_eod_qa.rel_distributor r
        on      d.src = r.src
        and     d.distributor_uid = r.distributor_uid
        and     d.current_version = r.current_version
    ) target
on      source.firm_party_id = target.r_src_id
and     source.bus_unit_key = target.d_bus_unit_key
and     source.firm_name = target.d_distributor_name
and     source.firm_name = target.d_distributor_desc
and     target.d_bus_eff_date_from = cast('1980-01-01 00:00:00.0' as timestamp)
and     target.d_bus_eff_date_to = cast('2099-12-31 00:00:00.0' as timestamp)
and     target.current_version = 'Y'
and     target.d_edl_curated_ts = '2020-02-24 17:17:04.374'
where   target.src = 'JHISDH'
and     target.r_distributor_uid is null; --should return no records
+------+--+
| _c0  |
+------+--+
| 0    |
+------+--+
1 row selected (95.692 seconds)


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
select  *
from    (
            select  nvl(trim(s.firm_name),'<NULL>')                 as firm_name,
                    nvl(cast(s.firm_party_id as string) ,'<NULL>')  as firm_party_id,
                    b.bus_unit_key,
                    s.process_timestamp,
                    row_number() over(partition by s.firm_party_id order by s.process_timestamp desc) as rn
            from    inv_jhi_typed_qa.sdh_firm               s,
                    inv_curated_eod_qa.bd_business_unit     b,
                    CurrentCurationDateTime                 c
            where   trim(b.bus_unit_name) = 'US Retail'--'john hancock investment'
            and     b.current_version = 'Y'
            and     c.rank = 2
            and     s.process_timestamp > c.end_time
        ) a     
where   a.rn = 1
),
source_flag    as (
select  s.firm_party_id         as src_firm_party_id,
        s.bus_unit_key          as src_bus_unit_key,
        s.firm_name             as src_firm_name,
        s.process_timestamp     as src_process_timestamp,

        t.d_bus_eff_date_from   as tar_d_bus_eff_date_from,
        t.d_bus_eff_date_to     as tar_d_bus_eff_date_to,
        t.d_bus_unit_key        as tar_d_bus_unit_key,
        t.d_distributor_uid     as tar_d_distributor_uid,
        t.d_distributor_name    as tar_d_distributor_name,
        t.d_distributor_desc    as tar_d_distributor_desc,
        t.d_edl_curated_ts      as tar_d_edl_curated_ts,
        t.d_current_version     as tar_d_current_version,
        
        t.r_distributor_uid     as tar_r_distributor_uid,
        t.r_src_id              as tar_r_src_id,
        
        case    when t.r_src_id is null                                     then 'new'
                when s.firm_party_id is not null and t.r_src_id is not null 
                     and  s.firm_name <> t.d_distributor_name               then 'updated'
                else                                                             'existing'
        end                     as flag
from    source s full outer join 
    (   select  cast(d.bus_eff_date_from as timestamp)  as d_bus_eff_date_from,
                cast(d.bus_eff_date_to as timestamp)    as d_bus_eff_date_to,
                nvl(trim(d.bus_unit_key),'<NULL>')      as d_bus_unit_key,
                d.distributor_uid                       as d_distributor_uid,
                nvl(trim(d.distributor_name),'<NULL>')  as d_distributor_name,
                nvl(trim(d.distributor_desc),'<NULL>')  as d_distributor_desc,
                cast(d.edl_curated_ts as timestamp)     as d_edl_curated_ts,
                d.current_version                       as d_current_version,
                
                r.distributor_uid                       as r_distributor_uid,
                nvl(r.src_id,'<NULL>')                  as r_src_id
        from    inv_curated_eod_qa.20200225_bkp_SDH_bd_distributor d inner join inv_curated_eod_qa.rel_distributor r   --day0 backup table
        on      d.distributor_uid = r.distributor_uid
        and     d.current_version = r.current_version) t
on      s.firm_party_id = t.r_src_id
where   s.firm_party_id is not null
)
select  'bd_distributor_Source' as tableName, count(*)  from source_flag where flag in ('new','updated')
union all
select  'bd_distributor_Target' as tableName, count(*) 
from    inv_curated_eod_qa.20200227_bkp_SDH_bd_distributor   t,
        CurrentCurationDateTime             c
where   src = 'JHISDH'
and     c.rank = 2
and     t.edl_curated_ts > c.end_time
and     current_version = 'Y';

+------------------------+----------+--+
|     _u1.tablename      | _u1._c1  |
+------------------------+----------+--+
| bd_distributor_Source  | 4        |
| bd_distributor_Target  | 4        |
+------------------------+----------+--+
2 rows selected (14.043 seconds)

    --day1 Data Validation
with    CurrentCurationDateTime as (
        select  start_time,
                end_time,
                row_number () over (partition by job_name order by start_time desc) as rank
        from    inv_curated_eod_qa.curation_registry
        where   job_name = 'sdh_curation'
),
source as (
select  *
from    (
            select  nvl(trim(s.firm_name),'<NULL>')                 as firm_name,
                    nvl(cast(s.firm_party_id as string) ,'<NULL>')  as firm_party_id,
                    b.bus_unit_key,
                    s.process_timestamp,
                    row_number() over(partition by s.firm_party_id order by s.process_timestamp desc) as rn
            from    inv_jhi_typed_qa.sdh_firm               s,
                    inv_curated_eod_qa.bd_business_unit     b,
                    CurrentCurationDateTime                 c
            where   trim(b.bus_unit_name) = 'US Retail'--'john hancock investment'
            and     b.current_version = 'Y'
            and     c.rank = 2
            and     s.process_timestamp > c.end_time
        ) a     
where   a.rn = 1
),
source_flag    as (
select  s.firm_party_id         as src_firm_party_id,
        s.bus_unit_key          as src_bus_unit_key,
        s.firm_name             as src_firm_name,
        s.process_timestamp     as src_process_timestamp,

        t.d_bus_eff_date_from   as tar_d_bus_eff_date_from,
        t.d_bus_eff_date_to     as tar_d_bus_eff_date_to,
        t.d_bus_unit_key        as tar_d_bus_unit_key,
        t.d_distributor_uid     as tar_d_distributor_uid,
        t.d_distributor_name    as tar_d_distributor_name,
        t.d_distributor_desc    as tar_d_distributor_desc,
        t.d_edl_curated_ts      as tar_d_edl_curated_ts,
        t.d_current_version     as tar_d_current_version,
        
        t.r_distributor_uid     as tar_r_distributor_uid,
        t.r_src_id              as tar_r_src_id,
        
        case    when t.r_src_id is null                                     then 'new'
                when s.firm_party_id is not null and t.r_src_id is not null 
                     and  s.firm_name <> t.d_distributor_name               then 'updated'
                else                                                             'existing'
        end                     as flag
from    source s full outer join 
    (   select  cast(d.bus_eff_date_from as timestamp)  as d_bus_eff_date_from,
                cast(d.bus_eff_date_to as timestamp)    as d_bus_eff_date_to,
                nvl(trim(d.bus_unit_key),'<NULL>')      as d_bus_unit_key,
                d.distributor_uid                       as d_distributor_uid,
                nvl(trim(d.distributor_name),'<NULL>')  as d_distributor_name,
                nvl(trim(d.distributor_desc),'<NULL>')  as d_distributor_desc,
                cast(d.edl_curated_ts as timestamp)     as d_edl_curated_ts,
                d.current_version                       as d_current_version,
                
                r.distributor_uid                       as r_distributor_uid,
                nvl(r.src_id,'<NULL>')                  as r_src_id
        from    inv_curated_eod_qa.20200225_bkp_SDH_bd_distributor d inner join inv_curated_eod_qa.rel_distributor r   --day0 backup table
        on      d.distributor_uid = r.distributor_uid
        and     d.current_version = r.current_version) t
on      s.firm_party_id = t.r_src_id
where   s.firm_party_id is not null
),
source_master   as  (
-------------new records-------------
select  src_firm_party_id                           as r_src_id,
        cast('1980-01-01 00:00:00' as timestamp)    as bus_eff_date_from,
        cast('2099-12-31 00:00:00' as timestamp)    as bus_eff_date_to,
        src_bus_unit_key                            as d_bus_unit_key,
        r.  distributor_uid                         as d_distributor_uid,
        trim(src_firm_name)                         as d_distributor_name,
        trim(src_firm_name)                         as d_distributor_desc,
        'Y'                                         as current_version,
        cast('2099-12-31 00:00:00' as timestamp)    as edl_curated_ts,
        flag  
from    source_flag s
        inner join (
            select  distributor_uid,
                    src_id,
                    src
            from    inv_curated_eod_qa.rel_distributor  r, 
                    CurrentCurationDateTime             c
            where   c.rank = 1
            and     r.edl_curated_ts >= c.start_time
            and     r.edl_curated_ts <= c.end_time
            ) r
        on  s.src_firm_party_id = r.src_id
        and r.src = 'JHISDH'
where   flag='new'

union all
-------------updated records / new inserted-------------
select  src_firm_party_id                           as r_src_id,
        cast(concat(to_date(src_process_timestamp),' 00:00:00') as timestamp)    as bus_eff_date_from,
        cast('2099-12-31 00:00:00' as timestamp)    as bus_eff_date_to,
        src_bus_unit_key                            as d_bus_unit_key,
        r.distributor_uid                         as d_distributor_uid,
        trim(src_firm_name)                         as d_distributor_name,
        trim(src_firm_name)                         as d_distributor_desc,
        'Y'                                         as current_version,
        cast('2099-12-31 00:00:00' as timestamp)    as edl_curated_ts,
        flag  
from    source_flag  s
        inner join (
            select  distributor_uid,
                    src_id,
                    src
            from    inv_curated_eod_qa.rel_distributor  r, 
                    CurrentCurationDateTime             c
            where   c.rank = 1
            and     r.edl_curated_ts < c.start_time
            ) r
        on  s.src_firm_party_id = r.src_id
        and r.src = 'JHISDH'
where   flag='updated'

union all
-------------updated records / expired records-------------
select  tar_r_src_id                                as r_src_id,
        tar_d_bus_eff_date_from                     as bus_eff_date_from,
        cast(concat(to_date(src_process_timestamp),' 00:00:00') as timestamp) as bus_eff_date_to,
        tar_d_bus_unit_key                          as d_bus_unit_key,
        tar_d_distributor_uid                       as d_distributor_uid,
        tar_d_distributor_name                      as d_distributor_name,
        tar_d_distributor_desc                      as d_distributor_desc,
        'N'                                         as current_version,
        tar_d_edl_curated_ts                        as edl_curated_ts,
        flag  
from    source_flag 
where   flag='updated'

union all
-------------existing records-------------
select  tar_r_src_id                                as r_src_id,
        tar_d_bus_eff_date_from                     as bus_eff_date_from,
        tar_d_bus_eff_date_to                       as bus_eff_date_to,
        tar_d_bus_unit_key                          as d_bus_unit_key,
        tar_d_distributor_uid                       as d_distributor_uid,
        tar_d_distributor_name                      as d_distributor_name,
        tar_d_distributor_desc                      as d_distributor_desc,
        tar_d_current_version                       as current_version,
        tar_d_edl_curated_ts                        as edl_curated_ts,
        flag  
from    source_flag 
where   flag='existing'
) select  distinct flag,current_version, 
        count(*) over (partition by flag,current_version ) as flag_cnt
from    source_master;
+-----------+------------------+-----------+--+
|   flag    | current_version  | flag_cnt  |
+-----------+------------------+-----------+--+
| existing  | Y                | 2         |
| new       | Y                | 2         |
| updated   | N                | 2         |
| updated   | Y                | 2         |
+-----------+------------------+-----------+--+
4 rows selected (23.706 seconds)

    --new and updated new validation (should return 4)
with    CurrentCurationDateTime as (
        select  start_time,
                end_time,
                row_number () over (partition by job_name order by start_time desc) as rank
        from    inv_curated_eod_qa.curation_registry
        where   job_name = 'sdh_curation'
),
source as (
select  *
from    (
            select  nvl(trim(s.firm_name),'<NULL>')                 as firm_name,
                    nvl(cast(s.firm_party_id as string) ,'<NULL>')  as firm_party_id,
                    b.bus_unit_key,
                    s.process_timestamp,
                    row_number() over(partition by s.firm_party_id order by s.process_timestamp desc) as rn
            from    inv_jhi_typed_qa.sdh_firm               s,
                    inv_curated_eod_qa.bd_business_unit     b,
                    CurrentCurationDateTime                 c
            where   trim(b.bus_unit_name) = 'US Retail'--'john hancock investment'
            and     b.current_version = 'Y'
            and     c.rank = 2
            and     s.process_timestamp > c.end_time
        ) a     
where   a.rn = 1
),
source_flag    as (
select  s.firm_party_id         as src_firm_party_id,
        s.bus_unit_key          as src_bus_unit_key,
        s.firm_name             as src_firm_name,
        s.process_timestamp     as src_process_timestamp,

        t.d_bus_eff_date_from   as tar_d_bus_eff_date_from,
        t.d_bus_eff_date_to     as tar_d_bus_eff_date_to,
        t.d_bus_unit_key        as tar_d_bus_unit_key,
        t.d_distributor_uid     as tar_d_distributor_uid,
        t.d_distributor_name    as tar_d_distributor_name,
        t.d_distributor_desc    as tar_d_distributor_desc,
        t.d_edl_curated_ts      as tar_d_edl_curated_ts,
        t.d_current_version     as tar_d_current_version,
        
        t.r_distributor_uid     as tar_r_distributor_uid,
        t.r_src_id              as tar_r_src_id,
        
        case    when t.r_src_id is null                                     then 'new'
                when s.firm_party_id is not null and t.r_src_id is not null 
                     and  s.firm_name <> t.d_distributor_name               then 'updated'
                else                                                             'existing'
        end                     as flag
from    source s full outer join 
    (   select  cast(d.bus_eff_date_from as timestamp)  as d_bus_eff_date_from,
                cast(d.bus_eff_date_to as timestamp)    as d_bus_eff_date_to,
                nvl(trim(d.bus_unit_key),'<NULL>')      as d_bus_unit_key,
                d.distributor_uid                       as d_distributor_uid,
                nvl(trim(d.distributor_name),'<NULL>')  as d_distributor_name,
                nvl(trim(d.distributor_desc),'<NULL>')  as d_distributor_desc,
                cast(d.edl_curated_ts as timestamp)     as d_edl_curated_ts,
                d.current_version                       as d_current_version,
                
                r.distributor_uid                       as r_distributor_uid,
                nvl(r.src_id,'<NULL>')                  as r_src_id
        from    inv_curated_eod_qa.20200225_bkp_SDH_bd_distributor d inner join inv_curated_eod_qa.rel_distributor r   --day0 backup table
        on      d.distributor_uid = r.distributor_uid
        and     d.current_version = r.current_version) t
on      s.firm_party_id = t.r_src_id
where   s.firm_party_id is not null
),
source_master   as  (
-------------new records-------------
select  src_firm_party_id                           as r_src_id,
        cast('1980-01-01 00:00:00' as timestamp)    as bus_eff_date_from,
        cast('2099-12-31 00:00:00' as timestamp)    as bus_eff_date_to,
        src_bus_unit_key                            as d_bus_unit_key,
        r.  distributor_uid                         as d_distributor_uid,
        trim(src_firm_name)                         as d_distributor_name,
        trim(src_firm_name)                         as d_distributor_desc,
        'Y'                                         as current_version,
        cast('2099-12-31 00:00:00' as timestamp)    as edl_curated_ts,
        flag  
from    source_flag s
        inner join (
            select  distributor_uid,
                    src_id,
                    src
            from    inv_curated_eod_qa.rel_distributor  r, 
                    CurrentCurationDateTime             c
            where   c.rank = 1
            and     r.edl_curated_ts >= c.start_time
            and     r.edl_curated_ts <= c.end_time
            ) r
        on  s.src_firm_party_id = r.src_id
        and r.src = 'JHISDH'
where   flag='new'

union all
-------------updated records / new inserted-------------
select  src_firm_party_id                           as r_src_id,
        cast(concat(to_date(src_process_timestamp),' 00:00:00') as timestamp)    as bus_eff_date_from,
        cast('2099-12-31 00:00:00' as timestamp)    as bus_eff_date_to,
        src_bus_unit_key                            as d_bus_unit_key,
        r.  distributor_uid                         as d_distributor_uid,
        trim(src_firm_name)                         as d_distributor_name,
        trim(src_firm_name)                         as d_distributor_desc,
        'Y'                                         as current_version,
        cast('2099-12-31 00:00:00' as timestamp)    as edl_curated_ts,
        flag  
from    source_flag  s
        inner join (
            select  distributor_uid,
                    src_id,
                    src
            from    inv_curated_eod_qa.rel_distributor  r, 
                    CurrentCurationDateTime             c
            where   c.rank = 1
            and     r.edl_curated_ts < c.start_time
            ) r
        on  s.src_firm_party_id = r.src_id
        and r.src = 'JHISDH'
where   flag='updated'

union all
-------------updated records / expired records-------------
select  tar_r_src_id                                as r_src_id,
        tar_d_bus_eff_date_from                     as bus_eff_date_from,
        cast(concat(to_date(src_process_timestamp),' 00:00:00') as timestamp) as bus_eff_date_to,
        tar_d_bus_unit_key                          as d_bus_unit_key,
        tar_d_distributor_uid                       as d_distributor_uid,
        tar_d_distributor_name                      as d_distributor_name,
        tar_d_distributor_desc                      as d_distributor_desc,
        'N'                                         as current_version,
        tar_d_edl_curated_ts                        as edl_curated_ts,
        flag  
from    source_flag 
where   flag='updated'

union all
-------------existing records-------------
select  tar_r_src_id                                as r_src_id,
        tar_d_bus_eff_date_from                     as bus_eff_date_from,
        tar_d_bus_eff_date_to                       as bus_eff_date_to,
        tar_d_bus_unit_key                          as d_bus_unit_key,
        tar_d_distributor_uid                       as d_distributor_uid,
        tar_d_distributor_name                      as d_distributor_name,
        tar_d_distributor_desc                      as d_distributor_desc,
        tar_d_current_version                       as current_version,
        tar_d_edl_curated_ts                        as edl_curated_ts,
        flag  
from    source_flag 
where   flag='existing'
) 
select  count(*)    
from    source_master   source
where   ((source.flag ='new' and source.current_version = 'Y') or    (source.flag ='updated' and source.current_version = 'Y'))
and     exists (
        select  *
        from    (
                    select  cast(d.bus_eff_date_from as timestamp)  as d_bus_eff_date_from,
                            cast(d.bus_eff_date_to as timestamp)    as d_bus_eff_date_to,
                            nvl(trim(d.bus_unit_key),'<NULL>')      as d_bus_unit_key,
                            d.distributor_uid                       as d_distributor_uid,
                            nvl(trim(d.distributor_name),'<NULL>')  as d_distributor_name,
                            nvl(trim(d.distributor_desc),'<NULL>')  as d_distributor_desc,
                            cast(d.edl_curated_ts as timestamp)     as d_edl_curated_ts,
                            r.distributor_uid                       as r_distributor_uid,
                            nvl(r.src_id,'<NULL>')                  as r_src_id
                    from    inv_curated_eod_qa.20200227_bkp_SDH_bd_distributor   d inner join inv_curated_eod_qa.rel_distributor r
                    on      d.distributor_uid = r.distributor_uid
                    and     d.current_version = r.current_version
                    and     r.src = 'JHISDH' and d.src = 'JHISDH'
                    and     d.current_version = 'Y'
                ) target,
                CurrentCurationDateTime       c
        where   c.rank = 1
        and     target.d_edl_curated_ts >= c.start_time
        and     target.d_edl_curated_ts <= c.end_time       
        and     source.r_src_id = target.r_src_id
        and     source.d_distributor_uid = target.d_distributor_uid   
        and     source.bus_eff_date_from = target.d_bus_eff_date_from
        and     source.bus_eff_date_to = target.d_bus_eff_date_to
        and     source.d_bus_unit_key = target.d_bus_unit_key
        and     source.d_distributor_uid = target.d_distributor_uid
        and     source.d_distributor_name = target.d_distributor_name
        and     source.d_distributor_desc = target.d_distributor_desc );
+------+--+
| _c0  |
+------+--+
| 4    |
+------+--+
1 row selected (19.349 seconds)

 --Day1 Data validation updated/expired and existing(should return 4)
with    CurrentCurationDateTime as (
        select  start_time,
                end_time,
                row_number () over (partition by job_name order by start_time desc) as rank
        from    inv_curated_eod_qa.curation_registry
        where   job_name = 'sdh_curation'
),
source as (
select  *
from    (
            select  nvl(trim(s.firm_name),'<NULL>')                 as firm_name,
                    nvl(cast(s.firm_party_id as string) ,'<NULL>')  as firm_party_id,
                    b.bus_unit_key,
                    s.process_timestamp,
                    row_number() over(partition by s.firm_party_id order by s.process_timestamp desc) as rn
            from    inv_jhi_typed_qa.sdh_firm               s,
                    inv_curated_eod_qa.bd_business_unit     b,
                    CurrentCurationDateTime                 c
            where   trim(b.bus_unit_name) = 'US Retail'--'john hancock investment'
            and     b.current_version = 'Y'
            and     c.rank = 2
            and     s.process_timestamp > c.end_time
        ) a     
where   a.rn = 1
),
source_flag    as (
select  s.firm_party_id         as src_firm_party_id,
        s.bus_unit_key          as src_bus_unit_key,
        s.firm_name             as src_firm_name,
        s.process_timestamp     as src_process_timestamp,

        t.d_bus_eff_date_from   as tar_d_bus_eff_date_from,
        t.d_bus_eff_date_to     as tar_d_bus_eff_date_to,
        t.d_bus_unit_key        as tar_d_bus_unit_key,
        t.d_distributor_uid     as tar_d_distributor_uid,
        t.d_distributor_name    as tar_d_distributor_name,
        t.d_distributor_desc    as tar_d_distributor_desc,
        t.d_edl_curated_ts      as tar_d_edl_curated_ts,
        t.d_current_version     as tar_d_current_version,
        
        t.r_distributor_uid     as tar_r_distributor_uid,
        t.r_src_id              as tar_r_src_id,
        
        case    when t.r_src_id is null                                     then 'new'
                when s.firm_party_id is not null and t.r_src_id is not null 
                     and  s.firm_name <> t.d_distributor_name               then 'updated'
                else                                                             'existing'
        end                     as flag
from    source s full outer join 
    (   select  cast(d.bus_eff_date_from as timestamp)  as d_bus_eff_date_from,
                cast(d.bus_eff_date_to as timestamp)    as d_bus_eff_date_to,
                nvl(trim(d.bus_unit_key),'<NULL>')      as d_bus_unit_key,
                d.distributor_uid                       as d_distributor_uid,
                nvl(trim(d.distributor_name),'<NULL>')  as d_distributor_name,
                nvl(trim(d.distributor_desc),'<NULL>')  as d_distributor_desc,
                cast(d.edl_curated_ts as timestamp)     as d_edl_curated_ts,
                d.current_version                       as d_current_version,
                
                r.distributor_uid                       as r_distributor_uid,
                nvl(r.src_id,'<NULL>')                  as r_src_id
        from    inv_curated_eod_qa.20200225_bkp_SDH_bd_distributor d inner join inv_curated_eod_qa.rel_distributor r   --day0 backup table
        on      d.distributor_uid = r.distributor_uid
        and     d.current_version = r.current_version) t
on      s.firm_party_id = t.r_src_id
where   s.firm_party_id is not null
),
source_master   as  (
-------------new records-------------
select  src_firm_party_id                           as r_src_id,
        cast('1980-01-01 00:00:00' as timestamp)    as bus_eff_date_from,
        cast('2099-12-31 00:00:00' as timestamp)    as bus_eff_date_to,
        src_bus_unit_key                            as d_bus_unit_key,
        r.  distributor_uid                         as d_distributor_uid,
        trim(src_firm_name)                         as d_distributor_name,
        trim(src_firm_name)                         as d_distributor_desc,
        'Y'                                         as current_version,
        cast('2099-12-31 00:00:00' as timestamp)    as edl_curated_ts,
        flag  
from    source_flag s
        inner join (
            select  distributor_uid,
                    src_id,
                    src
            from    inv_curated_eod_qa.rel_distributor  r, 
                    CurrentCurationDateTime             c
            where   c.rank = 1
            and     r.edl_curated_ts >= c.start_time
            and     r.edl_curated_ts <= c.end_time
            ) r
        on  s.src_firm_party_id = r.src_id
        and r.src = 'JHISDH'
where   flag='new'

union all
-------------updated records / new inserted-------------
select  src_firm_party_id                           as r_src_id,
        cast(concat(to_date(src_process_timestamp),' 00:00:00') as timestamp)    as bus_eff_date_from,
        cast('2099-12-31 00:00:00' as timestamp)    as bus_eff_date_to,
        src_bus_unit_key                            as d_bus_unit_key,
        r.  distributor_uid                         as d_distributor_uid,
        trim(src_firm_name)                         as d_distributor_name,
        trim(src_firm_name)                         as d_distributor_desc,
        'Y'                                         as current_version,
        cast('2099-12-31 00:00:00' as timestamp)    as edl_curated_ts,
        flag  
from    source_flag  s
        inner join (
            select  distributor_uid,
                    src_id,
                    src
            from    inv_curated_eod_qa.rel_distributor  r, 
                    CurrentCurationDateTime             c
            where   c.rank = 1
            and     r.edl_curated_ts < c.start_time
            ) r
        on  s.src_firm_party_id = r.src_id
        and r.src = 'JHISDH'
where   flag='updated'

union all
-------------updated records / expired records-------------
select  tar_r_src_id                                as r_src_id,
        tar_d_bus_eff_date_from                     as bus_eff_date_from,
        cast(concat(to_date(src_process_timestamp),' 00:00:00') as timestamp) as bus_eff_date_to,
        tar_d_bus_unit_key                          as d_bus_unit_key,
        tar_d_distributor_uid                       as d_distributor_uid,
        tar_d_distributor_name                      as d_distributor_name,
        tar_d_distributor_desc                      as d_distributor_desc,
        'N'                                         as current_version,
        tar_d_edl_curated_ts                        as edl_curated_ts,
        flag  
from    source_flag 
where   flag='updated'

union all
-------------existing records-------------
select  tar_r_src_id                                as r_src_id,
        tar_d_bus_eff_date_from                     as bus_eff_date_from,
        tar_d_bus_eff_date_to                       as bus_eff_date_to,
        tar_d_bus_unit_key                          as d_bus_unit_key,
        tar_d_distributor_uid                       as d_distributor_uid,
        tar_d_distributor_name                      as d_distributor_name,
        tar_d_distributor_desc                      as d_distributor_desc,
        tar_d_current_version                       as current_version,
        tar_d_edl_curated_ts                        as edl_curated_ts,
        flag  
from    source_flag 
where   flag='existing'
) 
select  count(*)    
from    source_master   source
where   ((source.flag ='existing' and source.current_version = 'Y') or    (source.flag ='updated' and source.current_version = 'N'))
and     exists (
        select  *
        from    (
                    select  cast(d.bus_eff_date_from as timestamp)  as d_bus_eff_date_from,
                            cast(d.bus_eff_date_to as timestamp)    as d_bus_eff_date_to,
                            nvl(trim(d.bus_unit_key),'<NULL>')      as d_bus_unit_key,
                            d.distributor_uid                       as d_distributor_uid,
                            nvl(trim(d.distributor_name),'<NULL>')  as d_distributor_name,
                            nvl(trim(d.distributor_desc),'<NULL>')  as d_distributor_desc,
                            cast(d.edl_curated_ts as timestamp)     as d_edl_curated_ts,
                            r.distributor_uid                       as r_distributor_uid,
                            nvl(r.src_id,'<NULL>')                  as r_src_id
                    from    inv_curated_eod_qa.20200227_bkp_SDH_bd_distributor   d inner join inv_curated_eod_qa.rel_distributor r
                    on      d.distributor_uid = r.distributor_uid
                    --and     d.current_version = r.current_version
                    and     r.src = 'JHISDH' 
                ) target
        where   source.r_src_id = target.r_src_id
        and     source.d_distributor_uid = target.d_distributor_uid   
        and     source.bus_eff_date_from = target.d_bus_eff_date_from
        and     source.bus_eff_date_to = target.d_bus_eff_date_to
        and     source.edl_curated_ts = target.d_edl_curated_ts
        and     source.d_bus_unit_key = target.d_bus_unit_key
        and     source.d_distributor_uid = target.d_distributor_uid
        and     source.d_distributor_name = target.d_distributor_name
        and     source.d_distributor_desc = target.d_distributor_desc );
        
+------+--+
| _c0  |
+------+--+
| 4    |
+------+--+
1 row selected (20.56 seconds)