--inv_curated_eod_qa.20200225_bkp_SDH_day0_rel_distributor


--Target table creation
use     inv_curated_eod_qa;
show    tables;

--Target Table Creation and Schema validation
describe    inv_curated_eod_qa.rel_distributor;
show partitions inv_curated_eod_qa.rel_distributor;

--Natural Keys valiation - NoNull //should return no record
select  count(*)
from    inv_curated_eod_qa.rel_distributor
where   src = 'JHISDH'
and     distributor_uid is null;

    --day0
+------+--+
| _c0  |
+------+--+
| 0    |
+------+--+
1 row selected (10.379 seconds)

    --day1

--Natural Keys valiation - NoDuplicate //should return no record
select  distributor_uid,
        count(*)
from    inv_curated_eod_qa.rel_distributor
where   src = 'JHISDH'
and     current_version = 'Y'
group by distributor_uid 
having  count(*) >1;

    --day0
+------------------+------+--+
| distributor_uid  | _c1  |
+------------------+------+--+
+------------------+------+--+
No rows selected (10.493 seconds)

    --day1


--Mandatory Fields Validation //should return no record
select  count(*)
from    inv_curated_eod_qa.rel_distributor
where   src = 'JHISDH' 
and     (bus_eff_date_from  is null
or      bus_eff_date_to     is null
or      distributor_uid     is null
or      src_id              is null
or      edl_curated_ts      is null
or      current_version     is null);

    --day0
+------+--+
| _c0  |
+------+--+
| 0    |
+------+--+
1 row selected (10.401 seconds)

    --day1



--Null and hard coded Columns Validation //should return no record
--N/A


--Curation query
--////////////////////////day0

    --Day0 Record count
select  'rel_distributor_Source' as tableName, count(*)
from    (
            
            select  cast(s.firm_party_id as string)    as firm_party_id,
                    s.process_timestamp,
                    row_number() over(partition by s.firm_party_id order by s.process_timestamp desc) as rn
            from    inv_jhi_typed_qa.sdh_firm  s
        ) a  
where   a.rn = 1
and     a.firm_party_id not in 
        (select src_id
        from    inv_curated_eod_qa.20200222_bkp_master_rel_distributor --master records are loaded in this backup table
        where   src = 'JHISDH'
        )
union all
select 'rel_distributor_Target' as tableName, count(*) from inv_curated_eod_qa.20200225_bkp_SDH_day0_rel_distributor where src = 'JHISDH';

+-------------------------+----------+--+
|      _u1.tablename      | _u1._c1  |
+-------------------------+----------+--+
| rel_distributor_Source  | 1614     |
| rel_distributor_Target  | 1614     |
+-------------------------+----------+--+
2 rows selected (10.88 seconds)



    --Day0 Data validation
with source as (
select  *
from    ( 
            select  nvl(cast(s.firm_party_id as string) ,'<NULL>')  as firm_party_id,
                    b.bus_unit_key,
                    s.process_timestamp,
                    row_number() over(partition by s.firm_party_id order by s.process_timestamp desc) as rn
            from    inv_jhi_typed_qa.sdh_firm      s,
                    inv_curated_eod_qa.bd_business_unit    b
            where   trim(b.bus_unit_name) = 'JHI'
            and     current_version = 'Y'
        ) a 
where   a.rn = 1
and     a.firm_party_id not in 
        (select src_id
        from    inv_curated_eod_qa.20200222_bkp_master_rel_distributor --master records are loaded in this backup table
        where   src = 'JHISDH'
        )
)
select  count(*)
from    source
left outer join 
    (   select  src,
                cast(bus_eff_date_from as timestamp)  as bus_eff_date_from,
                cast(bus_eff_date_to as timestamp)    as bus_eff_date_to,
                nvl(src_id,'<NULL>')                  as src_id,
                edl_curated_ts                        as edl_curated_ts,
                distributor_uid                       as distributor_uid,
                current_version
        from    inv_curated_eod_qa.20200225_bkp_SDH_day0_rel_distributor
    ) target
on      source.firm_party_id = target.src_id
and     target.bus_eff_date_from = cast('1980-01-01 00:00:00.0' as timestamp)
and     target.bus_eff_date_to = cast('2099-12-31 00:00:00.0' as timestamp)
and     target.current_version = 'Y'
and     target.edl_curated_ts = '2020-02-24 17:15:48.933'
and     target.src = 'JHISDH'
where   target.distributor_uid is null; --should return no records
+------+--+
| _c0  |
+------+--+
| 0    |
+------+--+
1 row selected (13.619 seconds)

/*and     a.firm_party_id not in 
        (select src_id
        from    inv_curated_eod_qa.20200225_bkp_SDH_rel_distributor --master records are loaded in this backup table
        where   src = 'JHISDH'
        )*/
--////////////////////////day1

    --Day1 Record count
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
                    select  nvl(cast(s.firm_party_id as string) ,'<NULL>')  as firm_party_id,
                            b.bus_unit_key,
                            s.process_timestamp,
                            row_number() over(partition by s.firm_party_id order by s.process_timestamp desc) as rn
                    from    inv_jhi_typed_qa.sdh_firm      s,
                            inv_curated_eod_qa.bd_business_unit    b,
                            CurrentCurationDateTime             c
                    where   trim(b.bus_unit_name) = 'JHI'
                    and     b.current_version = 'Y'
                    and     c.rank = 2
                    and     s.process_timestamp > c.end_time
                ) a 
        where   a.rn = 1
        and     a.firm_party_id not in 
        (select r.src_id
        from    inv_curated_eod_qa.rel_distributor r inner join inv_curated_eod_qa.20200225_bkp_SDH_bd_distributor d --day0 backup table
        on      d.distributor_uid = r.distributor_uid
        and     d.current_version = r.current_version
        )
)
select  count(*)
from    source;

+------+--+
| _c0  |
+------+--+
| 2    |
+------+--+
1 row selected (28.152 seconds)


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
                    select  nvl(cast(s.firm_party_id as string) ,'<NULL>')  as firm_party_id,
                            b.bus_unit_key,
                            s.process_timestamp,
                            row_number() over(partition by s.firm_party_id order by s.process_timestamp desc) as rn
                    from    inv_jhi_typed_qa.sdh_firm      s,
                            inv_curated_eod_qa.bd_business_unit    b,
                            CurrentCurationDateTime             c
                    where   trim(b.bus_unit_name) = 'JHI'
                    and     b.current_version = 'Y'
                    and     c.rank = 2
                    and     s.process_timestamp > c.end_time
                ) a 
        where   a.rn = 1
        and     a.firm_party_id not in 
        (select r.src_id
        from    inv_curated_eod_qa.rel_distributor r inner join inv_curated_eod_qa.20200225_bkp_SDH_bd_distributor d --day0 backup table
        on      d.distributor_uid = r.distributor_uid
        and     d.current_version = r.current_version
        )
)
select  count(*)
from    source,
        inv_curated_eod_qa.rel_distributor target,
        CurrentCurationDateTime c
where   source.firm_party_id = target.src_id
and     target.bus_eff_date_from = cast('1980-01-01 00:00:00.0' as timestamp)
and     target.bus_eff_date_to = cast('2099-12-31 00:00:00.0' as timestamp)
and     target.current_version = 'Y'
and     target.src = 'JHISDH'
and     c.rank = 1
and     target.edl_curated_ts >= c.start_time
and     target.edl_curated_ts <= c.end_time;

+------+--+
| _c0  |
+------+--+
| 2    |
+------+--+
1 row selected (14.156 seconds)
