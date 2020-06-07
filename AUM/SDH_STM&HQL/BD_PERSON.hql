set hivevar:targetschema = inv_curated_eod_qa;
set hivevar:targettable = bd_person;
set hivevar:sourceschema = inv_jhi_typed_qa;
set hivevar:sourcetable = sdh_individual;
set hivevar:day0backup = 20200108_bkp_bd_person;

--Target Table Creation and Schema validation
describe    inv_curated_eod_qa.bd_person;

--Registry Log Validation
select  *
from    inv_curated_eod_qa.curation_registry
where   job_name = 'sdh_curation';

--Natural Keys valiation - NoNull //should return no record
select  count(*)
from    inv_curated_eod_qa.bd_person
where   src = 'JHISDH'
and     src_ids['JHISDH'] is null;

--Natural Keys valiation - NoDuplicate //should return no record
select  src_ids['JHISDH'],
        count(*)
from    inv_curated_eod_qa.bd_person
where   src = 'JHISDH'
and     current_version = 'Y'
group by src_ids['JHISDH'] 
having  count(*) >1;

--Mandatory Fields Validation //should return no record
select  count(*)
from    inv_curated_eod_qa.bd_person
where   src = 'JHISDH'
and     (trim(person_key) is null
or      person_full_name is null
or      bus_eff_date_from is null
or      bus_eff_date_to is null
or      trim(current_version) is null
or      trim(src_ids['JHISDH']) is null);

--Null and hard coded Columns Validation //should return no record
select  count(*)
from    inv_curated_eod_qa.bd_person
where   src = 'JHISDH'
and     (inv_advisor_code is not null
or      branch_code is not null
or      primary_cntry_domicile is not null
or      region_code is not null
or      managing_director_name is not null
or      regional_director_name is not null
or      md_recruiter_name is not null);

--Curation query
--////////////////////////day0

    --Day0 Record count
select  'bd_person_Target' as tableName, count(*) 
from    inv_curated_eod_qa.bd_person 
where   src = 'JHISDH'
union all
select  'bd_person_Source' as tableName, count(*)
from    (
            select  *,
                    row_number () over (partition by s.indv_party_id order by s.process_timestamp desc) as rn
            from    inv_jhi_typed_qa.sdh_individual
        ) a  
where   a.rn = 1;

    --Day0 Data validation
with source as (
select  *
from    (    
            select  s.indv_party_id                     as  src_ids,
                    concat('JHISDH_', s.indv_party_id)  as  person_uid,
                    case    when trim(s.first_name) is null and trim(s.last_name) is null           then    'Unknown'
                            when    trim(s.first_name) is null and trim(s.last_name) is not null    then    s.last_name
                            when    trim(s.first_name) is not null and trim(s.last_name) is null    then    s.first_name
                    else    concat(s.first_name,' ',s.last_name)
                    end                                 as  person_full_name,
                    s.first_name                        as  first_name,
                    s.last_name                         as  last_name,
                    s.rep_type                          as  inv_advisor_type,
                    s.process_timestamp                 as  process_timestamp,
                    row_number () over (partition by s.indv_party_id order by s.process_timestamp desc) as rn
            from    inv_jhi_typed_qa.sdh_individual  s
        ) a
where   a.rn = 1
),
CuratedTs as (
select  start_time,
        end_time
from    (
            select  start_time,
                    end_time,
                    row_number () over (partition by job_name order by start_time desc) as rn
            from    inv_curated_eod_qa.curation_registry
            where   job_name = 'sdh_curation'
)   b
where   b.rn = 1
)
select  count(*)
from    source  inner    join   inv_curated_eod_qa.bd_person    target
on      source.src_ids = target.src_ids['JHISDH']
inner join  CuratedTs
where   target.src = 'JHISDH'
and     nvl(trim(source.person_uid), '<Empty>') = nvl(trim(target.person_uid), '<Empty>')
and     nvl(trim(source.person_full_name), '<Empty>') = nvl(trim(target.person_full_name), '<Empty>')
and     nvl(trim(source.first_name), '<Empty>') = nvl(trim(target.first_name), '<Empty>')
and     nvl(trim(source.last_name), '<Empty>') = nvl(trim(target.last_name), '<Empty>')
and     nvl(trim(source.inv_advisor_type), '<Empty>') = nvl(trim(target.inv_advisor_type), '<Empty>')
and     target.bus_eff_date_from = cast('1980-01-01 00:00:00.0' as timestamp)
and     target.bus_eff_date_to = cast('2099-12-31 00:00:00.0' as timestamp)
and     target.edl_curated_ts >= CuratedTs.start_time
and     target.edl_curated_ts <= CuratedTs.end_time
and     target.current_version = 'Y';

--////////////////////////day1
    
    --day1 Record Count Validation
with    CurrentCurationDateTime as (
        select  start_time,
                end_time,
                row_number () over (partition by job_name order by start_time desc) as rank
        from    inv_curated_eod_qa.curation_registry
        where   job_name = 'sdh_curation'
),
source  as(
select  a.*
from    (    
            select  cast(s.indv_party_id as string)  as indv_party_id,
                    s.first_name,
                    s.last_name,
                    s.rep_type,
                    s.process_timestamp,
                    row_number () over (partition by s.indv_party_id order by s.process_timestamp desc) as rn
            from    inv_jhi_typed_qa.sdh_individual  s,
                    CurrentCurationDateTime          c
            where   c.rank = 2
            and     s.process_timestamp > c.end_time
        ) a
),
source_flag as (
select  cast(s.indv_party_id as string)     as  src_src_ids,
        concat('JHISDH_', s.indv_party_id)  as  src_person_uid,
        case    when trim(s.first_name) is null and trim(s.last_name) is null           then    'Unknown'
                when    trim(s.first_name) is null and trim(s.last_name) is not null    then    s.last_name
                when    trim(s.first_name) is not null and trim(s.last_name) is null    then    s.first_name
                else                                                                            concat(s.first_name,' ',s.last_name)
        end                                 as  src_person_full_name,
        s.first_name                        as  src_first_name,
        s.last_name                         as  src_last_name,
        s.rep_type                          as  src_inv_advisor_type,
        s.process_timestamp                 as  src_process_timestamp,
        
        t.src_ids['JHISDH']                 as  tar_src_ids,
        t.person_uid                        as  tar_person_uid,
        t.person_full_name                  as  tar_person_full_name,
        t.first_name                        as  tar_first_name,
        t.last_name                         as  tar_last_name,
        t.inv_advisor_type                  as  tar_inv_advisor_type,
        t.bus_eff_date_from                 as  tar_bus_eff_date_from,
        t.bus_eff_date_to                   as  tar_bus_eff_date_to,
        t.edl_curated_ts                    as  tar_edl_curated_ts,
        
        case    when t.src_ids['JHISDH'] is null                                            then 'new'
                when s.indv_party_id is not null and t.src_ids['JHISDH'] is not null
                        and (nvl(trim(s.first_name),'') <> nvl(trim(t.first_name),'')
                        or  nvl(trim(s.last_name),'') <> nvl(trim(t.last_name),'')
                        or  nvl(trim(s.rep_type),'') <> nvl(trim(t.inv_advisor_type),''))   then 'updated'
                else                                                                             'existing'
        end                                     as flag        
from    source s full outer join 
    (   select *
        from    inv_curated_eod_qa.20200119_bkp_bd_person  --day0 backup table
        where   src = 'JHISDH'
        and     current_version = 'Y') t
on      nvl(trim(s.indv_party_id),'<EMPTY>') = nvl(trim(t.src_ids['JHISDH']),'<EMPTY>')
where   s.indv_party_id is not null
) select    'bd_person_Source' as tableName, count(*) from source_flag where flag in ('new','updated')
union all
select  'bd_person_Target' as tableName, count(*) 
from    inv_curated_eod_qa.bd_person   t,
        CurrentCurationDateTime        c
where   src = 'JHISDH'
and     c.rank = 2
and     t.edl_curated_ts > c.end_time
and     current_version = 'Y';


    --day1 Data Validation updated/expired and existings
with    CurrentCurationDateTime as (
        select  start_time,
                end_time,
                row_number () over (partition by job_name order by start_time desc) as rank
        from    inv_curated_eod_qa.curation_registry
        where   job_name = 'sdh_curation'
),
source  as(
select  a.*
from    (    
            select  cast(s.indv_party_id as string)  as indv_party_id,
                    s.first_name,
                    s.last_name,
                    s.rep_type,
                    s.process_timestamp,
                    row_number () over (partition by s.indv_party_id order by s.process_timestamp desc) as rn
            from    inv_jhi_typed_qa.sdh_individual  s,
                    CurrentCurationDateTime          c
            where   c.rank = 1 --2
            and     s.process_timestamp > c.end_time
        ) a
where   a.rn = 1
),
source_flag as (
select  cast(s.indv_party_id as string)     as  src_src_ids,
        concat('JHISDH_', s.indv_party_id)  as  src_person_uid,
        case    when trim(s.first_name) is null and trim(s.last_name) is null           then    'Unknown'
                when    trim(s.first_name) is null and trim(s.last_name) is not null    then    s.last_name
                when    trim(s.first_name) is not null and trim(s.last_name) is null    then    s.first_name
                else                                                                            concat(s.first_name,' ',s.last_name)
        end                                 as  src_person_full_name,
        s.first_name                        as  src_first_name,
        s.last_name                         as  src_last_name,
        s.rep_type                          as  src_inv_advisor_type,
        s.process_timestamp                 as  src_process_timestamp,
        
        t.src_ids['JHISDH']                 as  tar_src_ids,
        t.person_uid                        as  tar_person_uid,
        t.person_full_name                  as  tar_person_full_name,
        t.first_name                        as  tar_first_name,
        t.last_name                         as  tar_last_name,
        t.inv_advisor_type                  as  tar_inv_advisor_type,
        t.bus_eff_date_from                 as  tar_bus_eff_date_from,
        t.bus_eff_date_to                   as  tar_bus_eff_date_to,
        t.edl_curated_ts                    as  tar_edl_curated_ts,
        
        case    when t.src_ids['JHISDH'] is null                                            then 'new'
                when s.indv_party_id is not null and t.src_ids['JHISDH'] is not null
                        and (nvl(trim(s.first_name),'') <> nvl(trim(t.first_name),'')
                        or  nvl(trim(s.last_name),'') <> nvl(trim(t.last_name),'')
                        or  nvl(trim(s.rep_type),'') <> nvl(trim(t.inv_advisor_type),''))   then 'updated'
                else                                                                             'existing'
        end                                     as flag        
from    source s full outer join 
    (   select *
        from    inv_curated_eod_qa.20200208_bkp_bd_person  --day0 backup table
        where   src = 'JHISDH'
        and     current_version = 'Y') t
on      nvl(trim(s.indv_party_id),'<EMPTY>') = nvl(trim(t.src_ids['JHISDH']),'<EMPTY>')
where   s.indv_party_id is not null
) ,
source_master    as (
-------------new records-------------
select  src_src_ids             as  src_ids,
        src_person_uid          as  person_uid,
        src_person_full_name    as  person_full_name,
        src_first_name          as  first_name,
        src_last_name           as  last_name,
        src_inv_advisor_type    as  inv_advisor_type,
        src_process_timestamp   as  edl_curated_ts,
        cast('1980-01-01 00:00:00.0' as timestamp)     as   bus_eff_date_from,
        cast('2099-12-31 00:00:00.0' as timestamp)     as   bus_eff_date_to,
        'Y'                     as  current_version,
        flag
from    source_flag
where   flag = 'new'

union all
-------------updated records / new inserted-------------
select  src_src_ids             as  src_ids,
        src_person_uid          as  person_uid,
        src_person_full_name    as  person_full_name,
        src_first_name          as  first_name,
        src_last_name           as  last_name,
        src_inv_advisor_type    as  inv_advisor_type,
        src_process_timestamp   as  edl_curated_ts,
        cast(concat(to_date(src_process_timestamp),' 00:00:00') as timestamp) as bus_eff_date_from,
        cast('2099-12-31 00:00:00.0' as timestamp)  as  bus_eff_date_to,
        'Y'                     as  current_version,
        flag    
from    source_flag
where   flag = 'updated'

union all
-------------updated records / Expired records-------------
select  tar_src_ids             as  src_ids,
        tar_person_uid          as  person_uid,
        tar_person_full_name    as  person_full_name,
        tar_first_name          as  first_name,
        tar_last_name           as  last_name,
        tar_inv_advisor_type    as  inv_advisor_type,
        tar_edl_curated_ts      as  edl_curated_ts,
        tar_bus_eff_date_from   as  bus_eff_date_from,
        cast(concat(to_date(src_process_timestamp),' 00:00:00') as timestamp) as bus_eff_date_to,
        'N'                     as  current_version,
        flag    
from    source_flag
where   flag = 'updated'

union all
-------------existing records-------------
select  tar_src_ids             as  src_ids,
        tar_person_uid          as  person_uid,
        tar_person_full_name    as  person_full_name,
        tar_first_name          as  first_name,
        tar_last_name           as  last_name,
        tar_inv_advisor_type    as  inv_advisor_type,
        tar_edl_curated_ts      as  edl_curated_ts,
        tar_bus_eff_date_from   as  bus_eff_date_from,
        tar_bus_eff_date_to     as bus_eff_date_to,
        'Y'                     as current_version,
        flag  
from    source_flag 
where   flag='existing'
)
select  distinct flag,current_version, 
        count(*) over (partition by flag,current_version ) as flag_cnt
from    source_master;
select  count(*)
from    source_master source
where   (source.flag ='existing' and source.current_version = 'Y' or    source.flag ='updated' and source.current_version = 'N')
and     exists (
        select  *
        from    inv_curated_eod_qa.bd_person  target,
                CurrentCurationDateTime       c
        where   target.src = 'JHISDH'        
        and     c.rank = 1
        and     source.edl_curated_ts = target.edl_curated_ts
        and     cast(source.src_ids as string) = cast(target.src_ids['JHISDH'] as string)
        and     nvl(trim(source.person_uid), '<Empty>') = nvl(trim(target.person_uid), '<Empty>')
        and     nvl(trim(source.person_full_name), '<Empty>') = nvl(trim(target.person_full_name), '<Empty>')
        and     nvl(trim(source.first_name), '<Empty>') = nvl(trim(target.first_name), '<Empty>')
        and     nvl(trim(source.last_name), '<Empty>') = nvl(trim(target.last_name), '<Empty>')
        and     nvl(trim(source.inv_advisor_type), '<Empty>') = nvl(trim(target.inv_advisor_type), '<Empty>')
        and     source.bus_eff_date_from = target.bus_eff_date_from
        and     source.bus_eff_date_to = target.bus_eff_date_to
        and     source.current_version = target.current_version);
        

    --day1 Data Validation updated/new and brand new
select  count(*)
from    source_master source
where   ((source.flag ='new' and source.current_version = 'Y') or    (source.flag ='updated' and source.current_version = 'Y'))
and     exists (
        select  *
        from    inv_curated_eod_qa.bd_person  target,
                CurrentCurationDateTime       c
        where   target.src = 'JHISDH'        
        and     c.rank = 1
        and     target.edl_curated_ts >= c.start_time
        and     target.edl_curated_ts <= c.end_time
        and     cast(source.src_ids as string) = cast(target.src_ids['JHISDH'] as string)
        and     nvl(trim(source.person_uid), '<Empty>') = nvl(trim(target.person_uid), '<Empty>')
        and     nvl(trim(source.person_full_name), '<Empty>') = nvl(trim(target.person_full_name), '<Empty>')
        and     nvl(trim(source.first_name), '<Empty>') = nvl(trim(target.first_name), '<Empty>')
        and     nvl(trim(source.last_name), '<Empty>') = nvl(trim(target.last_name), '<Empty>')
        and     nvl(trim(source.inv_advisor_type), '<Empty>') = nvl(trim(target.inv_advisor_type), '<Empty>')
        and     source.bus_eff_date_from = target.bus_eff_date_from
        and     source.bus_eff_date_to = target.bus_eff_date_to
        and     source.current_version = target.current_version);


select  distinct flag,current_version, 
        count(*) over (partition by flag,current_version ) as flag_cnt
from    source_master;


        and     target.edl_curated_ts >= c.start_time
        and     target.edl_curated_ts <= c.end_time

--Data Mockup for day1 - Jan13 done
select  max(src_ids['JHISDH']),min(src_ids['JHISDH'])
from    inv_curated_eod_qa.bd_person
where   src = 'JHISDH'
and     current_version = 'Y'
+---------+----------+--+
|   _c0   |   _c1    |
+---------+----------+--+
| 999999  | 1000000  |
+---------+----------+--+


select  src_ids['JHISDH'] as id,first_name, last_name, inv_advisor_type,current_version,edl_curated_ts
from    inv_curated_eod_qa.bd_person
where   src = 'JHISDH'
and     current_version = 'Y'
and     src_ids['JHISDH'] in ('1596209','1596210','1596206','1596171','1596148','1596207','1596125','1596124');
+----------+----------------+----------------------------------+-------------------+------------------+--------------------------+--+
|    id    |   first_name   |            last_name             | inv_advisor_type  | current_version  |      edl_curated_ts      |
+----------+----------------+----------------------------------+-------------------+------------------+--------------------------+--+
| 1596171  | David          | Hunt_udpate2                     | Hybrid Rep        | Y                | 2020-01-16 19:59:13.884  |
| 1596206  | House_udpate1  | Rina Wealth Management Services  | House             | Y                | 2020-01-16 19:59:13.884  |
| 1596124  | House          | Default Rep                      | Default Rep       | Y                | 2020-01-16 19:59:13.884  |
| 1596207  | FirstName1     | LastName1                        | House             | Y                | 2020-01-16 19:59:13.884  |
| 1596148  | Reis           | Lubitz                           | Non Producer      | Y                | 2020-01-16 19:59:13.884  |
| 1596125  | NULL           | NULL                             | NULL              | Y                | 2020-01-16 19:59:13.884  |
+----------+----------------+----------------------------------+-------------------+------------------+--------------------------+--+



use inv_jhi_typed_qa;
--new records *2
Insert into sdh_individual partition(process_date) 
values (1596209,'FirstName1',null,'LastName1',null,null,null,null,'',null,709,null,null,null,null,null,null,null,null,null,null,'R',null,null,null,null,'',null,null,null,null,null,null,null,'Y','30000852216',null,null,null,null,null,null,null,'1000752001','2019-10-15 00:00:00.0','1000753001','2019-10-16 00:00:00.0',null,null,null,null,null,'','','','','','House','N',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,'0',null,null,null,'2020-01-19 19:17:34.0','2020-01-19');
Insert into sdh_individual partition(process_date) 
values (1596210,'FirstName2','MiddleName2','LastName2','Mr.',null,null,null,'','2772872',709,null,null,null,null,null,null,null,null,null,null,'R',null,null,null,null,'',null,null,null,null,null,null,null,'N','30000657787',null,null,null,null,null,null,null,'1000752001','2019-10-15 00:00:00.0','1000753001','2019-10-16 00:00:00.0',null,null,null,null,null,'','','','','','Hybrid Rep','N',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,'0',null,null,null,'2020-01-19 19:17:34.0','2020-01-19');

--updated records *4
Insert into sdh_individual partition(process_date) --FirstName(FName_update1)
values (1596206,'FName_update1',null,'Rina Wealth Management Services',null,null,null,null,'',null,709,null,null,null,null,null,null,null,null,null,null,'R',null,null,null,null,'',null,null,null,null,null,null,null,'Y','30000852216',null,null,null,null,null,null,null,'1000752001','2019-10-15 00:00:00.0','1000753001','2019-10-16 00:00:00.0',null,null,null,null,null,'','','','','','House','N',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,'0',null,null,null,'2020-01-19 19:17:34.0','2020-01-19');
Insert into sdh_individual partition(process_date) --lastName(LName_udpate2)
values (1596171,'David','Lee','LName_udpate2','Mr.',null,null,null,'','2772872',709,null,null,null,null,null,null,null,null,null,null,'R',null,null,null,null,'',null,null,null,null,null,null,null,'N','30000657787',null,null,null,null,null,null,null,'1000752001','2019-10-15 00:00:00.0','1000753001','2019-10-16 00:00:00.0',null,null,null,null,null,'','','','','','Hybrid Rep','N',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,'0',null,null,null,'2020-01-19 19:17:34.0','2020-01-19');
Insert into sdh_individual partition(process_date)  --rep_tyep='Hybrid Rep'
values (1596148,'Reis','Mitchell','Lubitz','Mr.',null,null,null,'','6791941',1180,1246,null,null,null,null,null,null,null,null,null,'R',null,null,null,null,'',null,null,null,null,null,null,null,'N','30000856271',null,null,null,null,null,null,null,'1000752001','2019-10-15 00:00:00.0','1000753001','2019-10-16 00:00:00.0',null,null,null,null,null,'10','6','','','','Hybrid Rep','N',1246,1247,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,'0',null,null,null,'2020-01-19 19:17:34.0','2020-01-19');
Insert into sdh_individual partition(process_date) --update first and last name to NULL
values (1596207,null,null,null,'Mr.',null,null,null,'','6649550',709,1246,null,null,null,null,null,null,null,null,null,'R',null,null,null,null,'',null,null,null,null,null,null,null,'N','30000874234',null,null,null,null,null,null,null,'1000752001','2019-10-15 00:00:00.0','1000753001','2019-10-16 00:00:00.0',null,null,null,1430,'2019-10-16 13:45:03.797','','','','','','Non Producer','N',1246,1247,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,'0',null,null,null,'2020-01-19 19:17:34.0','2020-01-19');

--existing *2
Insert into sdh_individual partition(process_date) 
values (1596125,null,null,null,null,null,null,null,'',null,899,null,null,null,null,null,null,null,null,null,null,'R',null,null,null,null,'',null,null,null,null,null,null,null,'N','64173494855912248','2019-10-16 16:23:21.71',null,null,null,null,null,null,'1000752001','2019-10-15 00:00:00.0','1000753001','2019-10-16 00:00:00.0',406008,441513,501265,1386,'2019-10-16 16:23:21.71','','','','','',null,' ',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,'0',null,null,null,'2020-01-19 19:17:34.0','2020-01-19');
Insert into sdh_individual partition(process_date) 
values (1596124,'House',null,'Default Rep',null,null,null,null,'',null,709,1271,null,null,null,null,null,null,null,null,null,'R',null,null,null,null,'',null,null,null,null,null,null,null,'N','30000946756',null,null,null,null,null,null,null,'1000752001','2019-10-15 00:00:00.0','1000753001','2019-10-16 00:00:00.0',null,null,null,1386,'2019-10-16 06:39:55.02','','','','','','Default Rep','N',1271,1272,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,'0',null,null,null,'2020-01-19 19:17:34.0','2020-01-19');

--TC015_AUM_JHI_Curation_BD_PERSON_FailureNotification
select  *
from    inv_curated_eod_qa.error_log a
order by a.start_time desc
limit   10;