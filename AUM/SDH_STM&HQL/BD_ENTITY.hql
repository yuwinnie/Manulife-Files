set hivevar:targetschema = inv_curated_eod_qa;
set hivevar:targettable = bd_entity;
set hivevar:sourceschema = inv_jhi_typed_qa;
set hivevar:sourcetable = sdh_firm;
set hivevar:day0backup = 20191216_bkp_bd_entity;

--Target table creation
use     inv_curated_eod_qa;
show    tables;

--Target Table Creation and Schema validation
describe    inv_curated_eod_qa.bd_entity;
show partitions inv_curated_eod_qa.bd_entity;
--Registry Log Validation
select  *
from    inv_curated_eod_qa.curation_registry
where   job_name = 'sdh_curation';

--Natural Keys valiation - NoNull //should return no record
select  count(*)
from    inv_curated_eod_qa.bd_entity
where   src = 'JHISDH'
and     src_entity_id is null;

--Natural Keys valiation - NoDuplicate //should return no record
select  src_entity_id,
        count(*)
from    inv_curated_eod_qa.bd_entity
where   src = 'JHISDH'
and     current_version = 'Y'
group by src_entity_id 
having  count(*) >1;

--Mandatory Fields Validation //should return no record
select  count(*)
from    inv_curated_eod_qa.bd_entity
where   src = 'JHISDH' 
and     (entity_key is null
or      src_entity_id is null
or      bus_eff_date_to is null
or      bus_eff_date_from is null
or      cntry_of_incorporation is null
or      current_version is null
or      edl_create_ts is null);

--Null and hard coded Columns Validation //should return no record
select  count(*)
from    inv_curated_eod_qa.bd_entity
where   src = 'JHISDH'
and     (cntry_of_incorporation  <> 'USA'
or      ml_functional_currency is not null
or      acquired_by_parent is not null
or      alternate_company_name is not null
or      cie_des is not null
or      cntry_of_domicile is not null
or      cntry_of_risk is not null
or      company_address is not null
or      company_corp_ticker is not null
or      company_is_private is not null
or      company_legal_name is not null
or      company_to_parent_relationship is not null
or      country_risk_iso_code is not null
or      ddis_avg_coupon_issuer is not null
or      ddis_avg_mty_issuer is not null
or      eqy_sic_code is not null
or      id_bb_company is not null
or      id_bb_glb_company is not null
or      id_bb_glb_company_name is not null
or      id_bb_glb_obligor_company is not null
or      id_bb_glb_obligor_name is not null
or      id_bb_glb_parent_co is not null
or      id_bb_glb_parent_company_name is not null
or      id_bb_glb_ultimate_parent_co is not null
or      id_bb_parent_co is not null
or      id_bb_ultimate_parent_co is not null
or      id_replacement_identifier is not null
or      is_ult_parent is not null
or      issuer_name_types is not null
or      legal_entity_identifier is not null
or      lei_assigned_date is not null
or      lei_disabled_date is not null
or      lei_entity_status is not null
or      lei_last_update is not null
or      lei_legal_form is not null
or      lei_maintenance_state is not null
or      lei_name is not null
or      lei_next_maintenance_date is not null
or      lei_record_state is not null
or      lei_registration_address is not null
or      lei_ultimate_parent_company is not null
or      ml_analyst is not null
or      ml_broker_code is not null
or      ml_buy_sell_flag is not null
or      ml_elimination_company_id is not null
or      ml_entity_status is not null
or      ml_ile_branch is not null
or      ml_investga_name_owner is not null
or      ml_irr_rating is not null
or      ml_jurisdiction is not null
or      ml_lawson_entity_type is not null
or      ml_lawson_legal_id is not null
or      ml_life_company_type is not null
or      ml_report_pool_assets is not null
or      ml_role is not null
or      ml_sec_lending_flag is not null
or      ml_strght_repo_flag is not null
or      ml_tax_exempt_flag is not null
or      oblig_industry_subgroup is not null
or      obligor_bbid is not null
or      ofac_full_sanc_company is not null
or      parent_obligor_id is not null
or      parent_obligor_name is not null
or      state_of_domicile is not null
or      state_of_incorporation is not null
or      ult_parent_cntry_domicile is not null
or      ult_parent_cntry_incorporation is not null
or      ult_parent_cntry_of_risk is not null
or      ult_parent_corp_ticker is not null
or      ult_parent_ticker_exchange is not null
or      rtg_fitch_short_term is not null
or      rtg_mdy_short_term_debt is not null
or      rtg_dbrs_st_issuer_rating is not null
or      rtg_fitch_long_term is not null
or      rtg_mdy_issuer is not null
or      rtg_dbrs_lt_issuer_rating is not null
or      rtg_sp_st_issuer_credit is not null
or      mm_fitch_rtg_shrt is not null
or      mm_mdy_rtg_shrt is not null
or      mm_sp_rtg_sht is not null
or      rtg_fitch_issuer_outlook is not null
or      rtg_sp_issuer_rating is not null
or      gics_sector_cd is not null
or      gics_industry_group_cd is not null
or      gics_industry_cd is not null
or      gics_sub_industry_cd is not null
or      gics_sector_name is not null
or      gics_industry_name is not null
or      gics_sub_industry_name is not null
or      gics_industry_group_name is not null
or      rtg_moody_long_term is not null
or      rtg_sp_outlook is not null
or      rtg_sp_outlook_dir is not null
or      rtg_sp_outlook_dt is not null
or      rtg_sp_lt_fi_credit is not null
or      rtg_sp_st_fi_credit is not null
or      rtg_sp_st_fi_credit_dir is not null
or      rtg_sp_st_fi_credit_dt is not null
or      rtg_sp_lt_li_credit is not null
or      rtg_sp_lt_li_credit_dir is not null
or      rtg_sp_lt_li_credit_dt is not null
or      rtg_sp_st_li_credit is not null
or      rtg_sp_st_li_credit_dir is not null
or      rtg_sp_st_li_credit_dt is not null
or      rtg_sp_lt_fc_issuer_credit is not null
or      rtg_sp_lt_fc_issuer_credit_dir is not null
or      rtg_sp_lt_fc_issuer_credit_dt is not null
or      rtg_sp_lt_fc_iss_cred_rtg_dt is not null
or      id_bb_ultimate_parent_co_name is not null
or      src_apollo_id is not null
or      affiliate_ind is not null
or      entity_type is not null
or      commited_capital is not null
or      non_committed_capital is not null
or      shariah_compliant_ind is not null
or      has_bus_unit_ind is not null
or      client_start_date is not null
or      client_end_date is not null
or      src_slx_id is not null
or      src_idr_id is not null
);

--Curation query
--////////////////////////day0

    --Day0 Record count
select  'bd_entity_Target' as tableName, count(*) 
from    inv_curated_eod_qa.bd_entity 
where   src = 'JHISDH'
union all
select  'bd_entity_Source' as tableName, count(*)
from    (
            select  *,
                    row_number () over (partition by s.firm_party_id order by s.process_timestamp desc) as rn
            from    inv_jhi_typed_qa.sdh_firm
        ) a  
where   a.rn = 1;


    --Day0 Data validation
with source as (
select  *
from    (    
            select  s.firm_name,
                    s.firm_party_id,
                    s.process_timestamp,
                    row_number () over (partition by s.firm_party_id order by s.process_timestamp desc) as rn
            from    inv_jhi_typed_qa.sdh_firm
        ) a
where   a.rn = 1
)
select  count(*) 
from    source
inner outer  join   inv_curated_eod_qa.bd_entity    target
on      nvl(source.firm_party_id,'<EMPTY>')=nvl(target.src_entity_id,'<EMPTY>')
and     nvl(source.firm_party_id,'<EMPTY>')=nvl(target.src_saleshub_id,'<EMPTY>')
and     target.bus_eff_date_from = cast('1980-01-01 00:00:00.0' as timestamp)
and     target.bus_eff_date_to = cast('2099-12-31 00:00:00.0' as timestamp)
and     target.edl_create_ts = source.process_timestamp
and     nvl(trim(source.firm_name),'<EMPTY>')=nvl(trim(target.long_comp_name),'<EMPTY>')
and     trim(target.current_version) = 'Y'
where   target.src = 'JHISDH'
and     target.src_entity_id is null;


--////////////////////////day1
    --day1 Record Count Validation
with    CurrentCurationDateTime as (
        select  start_time,
                end_time,
                row_number () over (partition by job_name order by start_time desc) as rank
        from    inv_curated_eod_qa.20200121_bkp_curation_registry
        where   job_name = 'sdh_curation'
),
source as ( 
select  a.*
from    (    
            select  'JHISDH'            as src,
                    s.firm_name         as long_comp_name,
                    cast(s.firm_party_id as string)    as src_saleshub_id,
                    s.process_timestamp as edl_create_ts,
                    row_number() over(partition by s.firm_party_id order by s.process_timestamp desc) as rn
            from    inv_jhi_typed_qa.sdh_firm   s,
                    CurrentCurationDateTime     c
            where   c.rank = 2
            and     s.process_timestamp > c.end_time
        ) a 
where   a.rn = 1
) ,
source_flag    as (
select  s.src                   as src_src,
        s.src_saleshub_id       as src_src_saleshub_id,
        s.long_comp_name        as src_long_comp_name,
        s.edl_create_ts         as src_edl_create_ts,

        t.src                   as tar_src,
        t.src_saleshub_id       as tar_src_saleshub_id,
        t.long_comp_name        as tar_long_comp_name,
        t.edl_create_ts         as tar_edl_create_ts ,
        t.bus_eff_date_from     as tar_bus_eff_date_from,
        t.bus_eff_date_to       as tar_bus_eff_date_to,
        
        case    when t.src_saleshub_id is null                                                              then    'new'
                when s.src_saleshub_id is not null and t.src_saleshub_id is not null
                     and  nvl(trim(s.long_comp_name),'<EMPTY>') <>  nvl(trim(t.long_comp_name),'<EMPTY>')   then    'updated'
                else                                                                                                'existing'
        end                     as flag

from    source s full outer join 
    (   select *
        from    inv_curated_eod_qa.20200208_bkp_bd_entity  --day0 backup table
        where   src = 'JHISDH'
        and     current_version = 'Y') t
on       nvl(trim(s.src_saleshub_id),'<EMPTY>') = nvl(trim(t.src_saleshub_id),'<EMPTY>')
where   s.src_saleshub_id is not null
)
select  'bd_entity_Source' as tableName, count(*)   from source_flag where flag in ('new','updated')
union all
select  'bd_entity_Target' as tableName, count(*) 
from    inv_curated_eod_qa.20200121_bkp_bd_entity   t,
        CurrentCurationDateTime             c
where   src = 'JHISDH'
and     c.rank = 2
and     t.edl_create_ts > c.end_time
and     current_version = 'Y';

    --day1 Data Validation    
with    CurrentCurationDateTime as (
        select  start_time,
                end_time,
                row_number () over (partition by job_name order by start_time desc) as rank
        from    inv_curated_eod_qa.curation_registry
        where   job_name = 'sdh_curation'
),
source as ( 
select  a.*
from    (    
            select  'JHISDH'            as src,
                    s.firm_name         as long_comp_name,
                    cast(s.firm_party_id as string)    as src_saleshub_id,
                    s.process_timestamp as edl_create_ts,
                    row_number() over(partition by s.firm_party_id order by s.process_timestamp desc) as rn
            from    inv_jhi_typed_qa.sdh_firm   s,
                    CurrentCurationDateTime     c
            where   c.rank = 1--2
            and     s.process_timestamp > c.end_time
        ) a 
where   a.rn = 1
) ,
source_flag    as (
select  s.src                   as src_src,
        s.src_saleshub_id       as src_src_saleshub_id,
        s.long_comp_name        as src_long_comp_name,
        s.edl_create_ts         as src_edl_create_ts,

        t.src                   as tar_src,
        t.src_saleshub_id       as tar_src_saleshub_id,
        t.long_comp_name        as tar_long_comp_name,
        t.edl_create_ts         as tar_edl_create_ts ,
        t.bus_eff_date_from     as tar_bus_eff_date_from,
        t.bus_eff_date_to       as tar_bus_eff_date_to,
        
        case    when t.src_saleshub_id is null                                                              then    'new'
                when s.src_saleshub_id is not null and t.src_saleshub_id is not null
                     and  nvl(trim(s.long_comp_name),'<EMPTY>') <>  nvl(trim(t.long_comp_name),'<EMPTY>')   then    'updated'
                else                                                                                                'existing'
        end                     as flag

from    source s full outer join 
    (   select *
        from    inv_curated_eod_qa.20200208_bkp_bd_entity  --day0 backup table
        where   src = 'JHISDH'
        and     current_version = 'Y') t
on       nvl(trim(s.src_saleshub_id),'<EMPTY>') = nvl(trim(t.src_saleshub_id),'<EMPTY>')
where   s.src_saleshub_id is not null
),
source_master    as (
-------------new records-------------
select  src_src                 as src,
        src_src_saleshub_id     as src_saleshub_id,
        src_src_saleshub_id     as src_entity_id,
        cast('1980-01-01 00:00:00' as timestamp) as bus_eff_date_from,
        cast('2099-12-31 00:00:00' as timestamp) as bus_eff_date_to,
        src_long_comp_name      as long_comp_name,
        src_edl_create_ts       as edl_create_ts,
        'Y'                     as current_version,
        flag  
from    source_flag 
where   flag='new'

union all
-------------updated records / new inserted-------------
select  src_src                 as src,
        src_src_saleshub_id     as src_saleshub_id,
        src_src_saleshub_id     as src_entity_id,
        cast(concat(to_date(src_edl_create_ts),' 00:00:00') as timestamp) as bus_eff_date_from,
        cast('2099-12-31 00:00:00' as timestamp) as bus_eff_date_to,
        src_long_comp_name      as long_comp_name,
        src_edl_create_ts       as edl_create_ts,
        'Y'                     as current_version,
        flag  
from    source_flag 
where   flag='updated'

union all
-------------updated records / expired records-------------
select  tar_src                 as src,
        tar_src_saleshub_id     as src_saleshub_id,
        tar_src_saleshub_id     as src_entity_id,
        tar_bus_eff_date_from   as bus_eff_date_from,
        cast(concat(to_date(src_edl_create_ts),' 00:00:00') as timestamp) as bus_eff_date_to,
        tar_long_comp_name      as long_comp_name,
        tar_edl_create_ts       as edl_create_ts,
        'N'                     as current_version,
        flag  
from    source_flag 
where   flag='updated'

union all
-------------existing records-------------
select  tar_src                 as src,
        tar_src_saleshub_id     as src_saleshub_id,
        tar_src_saleshub_id     as src_entity_id,
        tar_bus_eff_date_from   as bus_eff_date_from,
        tar_bus_eff_date_to     as bus_eff_date_to,
        tar_long_comp_name      as long_comp_name,
        tar_edl_create_ts       as edl_create_ts,
        'Y'                     as current_version,
        flag  
from    source_flag 
where   flag='existing'
)
select  distinct flag,current_version, 
        count(*) over (partition by flag,current_version ) as flag_cnt
from    source_master;

select  count(*)
from    source_master   source
where   exists (
        select  *
        from    inv_curated_eod_qa.bd_entity  target
        where   target.src = 'JHISDH'
        and     nvl(trim(source.src_entity_id),'<EMPTY>')=nvl(trim(target.src_entity_id),'<EMPTY>')
        and     nvl(trim(source.src_saleshub_id),'<EMPTY>') = nvl(trim(target.src_saleshub_id),'<EMPTY>')
        and     source.bus_eff_date_from=target.bus_eff_date_from
        and     source.bus_eff_date_to=target.bus_eff_date_to
        and     nvl(trim(source.long_comp_name),'<EMPTY>')=nvl(trim(target.long_comp_name),'<EMPTY>')
        and     nvl(trim(source.current_version),'<EMPTY>')=nvl(trim(target.current_version),'<EMPTY>')
        and     source.edl_create_ts=target.edl_create_ts);
        
--TC015_AUM_JHI_Curation_BD_PERSON_FailureNotification
select  *
from    inv_curated_eod_qa.error_log a
order by a.start_time desc
limit   10;

--Data Mockup for day1 - Jan13 done
select  max(src_entity_id),min(src_entity_id)
from    inv_curated_eod_qa.bd_entity
where   src = 'JHISDH'
and     current_version = 'Y';
+---------+----------+--+
|   _c0   |   _c1    |
+---------+----------+--+
| 440363  | 1234873  |
+---------+----------+--+

select  src_entity_id,long_comp_name,current_version,edl_create_ts
from    inv_curated_eod_qa.bd_entity
where   src = 'JHISDH'
and     current_version = 'Y'
and     src_entity_id in ('440362','440363')
and     src_entity_id in ('440338','440339','440336','440337','400273','400272','400271','400275');
+---------+---------+--+
|   _c0   |   _c1   |
+---------+---------+--+
| 440361  | 100000  |
+---------+---------+--+


Insert into SDH_FIRM
values (1593887,'ZWEIG ADVISERS LLC BrandNew','Zweig Advisers','108784',null,null,706,1207,null,'<EMPTY>',null,null,'<EMPTY>','1000003001','2017-07-31 00:00:00.0','1000024001','2017-08-21 00:00:00.0',null,'3029540',null,null,null,null,null,'<EMPTY>','<EMPTY>','<EMPTY>','<EMPTY>','<EMPTY>',1208,'2019-12-18 12:12:12.0');
Insert into SDH_FIRM
values (1593888,'ZRCOR INC BrandNew','Zee, Roland','147705',null,null,706,1207,null,'<EMPTY>',null,null,'<EMPTY>','1000003001','2017-07-31 00:00:00.0','1000024001','2017-08-21 00:00:00.0',null,'3007840',null,null,null,null,null,'<EMPTY>','<EMPTY>','<EMPTY>','<EMPTY>','<EMPTY>',1208,'2019-12-18 12:12:12.0');
Insert into SDH_FIRM
values (400245,'ZERO GRAVITY FINANCIAL LLC Updated','Zero Gravity Fi','169905',null,null,706,1207,null,'<EMPTY>',null,null,'<EMPTY>','1000003001','2017-07-31 00:00:00.0','1000024001','2017-08-21 00:00:00.0',null,'3014181',null,null,null,null,null,'<EMPTY>','<EMPTY>','<EMPTY>','<EMPTY>','<EMPTY>',1208,'2019-12-18 12:12:12.0');
Insert into SDH_FIRM 
values (400252,'ZAPPITELLI FINANCIAL SERVICES Updated','Zappitelli Fina','119711',null,null,706,1207,null,'<EMPTY>',null,null,'<EMPTY>','1000003001','2017-07-31 00:00:00.0','1000024001','2017-08-21 00:00:00.0',null,'3024022',null,null,null,null,null,'<EMPTY>','<EMPTY>','<EMPTY>','<EMPTY>','<EMPTY>',1208,'2019-12-18 12:12:12.0');
Insert into SDH_FIRM
values (400271,'ZIMMERMAN WEALTH MANAGEMENT LLC','Zimmerman Wealt','127526',null,null,706,1211,null,'<EMPTY>',null,null,'<EMPTY>','1000003001','2017-07-31 00:00:00.0','1000024001','2017-08-21 00:00:00.0',null,'503764',null,null,null,null,null,'<EMPTY>','<EMPTY>','<EMPTY>','<EMPTY>','<EMPTY>',1212,'2019-12-18 12:12:12.0');
Insert into SDH_FIRM
values (400275,'ZIMMERMAN ADVISORY GROUP LLC','Zimmerman Advis','148214',null,null,706,1207,null,'<EMPTY>',null,null,'<EMPTY>','1000003001','2017-07-31 00:00:00.0','1000024001','2017-08-21 00:00:00.0',null,'3025159',null,null,null,null,null,'<EMPTY>','<EMPTY>','<EMPTY>','<EMPTY>','<EMPTY>',1208,'2019-12-18 12:12:12.0');