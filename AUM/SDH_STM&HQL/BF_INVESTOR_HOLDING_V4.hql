--Changes for trans id mapping logic - Mar4 --20200305_bkp_day0_except_TAASIA_bf_investor_holding
--Natural Keys valiation - NoNull //should return no record
select  count(*)
from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_bf_investor_holding
where   src = 'JHISDH'
and     src_holding_id is null;

+------+--+
| _c0  |
+------+--+
| 0    |
+------+--+
1 row selected (21.487 seconds)

--Natural Keys valiation - NoDuplicate //should return no record
select  src_holding_id,
        count(*)
from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_bf_investor_holding
where   src = 'JHISDH'
and     current_version = 'Y'
and     distributor_key not in ('9a202039-e5c2-43fc-8c97-f32a2a18ae19','cdc4bcbf-0a88-4592-9278-aa039240b173')
group by src_holding_id
having  count(*) >1;

--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 23.50 s
--------------------------------------------------------------------------------
+-----------------+------+--+
| src_holding_id  | _c1  |
+-----------------+------+--+
+-----------------+------+--+
No rows selected (24.396 seconds)



--Mandatory Fields Validation //should return no record
select  count(*)
from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_bf_investor_holding
where   src = 'JHISDH'
and     (holding_key is null
or      effective_ts is null
or      sys_eff_date_from is null
or      sys_eff_date_to is null
or      src is null
or      src_holding_id is null
or      distributor_key is null
or      balance_type is null
or      orig_currency_code is null
or      edl_created_ts is null
or      current_version is null
);
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 15.42 s
--------------------------------------------------------------------------------
+------+--+
| _c0  |
+------+--+
| 0    |
+------+--+
1 row selected (16.479 seconds)


--Null and hard coded Columns Validation //should return no record
select  count(*)
from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_bf_investor_holding
where   src = 'JHISDH'
and     (security_key is not null
or      src_security_id is not null
or      val_price  is not null
or      unsettled_units  is not null
or      unsettled_value_orig  is not null
or      unsettled_value_cad  is not null
or      unsettled_value_usd  is not null
or      unsettled_value_hkd  is not null
or      src_created_ts  is not null
or      src_created_tz  is not null
or      orig_currency_code <> 'USD'
);
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 11.48 s
--------------------------------------------------------------------------------
+------+--+
| _c0  |
+------+--+
| 0    |
+------+--+
1 row selected (12.42 seconds)

   
--Curation query

    --Record count
with    CurrentCurationDateTime as (
        select  start_time,
                end_time,
                row_number () over (partition by job_name order by start_time desc) as rank
        from    inv_curated_eod_qa.curation_registry
        where   job_name = 'sdh_curation'
), 
dummy_account as ( 
    select  account_key, account_uid
    from    inv_curated_eod_qa.bd_investor_account
    where   account_uid = 'JHISDH_DUMMY'
),
dummy_distributor as ( 
    select  d.distributor_key, r.distributor_uid
    from    inv_curated_eod_qa.bd_distributor d inner join inv_curated_eod_qa.rel_distributor r
    where   trim(d.distributor_uid) = trim(r.distributor_uid)
    and     r.src = 'JHISDH'
    and     d.distributor_key = '11111111-1111-1111-1111-111111111111'
),
product as (
select  distinct prod_id,pl_prd_ty_id
from    inv_jhi_typed_qa.sdh_product
where   cast(pl_prd_ty_id as string) in ('1374','789','954','1163','1408','1391','1359','3988')
),
distributor as (
select  d.distributor_key,
        r.distributor_uid,
        r.src_id,
        d.bus_eff_date_from,
        d.bus_eff_date_to
from    inv_curated_eod_qa.bd_distributor d inner join inv_curated_eod_qa.rel_distributor r 
    on  trim(d.distributor_uid) = trim(r.distributor_uid)
    and r.src = 'JHISDH'
),
source_raw as (
    select  *
    from    (
            select  *,
                    row_number() over (partition by dh_asset_id order by process_timestamp  desc)    as rn
            from    inv_jhi_typed_qa.sdh_asset_position     s
            ) a,
            CurrentCurationDateTime                 c
    where   a.rn = 1
    and     c.rank = 2
    and     a.process_timestamp >c.end_time
)
select  'bf_investor_holding_Source' as tableName, count(*) from source_raw
union all
select  'bf_investor_holding_Target' as tableName, count(*) 
from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_bf_investor_holding  t,
        CurrentCurationDateTime                                 c
where   src = 'JHISDH'
and     c.rank =1
and     t.edl_curated_ts >= c.start_time 
and     t.edl_curated_ts <= c.end_time 
and     t.current_version = 'Y';
--------------------------------------------------------------------------------
VERTICES: 29/29  [==========================>>] 100%  ELAPSED TIME: 32.59 s
--------------------------------------------------------------------------------
+-----------------------------+----------+--+
|        _u1.tablename        | _u1._c1  |
+-----------------------------+----------+--+
| bf_investor_holding_Source  | 16       |
| bf_investor_holding_Target  | 16       |
+-----------------------------+----------+--+
2 rows selected (38.528 seconds)



    --Data validation
with    CurrentCurationDateTime as (
        select  start_time,
                end_time,
                row_number () over (partition by job_name order by start_time desc) as rank
        from    inv_curated_eod_qa.curation_registry
        where   job_name = 'sdh_curation'
), 
dummy_account as ( 
    select  account_key, account_uid
    from    inv_curated_eod_qa.bd_investor_account
    where   account_uid = 'JHISDH_DUMMY'
),
dummy_distributor as ( 
    select  d.distributor_key, r.distributor_uid
    from    inv_curated_eod_qa.bd_distributor d inner join inv_curated_eod_qa.rel_distributor r
    where   trim(d.distributor_uid) = trim(r.distributor_uid)
    and     r.src = 'JHISDH'
    and     d.distributor_key = '11111111-1111-1111-1111-111111111111'
),
product as (
select  distinct prod_id,pl_prd_ty_id
from    inv_jhi_typed_qa.sdh_product
where   cast(pl_prd_ty_id as string) in ('1374','789','954','1163','1408','1391','1359','3988')
),
distributor as (
select  d.distributor_key,
        r.distributor_uid,
        r.src_id,
        d.bus_eff_date_from,
        d.bus_eff_date_to
from    inv_curated_eod_qa.bd_distributor d inner join inv_curated_eod_qa.rel_distributor r 
    on  trim(d.distributor_uid) = trim(r.distributor_uid)
    and r.src = 'JHISDH'
),
source_raw as (
    select  *
    from    (
            select  *,
                    row_number() over (partition by dh_asset_id order by process_timestamp  desc)    as rn
            from    inv_jhi_typed_qa.sdh_asset_position     s
            ) a,
            CurrentCurationDateTime                 c
    where   a.rn = 1
    and     c.rank = 2
    and     a.process_timestamp >c.end_time
),
source_raw_filter as(
    select  eff_dt,
            cast(s.dh_asset_id as string)                   as dh_asset_id,
            ta_acct_nbr,
            nvl(cast(firm_party_id as string), '<NULL>')    as firm_party_id,
            cast(total_share_qty as decimal(32,8))          as total_share_qty,
            cast(aum_amt as decimal(24,2))                  as aum_amt,
            nvl(trim(cast(prod_id as string)), '<NULL>')    as prod_id,
            s.process_timestamp,
            case    when t.dh_asset_id is null then 'N/A'
                    else                            'TRADE_DATE'
            end                                             as balance_type
    from    source_raw s left outer join 
            (
                select  distinct cast(dh_asset_id as string)     as dh_asset_id
                from    source_raw t   inner join product p                           
                        on t.prod_id = p.prod_id
                where   (upper(trim(t.part_trade_ind)) <> 'P' or t.part_trade_ind is null)
                and     upper(trim(t.filter_status_cd)) <> 'NS'
                and     upper(trim(t.stat_cd)) <> 'D'
                and     t.rep_id not in (1445688, 1870105,1379603,1917045,1917059,1917069,1917078,
                                         1917173,1917298,1917299,1917300,1763852,1400601,1899863,1888320,1900083,
                                         1338205,1460849,1301941,1888679,666310,1917876,1917915,666309,1416535,1381732)             
            ) t
    on      cast(s.dh_asset_id as string) = t.dh_asset_id
),--pass
source as (    
            select  to_date(s.eff_dt)                                           as eff_dt,
                    cast(s.eff_dt as timestamp)                                 as eff_ts,
                    dh_asset_id,
                    case when   unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(i.bus_eff_date_from) >=  0 and
                                unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(i.bus_eff_date_to)   <=  0
                        then    nvl(trim(i.account_key), dummya.account_key)
                    else        dummya.account_key
                    end                                                         as  account_key,
                    case when   unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(d.bus_eff_date_from) >=  0 and
                                unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(d.bus_eff_date_to)   <=  0
                                then    nvl(trim(d.distributor_key),'11111111-1111-1111-1111-111111111111')
                    else        '11111111-1111-1111-1111-111111111111'
                    end                                                         as  distributor_key,
                    case when   unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(i.bus_eff_date_from) >=  0 and
                                unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(i.bus_eff_date_to)   <=  0
                        then    nvl(trim(i.account_uid), 'JHISDH_DUMMY')
                    else        'JHISDH_DUMMY'
                    end                                                         as  account_uid,
                    case when   unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(d.bus_eff_date_from) >=  0 and
                                unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(d.bus_eff_date_to)   <=  0
                                then    nvl(trim(d.distributor_uid),dummyd.distributor_uid)
                    else        dummyd.distributor_uid
                    end                                                         as  distributor_uid,
                    s.total_share_qty,
                    s.firm_party_id,
                    s.aum_amt,
                    s.process_timestamp,
                    i.bus_eff_date_from,
                    i.bus_eff_date_to,
                    s.liability_portfolio_key,
                    nvl(trim(p.liability_portf_uid),'<NULL>')                   as  liability_portf_uid,
                    s.balance_type
            from    (
            select  a.*,
                    case when   unix_timestamp(concat(a.eff_dt,' 00:00:00')) - unix_timestamp(l.bus_eff_date_from) >=  0 and
                                unix_timestamp(concat(a.eff_dt,' 00:00:00')) - unix_timestamp(l.bus_eff_date_to)   <=  0
                        then    nvl(trim(l.liability_portfolio_key),'00000000-0000-0000-0000-000000000000')
                    else        '00000000-0000-0000-0000-000000000000'
                    end                                                 as  liability_portfolio_key
            from    source_raw_filter                                           a
            left    outer join inv_curated_eod_qa.rel_liability_port_systems    l
                on  a.prod_id = nvl(trim(l.related_system_code),'<NULL>')
                and l.related_system_name = 'JHISDH')                           s
            left outer join inv_curated_eod_qa.20200315_bkp_SDHDAY1_bd_investor_account              i   --retrieve account_key/account_uid
                on  concat('JHISDH_',nvl(trim(s.ta_acct_nbr),'DUMMY')) = trim(i.account_uid)
                and i.current_version = 'Y'
            left outer join dummy_account                                       dummya
            left outer join (
                    select  d.distributor_key,
                            r.distributor_uid,
                            r.src_id,
                            d.bus_eff_date_from,
                            d.bus_eff_date_to
                    from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_bd_distributor d inner join inv_curated_eod_qa.20200315_bkp_SDHDAY1_rel_distributor r 
                        on  trim(d.distributor_uid) = trim(r.distributor_uid)
                        and r.src = 'JHISDH'
                        and d.distributor_key not in ('9a202039-e5c2-43fc-8c97-f32a2a18ae19','cdc4bcbf-0a88-4592-9278-aa039240b173')
                            )                                                   d
                on  s.firm_party_id = nvl(trim(d.src_id),'<NULL>')
            left outer join dummy_distributor                                   dummyd
            left outer join inv_curated_eod_qa.bd_liability_portfolio           p
                on  trim(s.liability_portfolio_key) = trim(p.liability_portfolio_key)
                and p.current_version = 'Y'
),--13859718
source_flag        as (                    
select  s.eff_ts                    as  src_effective_ts,
        s.eff_dt                    as  src_effective_dt,
        s.dh_asset_id               as  src_src_holding_id,
        s.account_key               as  src_account_key,
        s.distributor_key           as  src_distributor_key,
        s.account_uid               as  src_account_uid,
        s.distributor_uid           as  src_distributor_uid,
        s.total_share_qty           as  src_settled_units,
        s.aum_amt                   as  src_settled_value_orig,
        s.liability_portfolio_key   as  src_liability_portfolio_key,
        s.liability_portf_uid       as  src_liability_portf_uid,
        s.balance_type              as  src_balance_type,
        s.process_timestamp         as  src_process_timestamp,
        
        t.effective_ts              as  tar_effective_ts,
        t.effective_dt              as  tar_effective_dt,
        t.src_holding_id            as  tar_src_holding_id,
        t.account_key               as  tar_account_key,
        t.distributor_key           as  tar_distributor_key,
        t.account_uid               as  tar_account_uid,
        t.distributor_uid           as  tar_distributor_uid,
        t.settled_units             as  tar_settled_units,
        t.settled_value_orig        as  tar_settled_value_orig,
        t.liability_portfolio_key   as  tar_liability_portfolio_key,
        t.liability_portf_uid       as  tar_liability_portf_uid,
        t.balance_type              as  tar_balance_type,
        t.edl_created_ts            as  tar_edl_created_ts,
        t.edl_curated_ts            as  tar_edl_curated_ts,
        t.sys_eff_date_from         as  tar_sys_eff_date_from,
        t.sys_eff_date_to           as  tar_sys_eff_date_to,
        t.current_version           as  tar_current_version,
        
        case    when    t.src_holding_id is null                                            then    'new'  
                when    trim(s.dh_asset_id) is not null and t.src_holding_id is not null    then    'updated'
                else                                                                                'existing'
        end                 as flag     
        
from    source s left outer join 
    (   select *
        from    inv_curated_eod_qa.20200313_bkp_SDHDAY0_bf_investor_holding  --day0 backup table
        where   src = 'JHISDH'
        and     current_version = 'Y') t
on      nvl(trim(s.dh_asset_id),  '<NULL>') = nvl(trim(t.src_holding_id),  '<NULL>') 
where   s.dh_asset_id is not null 
)
select src_balance_type,flag, count(*) from source_flag group by src_balance_type,flag;
--------------------------------------------------------------------------------
VERTICES: 25/25  [==========================>>] 100%  ELAPSED TIME: 26.97 s
--------------------------------------------------------------------------------
+-------------------+----------+------+--+
| src_balance_type  |   flag   | _c2  |
+-------------------+----------+------+--+
| N/A               | new      | 6    |
| TRADE_DATE        | new      | 3    |
| N/A               | updated  | 3    |
| TRADE_DATE        | updated  | 4    |
+-------------------+----------+------+--+
4 rows selected (32.191 seconds)




with    CurrentCurationDateTime as (
        select  start_time,
                end_time,
                row_number () over (partition by job_name order by start_time desc) as rank
        from    inv_curated_eod_qa.curation_registry
        where   job_name = 'sdh_curation'
), 
dummy_account as ( 
    select  account_key, account_uid
    from    inv_curated_eod_qa.bd_investor_account
    where   account_uid = 'JHISDH_DUMMY'
),
dummy_distributor as ( 
    select  d.distributor_key, r.distributor_uid
    from    inv_curated_eod_qa.bd_distributor d inner join inv_curated_eod_qa.rel_distributor r
    where   trim(d.distributor_uid) = trim(r.distributor_uid)
    and     r.src = 'JHISDH'
    and     d.distributor_key = '11111111-1111-1111-1111-111111111111'
),
product as (
select  distinct prod_id,pl_prd_ty_id
from    inv_jhi_typed_qa.sdh_product
where   cast(pl_prd_ty_id as string) in ('1374','789','954','1163','1408','1391','1359','3988')
),
distributor as (
select  d.distributor_key,
        r.distributor_uid,
        r.src_id,
        d.bus_eff_date_from,
        d.bus_eff_date_to
from    inv_curated_eod_qa.bd_distributor d inner join inv_curated_eod_qa.rel_distributor r 
    on  trim(d.distributor_uid) = trim(r.distributor_uid)
    and r.src = 'JHISDH'
),
source_raw as (
    select  *
    from    (
            select  *,
                    row_number() over (partition by dh_asset_id order by process_timestamp  desc)    as rn
            from    inv_jhi_typed_qa.sdh_asset_position     s
            ) a,
            CurrentCurationDateTime                 c
    where   a.rn = 1
    and     c.rank = 2
    and     a.process_timestamp >c.end_time
),
source_raw_filter as(
    select  eff_dt,
            cast(s.dh_asset_id as string)                   as dh_asset_id,
            ta_acct_nbr,
            nvl(cast(firm_party_id as string), '<NULL>')    as firm_party_id,
            cast(total_share_qty as decimal(32,8))          as total_share_qty,
            cast(aum_amt as decimal(24,2))                  as aum_amt,
            nvl(trim(cast(prod_id as string)), '<NULL>')    as prod_id,
            s.process_timestamp,
            case    when t.dh_asset_id is null then 'N/A'
                    else                            'TRADE_DATE'
            end                                             as balance_type
    from    source_raw s left outer join 
            (
                select  distinct cast(dh_asset_id as string)     as dh_asset_id
                from    source_raw t   inner join product p                           
                        on t.prod_id = p.prod_id
                where   (upper(trim(t.part_trade_ind)) <> 'P' or t.part_trade_ind is null)
                and     upper(trim(t.filter_status_cd)) <> 'NS'
                and     upper(trim(t.stat_cd)) <> 'D'
                and     t.rep_id not in (1445688, 1870105,1379603,1917045,1917059,1917069,1917078,
                                         1917173,1917298,1917299,1917300,1763852,1400601,1899863,1888320,1900083,
                                         1338205,1460849,1301941,1888679,666310,1917876,1917915,666309,1416535,1381732)             
            ) t
    on      cast(s.dh_asset_id as string) = t.dh_asset_id
),--pass
source as (    
            select  to_date(s.eff_dt)                                           as eff_dt,
                    cast(s.eff_dt as timestamp)                                 as eff_ts,
                    dh_asset_id,
                    case when   unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(i.bus_eff_date_from) >=  0 and
                                unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(i.bus_eff_date_to)   <=  0
                        then    nvl(trim(i.account_key), dummya.account_key)
                    else        dummya.account_key
                    end                                                         as  account_key,
                    case when   unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(d.bus_eff_date_from) >=  0 and
                                unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(d.bus_eff_date_to)   <=  0
                                then    nvl(trim(d.distributor_key),'11111111-1111-1111-1111-111111111111')
                    else        '11111111-1111-1111-1111-111111111111'
                    end                                                         as  distributor_key,
                    case when   unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(i.bus_eff_date_from) >=  0 and
                                unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(i.bus_eff_date_to)   <=  0
                        then    nvl(trim(i.account_uid), 'JHISDH_DUMMY')
                    else        'JHISDH_DUMMY'
                    end                                                         as  account_uid,
                    case when   unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(d.bus_eff_date_from) >=  0 and
                                unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(d.bus_eff_date_to)   <=  0
                                then    nvl(trim(d.distributor_uid),dummyd.distributor_uid)
                    else        dummyd.distributor_uid
                    end                                                         as  distributor_uid,
                    s.total_share_qty,
                    s.firm_party_id,
                    s.aum_amt,
                    s.process_timestamp,
                    i.bus_eff_date_from,
                    i.bus_eff_date_to,
                    s.liability_portfolio_key,
                    nvl(trim(p.liability_portf_uid),'<NULL>')                   as  liability_portf_uid,
                    s.balance_type
            from    (
            select  a.*,
                    case when   unix_timestamp(concat(a.eff_dt,' 00:00:00')) - unix_timestamp(l.bus_eff_date_from) >=  0 and
                                unix_timestamp(concat(a.eff_dt,' 00:00:00')) - unix_timestamp(l.bus_eff_date_to)   <=  0
                        then    nvl(trim(l.liability_portfolio_key),'00000000-0000-0000-0000-000000000000')
                    else        '00000000-0000-0000-0000-000000000000'
                    end                                                 as  liability_portfolio_key
            from    source_raw_filter                                           a
            left    outer join inv_curated_eod_qa.rel_liability_port_systems    l
                on  a.prod_id = nvl(trim(l.related_system_code),'<NULL>')
                and l.related_system_name = 'JHISDH')                           s
            left outer join inv_curated_eod_qa.20200315_bkp_SDHDAY1_bd_investor_account              i   --retrieve account_key/account_uid
                on  concat('JHISDH_',nvl(trim(s.ta_acct_nbr),'DUMMY')) = trim(i.account_uid)
                and i.current_version = 'Y'
            left outer join dummy_account                                       dummya
            left outer join (
                    select  d.distributor_key,
                            r.distributor_uid,
                            r.src_id,
                            d.bus_eff_date_from,
                            d.bus_eff_date_to
                    from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_bd_distributor d inner join inv_curated_eod_qa.20200315_bkp_SDHDAY1_rel_distributor r 
                        on  trim(d.distributor_uid) = trim(r.distributor_uid)
                        and r.src = 'JHISDH'
                        and d.distributor_key not in ('9a202039-e5c2-43fc-8c97-f32a2a18ae19','cdc4bcbf-0a88-4592-9278-aa039240b173')
                            )                                                   d
                on  s.firm_party_id = nvl(trim(d.src_id),'<NULL>')
            left outer join dummy_distributor                                   dummyd
            left outer join inv_curated_eod_qa.bd_liability_portfolio           p
                on  trim(s.liability_portfolio_key) = trim(p.liability_portfolio_key)
                and p.current_version = 'Y'
),--13859718
source_flag        as (                    
select  s.eff_ts                    as  src_effective_ts,
        s.eff_dt                    as  src_effective_dt,
        s.dh_asset_id               as  src_src_holding_id,
        s.account_key               as  src_account_key,
        s.distributor_key           as  src_distributor_key,
        s.account_uid               as  src_account_uid,
        s.distributor_uid           as  src_distributor_uid,
        s.total_share_qty           as  src_settled_units,
        s.aum_amt                   as  src_settled_value_orig,
        s.liability_portfolio_key   as  src_liability_portfolio_key,
        s.liability_portf_uid       as  src_liability_portf_uid,
        s.balance_type              as  src_balance_type,
        s.process_timestamp         as  src_process_timestamp,
        
        t.effective_ts              as  tar_effective_ts,
        t.effective_dt              as  tar_effective_dt,
        t.src_holding_id            as  tar_src_holding_id,
        t.account_key               as  tar_account_key,
        t.distributor_key           as  tar_distributor_key,
        t.account_uid               as  tar_account_uid,
        t.distributor_uid           as  tar_distributor_uid,
        t.settled_units             as  tar_settled_units,
        t.settled_value_orig        as  tar_settled_value_orig,
        t.liability_portfolio_key   as  tar_liability_portfolio_key,
        t.liability_portf_uid       as  tar_liability_portf_uid,
        t.balance_type              as  tar_balance_type,
        t.edl_created_ts            as  tar_edl_created_ts,
        t.edl_curated_ts            as  tar_edl_curated_ts,
        t.sys_eff_date_from         as  tar_sys_eff_date_from,
        t.sys_eff_date_to           as  tar_sys_eff_date_to,
        t.current_version           as  tar_current_version,
        
        case    when    t.src_holding_id is null                                            then    'new'  
                when    trim(s.dh_asset_id) is not null and t.src_holding_id is not null    then    'updated'
                else                                                                                'existing'
        end                 as flag     
        
from    source s left outer join 
    (   select *
        from    inv_curated_eod_qa.20200313_bkp_SDHDAY0_bf_investor_holding  --day0 backup table
        where   src = 'JHISDH'
        and     current_version = 'Y') t
on      nvl(trim(s.dh_asset_id),  '<NULL>') = nvl(trim(t.src_holding_id),  '<NULL>') 
where   s.dh_asset_id is not null 
),
source_master    as (

-------------new records-------------
select  src_effective_ts            as  effective_ts,
        src_effective_dt            as  effective_dt,
        src_src_holding_id          as  src_holding_id,
        src_account_key             as  account_key,
        src_distributor_key         as  distributor_key,
        src_account_uid             as  account_uid,
        src_distributor_uid         as  distributor_uid,
        src_settled_units           as  settled_units,
        src_settled_value_orig      as  settled_value_orig,
        src_liability_portfolio_key as  liability_portfolio_key,
        src_liability_portf_uid     as  liability_portf_uid,
        src_balance_type            as  balance_type,
        cast(src_process_timestamp as timestamp)       as edl_created_ts,  
        src_process_timestamp       as  edl_curated_ts, --should be curation date
        cast('1980-01-01 00:00:00.0' as timestamp)     as     sys_eff_date_from,
        cast('2099-12-31 00:00:00.0' as timestamp)     as     sys_eff_date_to,
        'Y'                          as current_version,
        flag
from    source_flag
where   flag = 'new'

union all

-------------Existing records / insert new records-------------
select  src_effective_ts            as  effective_ts,
        src_effective_dt            as  effective_dt,
        src_src_holding_id          as  src_holding_id,
        src_account_key             as  account_key,
        src_distributor_key         as  distributor_key,
        src_account_uid             as  account_uid,
        src_distributor_uid         as  distributor_uid,
        src_settled_units           as  settled_units,
        src_settled_value_orig      as  settled_value_orig,
        src_liability_portfolio_key as  liability_portfolio_key,
        src_liability_portf_uid     as  liability_portf_uid,
        src_balance_type            as  balance_type,
        cast(src_process_timestamp as timestamp)       as  edl_created_ts,  
        src_process_timestamp       as  edl_curated_ts, --should be curation date 
        cast(concat(to_date(src_process_timestamp),' 00:00:00') as timestamp)   as  sys_eff_date_from,
        cast('2099-12-31 00:00:00.0' as timestamp)                              as  sys_eff_date_to,
        'Y'                         as    current_version,
        flag    
from    source_flag
where   flag = 'updated' 

union all
-------------updated records / Expired records------------- 
select  tar_effective_ts            as  effective_ts,
        to_date(tar_effective_dt)   as  effective_dt,
        tar_src_holding_id          as  src_holding_id,
        tar_account_key             as  account_key,
        tar_distributor_key         as  distributor_key,
        tar_account_uid             as  account_uid,
        tar_distributor_uid         as  distributor_uid,
        tar_settled_units           as  settled_units,
        tar_settled_value_orig      as  settled_value_orig,
        tar_liability_portfolio_key as  liability_portfolio_key,
        tar_liability_portf_uid     as  liability_portf_uid,
        tar_balance_type            as  balance_type,
        cast(tar_edl_created_ts as timestamp)                                 as edl_created_ts,
        tar_edl_curated_ts          as  edl_curated_ts,
        tar_sys_eff_date_from       as  sys_eff_date_from,
        cast(concat(to_date(src_process_timestamp),' 00:00:00') as timestamp) as sys_eff_date_to,
        'N'                         as  current_version,
        flag    
from    source_flag
where   flag = 'updated' 

union all
-------------existing records-------------    
select  tar_effective_ts            as  effective_ts,
        to_date(tar_effective_dt)   as  effective_dt,
        tar_src_holding_id          as  src_holding_id,
        tar_account_key             as  account_key,
        tar_distributor_key         as  distributor_key,
        tar_account_uid             as  account_uid,
        tar_distributor_uid         as  distributor_uid,
        tar_settled_units           as  settled_units,
        tar_settled_value_orig      as  settled_value_orig,
        tar_liability_portfolio_key as  liability_portfolio_key,
        tar_liability_portf_uid     as  liability_portf_uid,
        tar_balance_type            as  balance_type,
        cast(tar_edl_created_ts as timestamp)                                 as edl_created_ts,
        tar_edl_curated_ts          as  edl_curated_ts,
        tar_sys_eff_date_from       as  sys_eff_date_from,
        tar_sys_eff_date_to         as  sys_eff_date_to,
        tar_current_version         as  current_version,
        flag    
from    source_flag
where   flag = 'existing'
)
select balance_type,flag,current_version, count(*) from source_master group by balance_type,flag,current_version order by flag, balance_type;
--------------------------------------------------------------------------------
VERTICES: 98/98  [==========================>>] 100%  ELAPSED TIME: 55.40 s
--------------------------------------------------------------------------------
+---------------+----------+------------------+------+--+
| balance_type  |   flag   | current_version  | _c3  |
+---------------+----------+------------------+------+--+
| N/A           | new      | Y                | 6    |
| TRADE_DATE    | new      | Y                | 3    |
| N/A           | updated  | N                | 1    |
| N/A           | updated  | Y                | 3    |
| TRADE_DATE    | updated  | N                | 6    |
| TRADE_DATE    | updated  | Y                | 4    |
+---------------+----------+------------------+------+--+
6 rows selected (66.628 seconds)

----Data validation
with    CurrentCurationDateTime as (
        select  start_time,
                end_time,
                row_number () over (partition by job_name order by start_time desc) as rank
        from    inv_curated_eod_qa.curation_registry
        where   job_name = 'sdh_curation'
), 
dummy_account as ( 
    select  account_key, account_uid
    from    inv_curated_eod_qa.bd_investor_account
    where   account_uid = 'JHISDH_DUMMY'
),
dummy_distributor as ( 
    select  d.distributor_key, r.distributor_uid
    from    inv_curated_eod_qa.bd_distributor d inner join inv_curated_eod_qa.rel_distributor r
    where   trim(d.distributor_uid) = trim(r.distributor_uid)
    and     r.src = 'JHISDH'
    and     d.distributor_key = '11111111-1111-1111-1111-111111111111'
),
product as (
select  distinct prod_id,pl_prd_ty_id
from    inv_jhi_typed_qa.sdh_product
where   cast(pl_prd_ty_id as string) in ('1374','789','954','1163','1408','1391','1359','3988')
),
distributor as (
select  d.distributor_key,
        r.distributor_uid,
        r.src_id,
        d.bus_eff_date_from,
        d.bus_eff_date_to
from    inv_curated_eod_qa.bd_distributor d inner join inv_curated_eod_qa.rel_distributor r 
    on  trim(d.distributor_uid) = trim(r.distributor_uid)
    and r.src = 'JHISDH'
),
source_raw as (
    select  *
    from    (
            select  *,
                    row_number() over (partition by dh_asset_id order by process_timestamp  desc)    as rn
            from    inv_jhi_typed_qa.sdh_asset_position     s
            ) a,
            CurrentCurationDateTime                 c
    where   a.rn = 1
    and     c.rank = 2
    and     a.process_timestamp >c.end_time
),
source_raw_filter as(
    select  eff_dt,
            cast(s.dh_asset_id as string)                   as dh_asset_id,
            ta_acct_nbr,
            nvl(cast(firm_party_id as string), '<NULL>')    as firm_party_id,
            cast(total_share_qty as decimal(32,8))          as total_share_qty,
            cast(aum_amt as decimal(24,2))                  as aum_amt,
            nvl(trim(cast(prod_id as string)), '<NULL>')    as prod_id,
            s.process_timestamp,
            case    when t.dh_asset_id is null then 'N/A'
                    else                            'TRADE_DATE'
            end                                             as balance_type
    from    source_raw s left outer join 
            (
                select  distinct cast(dh_asset_id as string)     as dh_asset_id
                from    source_raw t   inner join product p                           
                        on t.prod_id = p.prod_id
                where   (upper(trim(t.part_trade_ind)) <> 'P' or t.part_trade_ind is null)
                and     upper(trim(t.filter_status_cd)) <> 'NS'
                and     upper(trim(t.stat_cd)) <> 'D'
                and     t.rep_id not in (1445688, 1870105,1379603,1917045,1917059,1917069,1917078,
                                         1917173,1917298,1917299,1917300,1763852,1400601,1899863,1888320,1900083,
                                         1338205,1460849,1301941,1888679,666310,1917876,1917915,666309,1416535,1381732)             
            ) t
    on      cast(s.dh_asset_id as string) = t.dh_asset_id
),--pass
source as (    
            select  to_date(s.eff_dt)                                           as eff_dt,
                    cast(s.eff_dt as timestamp)                                 as eff_ts,
                    dh_asset_id,
                    case when   unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(i.bus_eff_date_from) >=  0 and
                                unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(i.bus_eff_date_to)   <=  0
                        then    nvl(trim(i.account_key), dummya.account_key)
                    else        dummya.account_key
                    end                                                         as  account_key,
                    case when   unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(d.bus_eff_date_from) >=  0 and
                                unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(d.bus_eff_date_to)   <=  0
                                then    nvl(trim(d.distributor_key),'11111111-1111-1111-1111-111111111111')
                    else        '11111111-1111-1111-1111-111111111111'
                    end                                                         as  distributor_key,
                    case when   unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(i.bus_eff_date_from) >=  0 and
                                unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(i.bus_eff_date_to)   <=  0
                        then    nvl(trim(i.account_uid), 'JHISDH_DUMMY')
                    else        'JHISDH_DUMMY'
                    end                                                         as  account_uid,
                    case when   unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(d.bus_eff_date_from) >=  0 and
                                unix_timestamp(concat(s.eff_dt,' 00:00:00')) - unix_timestamp(d.bus_eff_date_to)   <=  0
                                then    nvl(trim(d.distributor_uid),dummyd.distributor_uid)
                    else        dummyd.distributor_uid
                    end                                                         as  distributor_uid,
                    s.total_share_qty,
                    s.firm_party_id,
                    s.aum_amt,
                    s.process_timestamp,
                    i.bus_eff_date_from,
                    i.bus_eff_date_to,
                    s.liability_portfolio_key,
                    nvl(trim(p.liability_portf_uid),'<NULL>')                   as  liability_portf_uid,
                    s.balance_type
            from    (
            select  a.*,
                    case when   unix_timestamp(concat(a.eff_dt,' 00:00:00')) - unix_timestamp(l.bus_eff_date_from) >=  0 and
                                unix_timestamp(concat(a.eff_dt,' 00:00:00')) - unix_timestamp(l.bus_eff_date_to)   <=  0
                        then    nvl(trim(l.liability_portfolio_key),'00000000-0000-0000-0000-000000000000')
                    else        '00000000-0000-0000-0000-000000000000'
                    end                                                 as  liability_portfolio_key
            from    source_raw_filter                                           a
            left    outer join inv_curated_eod_qa.rel_liability_port_systems    l
                on  a.prod_id = nvl(trim(l.related_system_code),'<NULL>')
                and l.related_system_name = 'JHISDH')                           s
            left outer join inv_curated_eod_qa.20200315_bkp_SDHDAY1_bd_investor_account              i   --retrieve account_key/account_uid
                on  concat('JHISDH_',nvl(trim(s.ta_acct_nbr),'DUMMY')) = trim(i.account_uid)
                and i.current_version = 'Y'
            left outer join dummy_account                                       dummya
            left outer join (
                    select  d.distributor_key,
                            r.distributor_uid,
                            r.src_id,
                            d.bus_eff_date_from,
                            d.bus_eff_date_to
                    from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_bd_distributor d inner join inv_curated_eod_qa.20200315_bkp_SDHDAY1_rel_distributor r 
                        on  trim(d.distributor_uid) = trim(r.distributor_uid)
                        and r.src = 'JHISDH'
                        and d.distributor_key not in ('9a202039-e5c2-43fc-8c97-f32a2a18ae19','cdc4bcbf-0a88-4592-9278-aa039240b173')
                            )                                                   d
                on  s.firm_party_id = nvl(trim(d.src_id),'<NULL>')
            left outer join dummy_distributor                                   dummyd
            left outer join inv_curated_eod_qa.bd_liability_portfolio           p
                on  trim(s.liability_portfolio_key) = trim(p.liability_portfolio_key)
                and p.current_version = 'Y'
),--13859718
source_flag        as (                    
select  s.eff_ts                    as  src_effective_ts,
        s.eff_dt                    as  src_effective_dt,
        s.dh_asset_id               as  src_src_holding_id,
        s.account_key               as  src_account_key,
        s.distributor_key           as  src_distributor_key,
        s.account_uid               as  src_account_uid,
        s.distributor_uid           as  src_distributor_uid,
        s.total_share_qty           as  src_settled_units,
        s.aum_amt                   as  src_settled_value_orig,
        s.liability_portfolio_key   as  src_liability_portfolio_key,
        s.liability_portf_uid       as  src_liability_portf_uid,
        s.balance_type              as  src_balance_type,
        s.process_timestamp         as  src_process_timestamp,
        
        t.effective_ts              as  tar_effective_ts,
        t.effective_dt              as  tar_effective_dt,
        t.src_holding_id            as  tar_src_holding_id,
        t.account_key               as  tar_account_key,
        t.distributor_key           as  tar_distributor_key,
        t.account_uid               as  tar_account_uid,
        t.distributor_uid           as  tar_distributor_uid,
        t.settled_units             as  tar_settled_units,
        t.settled_value_orig        as  tar_settled_value_orig,
        t.liability_portfolio_key   as  tar_liability_portfolio_key,
        t.liability_portf_uid       as  tar_liability_portf_uid,
        t.balance_type              as  tar_balance_type,
        t.edl_created_ts            as  tar_edl_created_ts,
        t.edl_curated_ts            as  tar_edl_curated_ts,
        t.sys_eff_date_from         as  tar_sys_eff_date_from,
        t.sys_eff_date_to           as  tar_sys_eff_date_to,
        t.current_version           as  tar_current_version,
        
        case    when    t.src_holding_id is null                                            then    'new'  
                when    trim(s.dh_asset_id) is not null and t.src_holding_id is not null    then    'updated'
                else                                                                                'existing'
        end                 as flag     
        
from    source s left outer join 
    (   select *
        from    inv_curated_eod_qa.20200313_bkp_SDHDAY0_bf_investor_holding  --day0 backup table
        where   src = 'JHISDH'
        and     current_version = 'Y') t
on      nvl(trim(s.dh_asset_id),  '<NULL>') = nvl(trim(t.src_holding_id),  '<NULL>') 
where   s.dh_asset_id is not null 
),
source_master    as (

-------------new records-------------
select  src_effective_ts            as  effective_ts,
        src_effective_dt            as  effective_dt,
        src_src_holding_id          as  src_holding_id,
        src_account_key             as  account_key,
        src_distributor_key         as  distributor_key,
        src_account_uid             as  account_uid,
        src_distributor_uid         as  distributor_uid,
        src_settled_units           as  settled_units,
        src_settled_value_orig      as  settled_value_orig,
        src_liability_portfolio_key as  liability_portfolio_key,
        src_liability_portf_uid     as  liability_portf_uid,
        src_balance_type            as  balance_type,
        cast(src_process_timestamp as timestamp)       as edl_created_ts,  
        src_process_timestamp       as  edl_curated_ts, --should be curation date
        cast('1980-01-01 00:00:00.0' as timestamp)     as     sys_eff_date_from,
        cast('2099-12-31 00:00:00.0' as timestamp)     as     sys_eff_date_to,
        'Y'                          as current_version,
        flag
from    source_flag
where   flag = 'new'

union all

-------------Existing records / insert new records-------------
select  src_effective_ts            as  effective_ts,
        src_effective_dt            as  effective_dt,
        src_src_holding_id          as  src_holding_id,
        src_account_key             as  account_key,
        src_distributor_key         as  distributor_key,
        src_account_uid             as  account_uid,
        src_distributor_uid         as  distributor_uid,
        src_settled_units           as  settled_units,
        src_settled_value_orig      as  settled_value_orig,
        src_liability_portfolio_key as  liability_portfolio_key,
        src_liability_portf_uid     as  liability_portf_uid,
        src_balance_type            as  balance_type,
        cast(src_process_timestamp as timestamp)       as  edl_created_ts,  
        src_process_timestamp       as  edl_curated_ts, --should be curation date 
        cast(concat(to_date(src_process_timestamp),' 00:00:00') as timestamp)   as  sys_eff_date_from,
        cast('2099-12-31 00:00:00.0' as timestamp)                              as  sys_eff_date_to,
        'Y'                         as    current_version,
        flag    
from    source_flag
where   flag = 'updated' 

union all
-------------updated records / Expired records------------- 
select  tar_effective_ts            as  effective_ts,
        to_date(tar_effective_dt)   as  effective_dt,
        tar_src_holding_id          as  src_holding_id,
        tar_account_key             as  account_key,
        tar_distributor_key         as  distributor_key,
        tar_account_uid             as  account_uid,
        tar_distributor_uid         as  distributor_uid,
        tar_settled_units           as  settled_units,
        tar_settled_value_orig      as  settled_value_orig,
        tar_liability_portfolio_key as  liability_portfolio_key,
        tar_liability_portf_uid     as  liability_portf_uid,
        tar_balance_type            as  balance_type,
        cast(tar_edl_created_ts as timestamp)                                 as edl_created_ts,
        tar_edl_curated_ts          as  edl_curated_ts,
        tar_sys_eff_date_from       as  sys_eff_date_from,
        cast(concat(to_date(src_process_timestamp),' 00:00:00') as timestamp) as sys_eff_date_to,
        'N'                         as  current_version,
        flag    
from    source_flag
where   flag = 'updated' 

union all
-------------existing records-------------    
select  tar_effective_ts            as  effective_ts,
        to_date(tar_effective_dt)   as  effective_dt,
        tar_src_holding_id          as  src_holding_id,
        tar_account_key             as  account_key,
        tar_distributor_key         as  distributor_key,
        tar_account_uid             as  account_uid,
        tar_distributor_uid         as  distributor_uid,
        tar_settled_units           as  settled_units,
        tar_settled_value_orig      as  settled_value_orig,
        tar_liability_portfolio_key as  liability_portfolio_key,
        tar_liability_portf_uid     as  liability_portf_uid,
        tar_balance_type            as  balance_type,
        cast(tar_edl_created_ts as timestamp)                                 as edl_created_ts,
        tar_edl_curated_ts          as  edl_curated_ts,
        tar_sys_eff_date_from       as  sys_eff_date_from,
        tar_sys_eff_date_to         as  sys_eff_date_to,
        tar_current_version         as  current_version,
        flag    
from    source_flag
where   flag = 'existing'
)
select  count(*)
from    source_master   source
where   source.current_version = 'N' --expired records, 7 is expected
and     exists(
        select  *
        from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_bf_investor_holding  target,
                CurrentCurationDateTime                 c
        where   target.src = 'JHISDH'
        and     target.current_version = 'N'
        and     nvl(trim(source.src_holding_id),  '<NULL>') = nvl(trim(target.src_holding_id),  '<NULL>')
        and     source.effective_ts = target.effective_ts
        and     to_date(source.effective_dt) = to_date(target.effective_dt)
        and     cast(source.sys_eff_date_from as timestamp) = cast(target.sys_eff_date_from as timestamp)
        and     cast(source.sys_eff_date_to as timestamp) = cast(target.sys_eff_date_to as timestamp)
        and     nvl(trim(source.account_key),'<NULL>') = nvl(trim(target.account_key),'<NULL>')
        and     nvl(trim(source.distributor_key),'<NULL>') = nvl(trim(target.distributor_key),'<NULL>')
        and     nvl(trim(source.account_uid),'<NULL>') = nvl(trim(target.account_uid),'<NULL>')
        and     nvl(trim(source.distributor_uid),'<NULL>') = nvl(trim(target.distributor_uid),'<NULL>')
        and     nvl(source.settled_units, 999999.99) = nvl(target.settled_units,999999.99)
        and     nvl(source.settled_value_orig, 999999.99) = nvl(target.settled_value_orig,999999.99)
        and     source.balance_type = target.balance_type
        and     target.settled_value_cad is null
        and     target.settled_value_usd is null
        and     target.settled_value_hkd is null
        and     trim(source.current_version) = trim(target.current_version)
        and     cast(source.edl_created_ts as timestamp) = cast(target.edl_created_ts as timestamp)
        and     cast(source.edl_curated_ts as timestamp) = cast(target.edl_curated_ts as timestamp)
        and     nvl(trim(source.liability_portfolio_key),'<NULL>') = nvl(trim(target.liability_portfolio_key),'<NULL>')
        and     nvl(trim(source.liability_portf_uid),'<NULL>') = nvl(trim(target.liability_portf_uid),'<NULL>'));
--------------------------------------------------------------------------------
VERTICES: 100/100  [==========================>>] 100%  ELAPSED TIME: 71.32 s
--------------------------------------------------------------------------------
+------+--+
| _c0  |
+------+--+
| 7    |
+------+--+
1 row selected (91.321 seconds)
       
        
select  count(*)
from    source_master   source  
where   source.current_version = 'Y' --new/updated records, 16 is expected
and     exists(
        select  *
        from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_bf_investor_holding  target,
                CurrentCurationDateTime                 c
        where   target.src = 'JHISDH'
        and     c.rank = 1
        and     target.edl_curated_ts >= c.start_time
        and     target.edl_curated_ts <= c.end_time
        and     nvl(trim(source.src_holding_id),  '<NULL>') = nvl(trim(target.src_holding_id),  '<NULL>')
        and     source.effective_ts = target.effective_ts
        and     to_date(source.effective_dt) = to_date(target.effective_dt)
        and     cast(source.sys_eff_date_from as timestamp) = cast(target.sys_eff_date_from as timestamp)
        and     cast(source.sys_eff_date_to as timestamp) = cast(target.sys_eff_date_to as timestamp)
        and     nvl(trim(source.account_key),'<NULL>') = nvl(trim(target.account_key),'<NULL>')--fail
        and     nvl(trim(source.distributor_key),'<NULL>') = nvl(trim(target.distributor_key),'<NULL>')
        and     nvl(trim(source.account_uid),'<NULL>') = nvl(trim(target.account_uid),'<NULL>')--fail
        and     nvl(trim(source.distributor_uid),'<NULL>') = nvl(trim(target.distributor_uid),'<NULL>')
        and     nvl(source.settled_units, 999999.99) = nvl(target.settled_units,999999.99)
        and     nvl(source.settled_value_orig, 999999.99) = nvl(target.settled_value_orig,999999.99)
        and     source.balance_type = target.balance_type
        and     target.settled_value_cad is null
        and     target.settled_value_usd is null
        and     target.settled_value_hkd is null
        and     trim(source.current_version) = trim(target.current_version)
        and     cast(source.edl_created_ts as timestamp) = cast(target.edl_created_ts as timestamp)
        and     nvl(trim(source.liability_portfolio_key),'<NULL>') = nvl(trim(target.liability_portfolio_key),'<NULL>')
        and     nvl(trim(source.liability_portf_uid),'<NULL>') = nvl(trim(target.liability_portf_uid),'<NULL>'));
--------------------------------------------------------------------------------
VERTICES: 100/100  [==========================>>] 100%  ELAPSED TIME: 92.93 s
--------------------------------------------------------------------------------
+------+--+
| _c0  |
+------+--+
| 16   |
+------+--+
1 row selected (113.722 seconds)  


--Data mockup -- Mar 11 - Trans_id changes day0
select max(dh_asset_id), min(dh_asset_id)
from    inv_jhi_typed_qa.sdh_asset_position;
+-----------+------+--+
|    _c0    | _c1  |
+-----------+------+--+
| 31017191  | 1    |
+-----------+------+--+

select max(prod_id), min(prod_id)
from    inv_jhi_typed_qa.sdh_asset_position;
desc inv_jhi_typed_qa.sdh_asset_position;
select distinct part_trade_ind from inv_jhi_typed_qa.sdh_asset_position;
--insert below day0 sourece records-------------
Insert into EXPORT_TABLE ("sdh_asset_position.dh_asset_id","sdh_asset_position.asset_id","sdh_asset_position.firm_party_id","sdh_asset_position.trn_als_id","sdh_asset_position.party_rlnshp_id","sdh_asset_position.ofc_als_id","sdh_asset_position.clr_firm_als_id","sdh_asset_position.soc_cd_ali_id","sdh_asset_position.prod_id","sdh_asset_position.pl_soc_cd_id","sdh_asset_position.pl_ty_soc_cd_grp_id","sdh_asset_position.cntry_id","sdh_asset_position.pl_stat_cd_id","sdh_asset_position.firm_id","sdh_asset_position.rep_id","sdh_asset_position.ofc_id","sdh_asset_position.ta_prod_cd","sdh_asset_position.aum_amt_sign","sdh_asset_position.aum_amt","sdh_asset_position.avg_assets","sdh_asset_position.resolve_cd","sdh_asset_position.eff_dt","sdh_asset_position.ta_acct_nbr","sdh_asset_position.account_id","sdh_asset_position.investor_nm","sdh_asset_position.omnibus_id","sdh_asset_position.cusip","sdh_asset_position.loi_nbr","sdh_asset_position.total_share_qty","sdh_asset_position.unissued_share_qty","sdh_asset_position.issued_share_qty","sdh_asset_position.accrued_div_amt","sdh_asset_position.escrow_share_qty","sdh_asset_position.ta_acct_nbr_cd","sdh_asset_position.ext_acct_nbr","sdh_asset_position.ext_acct_nbr_cd","sdh_asset_position.platform_nm","sdh_asset_position.tpa_nbr","sdh_asset_position.employee_cd","sdh_asset_position.ext_plan_id","sdh_asset_position.cumulative_disc_nbr","sdh_asset_position.ta_dlr_cntrl_level","sdh_asset_position.dlr_cntrl_level","sdh_asset_position.ta_nav_indicator","sdh_asset_position.nav_indicator","sdh_asset_position.ta_tin_cd","sdh_asset_position.tin_cd","sdh_asset_position.ssn","sdh_asset_position.ppa_controlled_flg","sdh_asset_position.avg_share_qty","sdh_asset_position.frequency_eff_dt","sdh_asset_position.assets_sim","sdh_asset_position.avg_assets_sim","sdh_asset_position.aum_amt_other","sdh_asset_position.avg_assets_other","sdh_asset_position.addr1","sdh_asset_position.addr2","sdh_asset_position.addr3","sdh_asset_position.addr4","sdh_asset_position.city","sdh_asset_position.state_id","sdh_asset_position.zip","sdh_asset_position.team_rep","sdh_asset_position.team_aum_id","sdh_asset_position.univ_fund_cd","sdh_asset_position.beg_asset_bal","sdh_asset_position.multi_prod_flg","sdh_asset_position.multi_prod_aum_id","sdh_asset_position.ta_managed_acct_fee_cd","sdh_asset_position.managed_acct_fee_cd","sdh_asset_position.ta_sleeve_prod_cd","sdh_asset_position.sleeve_prod_cd","sdh_asset_position.primary_source","sdh_asset_position.currency_cd","sdh_asset_position.phone_nbr","sdh_asset_position.trust_flg","sdh_asset_position.trust_comp_nbr","sdh_asset_position.party_ali_id","sdh_asset_position.fund_nbr","sdh_asset_position.vendor_source_cd","sdh_asset_position.transfer_agent","sdh_asset_position.asset_position_action_cd","sdh_asset_position.asset_position_activity_cd","sdh_asset_position.systematic_reslutn_stat_cd","sdh_asset_position.client_error_reason_cd","sdh_asset_position.ud_error_reason_cd","sdh_asset_position.time_based_ind","sdh_asset_position.stat_cd","sdh_asset_position.ta_firm_id","sdh_asset_position.ta_off_id","sdh_asset_position.ta_rep_id","sdh_asset_position.resolved_to_firm_trading_id","sdh_asset_position.resolved_to_office_trading_id","sdh_asset_position.resolved_to_rep_trading_id","sdh_asset_position.primary_firm_trading_id","sdh_asset_position.primary_office_trading_id","sdh_asset_position.primary_rep_trading_id","sdh_asset_position.source_firm_id","sdh_asset_position.source_office_id","sdh_asset_position.source_rep_id","sdh_asset_position.nscc_tpa_firm_internal_firm_id","sdh_asset_position.nscc_trust_nbr","sdh_asset_position.nscc_trust_internal_firm_id","sdh_asset_position.third_party_admin_id","sdh_asset_position.third_party_admin_internal_firm_id","sdh_asset_position.external_trust_id","sdh_asset_position.external_trust_internal_firm_id","sdh_asset_position.ufs_symbol","sdh_asset_position.ufs_symbol_internal_firm_id","sdh_asset_position.last_nav","sdh_asset_position.acct_type_cd","sdh_asset_position.social_cd_55","sdh_asset_position.vip_cd","sdh_asset_position.blue_sky_state_cd","sdh_asset_position.state_of_residence_cd","sdh_asset_position.cdsc_liable_shares_flag","sdh_asset_position.zip_cd_plus_4","sdh_asset_position.ssn_or_tax_id","sdh_asset_position.roa_group_nbr","sdh_asset_position.tpa_id_nbr","sdh_asset_position.bank_or_trust_id_nbr","sdh_asset_position.nscc_dealer_branch_id","sdh_asset_position.wrap_program_allocation_table","sdh_asset_position.additional_affiliate_id_nbr","sdh_asset_position.rep_last_nm","sdh_asset_position.ta2000_fund_nbr","sdh_asset_position.cusip_nbr","sdh_asset_position.nav_cd_for_wrap_acct","sdh_asset_position.wrap_months_to_age","sdh_asset_position.addr_info_provided_on_transmission","sdh_asset_position.acct_loi_info_provided_on_transmission","sdh_asset_position.acct_usr_defined_info_provided_on_transmission","sdh_asset_position.acct_bene_info_provided_on_transmission","sdh_asset_position.line_nbr_first_addr_line_56","sdh_asset_position.first_line_of_the_acct_reg","sdh_asset_position.second_line_of_the_acct_reg","sdh_asset_position.third_line_of_the_acct_reg","sdh_asset_position.line_nbr_first_addr_line_57","sdh_asset_position.fourth_line_of_the_acct_reg","sdh_asset_position.fifth_line_of_the_acct_reg","sdh_asset_position.city_of_the_acct_reg","sdh_asset_position.province_of_the_acct_reg","sdh_asset_position.country_nm_of_the_acct_reg","sdh_asset_position.loi_amt","sdh_asset_position.loi_start_dt","sdh_asset_position.loi_expire_dt","sdh_asset_position.loi_amt_invested_to_dt","sdh_asset_position.loi_fulfilled_dt","sdh_asset_position.loi_credit_amt_allowed","sdh_asset_position.advisor_1_id","sdh_asset_position.advisor_2_id","sdh_asset_position.advisor_3_id","sdh_asset_position.advisor_4_id","sdh_asset_position.advisor_5_id","sdh_asset_position.advisor_6_id","sdh_asset_position.native_branch_trading_id_from_source_dsp_feed","sdh_asset_position.native_rep_trading_id_from_the_source_dsp_feed","sdh_asset_position.cip_government_type_id","sdh_asset_position.cip_government_id_nbr","sdh_asset_position.prim_bene_tax_id_nbr","sdh_asset_position.prim_bene_nm","sdh_asset_position.prim_bene_dt_of_birth","sdh_asset_position.prim_bene_relationship","sdh_asset_position.prim_bene_us_non_citizen_flag","sdh_asset_position.prim_bene_gender","sdh_asset_position.sec_bene_tax_id_nbr","sdh_asset_position.sec_bene_nm","sdh_asset_position.sec_bene_dt_of_birth","sdh_asset_position.sec_bene_relationship","sdh_asset_position.sec_bene_us_non_citizen_flag","sdh_asset_position.sec_bene_gender","sdh_asset_position.plan_name","sdh_asset_position.plan_nbr","sdh_asset_position.plan_city_name","sdh_asset_position.plan_state_cd","sdh_asset_position.cusip_part_count","sdh_asset_position.tier_text_value","sdh_asset_position.label_id1","sdh_asset_position.label_id2","sdh_asset_position.label_id3","sdh_asset_position.label_id4","sdh_asset_position.label_id5","sdh_asset_position.resolved_rep_nm","sdh_asset_position.prev_soc_cd","sdh_asset_position.asset_change_type_cd","sdh_asset_position.filter_status_cd","sdh_asset_position.firm_advisory_platform_ind","sdh_asset_position.asset_create_dt","sdh_asset_position.nscc_tpa_firm_nbr","sdh_asset_position.ins_btch_id","sdh_asset_position.ins_ts","sdh_asset_position.upd_btch_id","sdh_asset_position.upd_ts","sdh_asset_position.dsp_type_cd","sdh_asset_position.dsp_vip_cd","sdh_asset_position.firm_party_ali_id","sdh_asset_position.ta_soc_cd","sdh_asset_position.part_trade_ind","sdh_asset_position.part_trade_grp_id","sdh_asset_position.new_asset_rec_ind","sdh_asset_position.dspmat","sdh_asset_position.process_timestamp","sdh_asset_position.process_date") values (64,'250123',403074,null,null,290144,null,null,280,null,null,null,null,403074,609480,511693,null,null,0,null,'S',to_timestamp('2017-07-31 00:00:00.0','null'),'9D5 57721 4269',null,'ASHWIN BRANDECKER',null,'47804M878',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,2488847,'3953','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0D2','0D2 GM','50040','0D2','0D2 GM','50040','0D2','0D2 GM','','','','','','','','','','','','','0000000000010.36','201','01','N','OH','OH','N','45429-3527','','','','','0D2','00000','00000','JAMES G. M','3953','47804M878','N',0,'Y','N','','N','3','UBS FINANCIAL SERVICES INC. FBO','JOAN ULLOTH','3205 WALTHAM AVE','3','','','KETTERING','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. James Gregory Mayeux','','R','ZZ','',to_timestamp('2017-07-01 00:00:00.0','null'),'','5000001001',to_timestamp('2017-07-01 00:00:00.0','null'),'5000001001',to_timestamp('2017-07-01 00:00:00.0','null'),'201','N',75,'005',null,null,'N',null,to_timestamp('2020-01-02 20:36:30.0','null'),'2020-01-02');

use inv_jhi_typed_qa;
Insert into sdh_asset_position partition(process_date) values (31017200,'250123',403074,null,null,290144,null,null,1140,null,null,null,null,403074,609480,511693,null,null,0,null,'S','2017-07-31 00:00:00.0','9D5 57721 4269',null,'ASHWIN BRANDECKER',null,'47804M878',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,2488847,'3953','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0D2','0D2 GM','50040','0D2','0D2 GM','50040','0D2','0D2 GM','','','','','','','','','','','','','0000000000010.36','201','01','N','OH','OH','N','45429-3527','','','','','0D2','00000','00000','JAMES G. M','3953','47804M878','N',0,'Y','N','','N','3','UBS FINANCIAL SERVICES INC. FBO','JOAN ULLOTH','3205 WALTHAM AVE','3','','','KETTERING','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. James Gregory Mayeux','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','201','N',75,'005',null,null,'N',null,'2020-03-11 20:36:30.0','2020-03-11');
Insert into sdh_asset_position partition(process_date) values (31017201,'250123',403074,null,null,290144,null,null,1141,null,null,null,null,403074,609480,511693,null,null,0,null,'S','2017-07-31 00:00:00.0','9D5 57721 4269',null,'ASHWIN BRANDECKER',null,'47804M878',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,2488847,'3953','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0D2','0D2 GM','50040','0D2','0D2 GM','50040','0D2','0D2 GM','','','','','','','','','','','','','0000000000010.36','201','01','N','OH','OH','N','45429-3527','','','','','0D2','00000','00000','JAMES G. M','3953','47804M878','N',0,'Y','N','','N','3','UBS FINANCIAL SERVICES INC. FBO','JOAN ULLOTH','3205 WALTHAM AVE','3','','','KETTERING','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. James Gregory Mayeux','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','201','N',75,'005',null,null,'N',null,'2020-03-11 18:16:03.0','2020-03-11');
Insert into sdh_asset_position partition(process_date) values (31017202,'250123',403074,null,null,290144,null,null,1144,null,null,null,null,403074,609480,511693,null,null,0,null,'S','2017-07-31 00:00:00.0','9D5 57721 4269',null,'ASHWIN BRANDECKER',null,'47804M878',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,2488847,'3953','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0D2','0D2 GM','50040','0D2','0D2 GM','50040','0D2','0D2 GM','','','','','','','','','','','','','0000000000010.36','201','01','N','OH','OH','N','45429-3527','','','','','0D2','00000','00000','JAMES G. M','3953','47804M878','N',0,'Y','N','','N','3','UBS FINANCIAL SERVICES INC. FBO','JOAN ULLOTH','3205 WALTHAM AVE','3','','','KETTERING','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. James Gregory Mayeux','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','201','N',75,'005',null,null,'N',null,'2020-03-11 19:17:34.0','2020-03-11');
Insert into sdh_asset_position partition(process_date) values (31017203,'250496',403074,null,null,290128,null,null,1145,null,null,null,null,403074,963073,511688,null,null,6126.23,null,'S','2017-07-31 00:00:00.0','9JR 52875 1716',null,'ASHWIN BRANDECKER',null,'47804A130',null,561.01,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1809994,'3563','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0JR','0JR DS','50040','0JR','0JR DS','50040','0JR','0JR DS','','','','','','','','','','','','','0000000000010.92','201','01','N','HI','HI','N','96825-2622','','32050','','','0JR','00000','00000','DEF REP','3563','47804A130','N',0,'Y','N','','N','3','UBS FINANCIAL SERVICES INC. FBO','RACIE G.N.H. BOTELHO','1054-A AWAWAMALU ST','3','','','HONOLULU','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','John/Daniel Miyamasu/Shiu','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','201','N',75,'005','P','250496','N',null,'2020-03-11 20:36:30.0','2020-03-11');
Insert into sdh_asset_position partition(process_date) values (31017204,'250496',403074,null,null,290128,null,null,1148,null,null,null,null,403074,963073,511688,null,null,6126.23,null,'S','2017-07-31 00:00:00.0','9JR 52875 1716',null,'ASHWIN BRANDECKER',null,'47804A130',null,561.01,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1809994,'3563','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0JR','0JR DS','50040','0JR','0JR DS','50040','0JR','0JR DS','','','','','','','','','','','','','0000000000010.92','201','01','N','HI','HI','N','96825-2622','','32050','','','0JR','00000','00000','DEF REP','3563','47804A130','N',0,'Y','N','','N','3','UBS FINANCIAL SERVICES INC. FBO','RACIE G.N.H. BOTELHO','1054-A AWAWAMALU ST','3','','','HONOLULU','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','John/Daniel Miyamasu/Shiu','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','201','N',75,'005','P','250496','N',null,'2020-03-11 19:17:34.0','2020-03-11');
Insert into sdh_asset_position partition(process_date) values (31017205,'250496',403074,null,null,290128,null,null,1149,null,null,null,null,403074,963073,511688,null,null,6126.23,null,'S','2017-07-31 00:00:00.0','9JR 52875 1716',null,'ASHWIN BRANDECKER',null,'47804A130',null,561.01,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1809994,'3563','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0JR','0JR DS','50040','0JR','0JR DS','50040','0JR','0JR DS','','','','','','','','','','','','','0000000000010.92','201','01','N','HI','HI','N','96825-2622','','32050','','','0JR','00000','00000','DEF REP','3563','47804A130','N',0,'Y','N','','N','3','UBS FINANCIAL SERVICES INC. FBO','RACIE G.N.H. BOTELHO','1054-A AWAWAMALU ST','3','','','HONOLULU','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','John/Daniel Miyamasu/Shiu','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','201','N',75,'005','P','250496','N',null,'2020-03-11 18:16:03.0','2020-03-11');
Insert into sdh_asset_position partition(process_date) values (31017206,'250987',403074,null,null,289792,null,null,1152,null,null,null,null,403074,755642,511538,null,null,30136.31,null,'S','2017-07-31 00:00:00.0','9SO 61239 9716',null,'ASHWIN BRANDECKER',null,'47803T627',null,1179.042,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,3076178,'487','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0SO','0SO 87','50040','0SO','0SO 87','50040','0SO','0SO 87','','','','','','','','','','','','','0000000000025.56','408','28','N','CA','CA','N','95831-5799','','','','','0SO','00000','00000','STEVEN MUE','487','47803T627','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','WILLIAM GARCIA','ROLLOVER IRA','4','7754 OAK BAY CIRCLE','','SACRAMENTO','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. Steven Mueller','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','408','N',75,'880',null,null,'N',null,'2020-03-11 20:36:30.0','2020-03-11');
Insert into sdh_asset_position partition(process_date) values (31017207,'250987',403074,null,null,289792,null,null,1153,null,null,null,null,403074,755642,511538,null,null,30136.31,null,'S','2017-07-31 00:00:00.0','9SO 61239 9716',null,'ASHWIN BRANDECKER',null,'47803T627',null,1179.042,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,3076178,'487','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0SO','0SO 87','50040','0SO','0SO 87','50040','0SO','0SO 87','','','','','','','','','','','','','0000000000025.56','408','28','N','CA','CA','N','95831-5799','','','','','0SO','00000','00000','STEVEN MUE','487','47803T627','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','WILLIAM GARCIA','ROLLOVER IRA','4','7754 OAK BAY CIRCLE','','SACRAMENTO','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. Steven Mueller','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','408','N',75,'880',null,null,'N',null,'2020-03-11 18:16:03.0','2020-03-11');
--invalid stat_cd = 'D'
Insert into sdh_asset_position partition(process_date) values (31017208,'250987',403074,null,null,289792,null,null,1156,null,null,null,null,403074,755642,511538,null,null,30136.31,null,'S','2017-07-31 00:00:00.0','9SO 61239 9716',null,'ASHWIN BRANDECKER',null,'47803T627',null,1179.042,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,3076178,'487','UBS-DSP','DST',' ','',null,'000','000','N','D','50040','0SO','0SO 87','50040','0SO','0SO 87','50040','0SO','0SO 87','','','','','','','','','','','','','0000000000025.56','408','28','N','CA','CA','N','95831-5799','','','','','0SO','00000','00000','STEVEN MUE','487','47803T627','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','WILLIAM GARCIA','ROLLOVER IRA','4','7754 OAK BAY CIRCLE','','SACRAMENTO','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. Steven Mueller','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','408','N',75,'880',null,null,'N',null,'2020-03-11 19:17:34.0','2020-03-11');
Insert into sdh_asset_position partition(process_date) values (31017209,'251002',403074,null,null,289792,null,null,1157,null,null,null,null,403074,755642,511538,null,null,17731.1,null,'S','2017-07-31 00:00:00.0','9SO 61347 6716',null,'ASHWIN BRANDECKER',null,'47803T627',null,693.705,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,3076178,'487','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0SO','0SO 87','50040','0SO','0SO 87','50040','0SO','0SO 87','','','','','','','','','','','','','0000000000025.56','403','17','N','CA','CA','N','95630-6145','','','','','0SO','00000','00000','STEVEN MUE','487','47803T627','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','JOSEPH HUGH BOGARD','TRADITIONAL IRA','4','787 MORNINGSIDE DRIVE','','FOLSOM','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. Steven Mueller','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','403','N',75,'880',null,null,'N',null,'2020-03-11 19:17:34.0','2020-03-11');
Insert into sdh_asset_position partition(process_date) values (31017210,'251002',403074,null,null,289792,null,null,1158,null,null,null,null,403074,755642,511538,null,null,17731.1,null,'S','2017-07-31 00:00:00.0','9SO 61347 6716',null,'ASHWIN BRANDECKER',null,'47803T627',null,693.705,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,3076178,'487','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0SO','0SO 87','50040','0SO','0SO 87','50040','0SO','0SO 87','','','','','','','','','','','','','0000000000025.56','403','17','N','CA','CA','N','95630-6145','','','','','0SO','00000','00000','STEVEN MUE','487','47803T627','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','JOSEPH HUGH BOGARD','TRADITIONAL IRA','4','787 MORNINGSIDE DRIVE','','FOLSOM','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. Steven Mueller','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','403','N',75,'880',null,null,'N',null,'2020-03-11 18:16:03.0','2020-03-11');
Insert into sdh_asset_position partition(process_date) values (31017211,'251002',403074,null,null,289792,null,null,1159,null,null,null,null,403074,755642,511538,null,null,17731.1,null,'S','2017-07-31 00:00:00.0','9SO 61347 6716',null,'ASHWIN BRANDECKER',null,'47803T627',null,693.705,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,3076178,'487','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0SO','0SO 87','50040','0SO','0SO 87','50040','0SO','0SO 87','','','','','','','','','','','','','0000000000025.56','403','17','N','CA','CA','N','95630-6145','','','','','0SO','00000','00000','STEVEN MUE','487','47803T627','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','JOSEPH HUGH BOGARD','TRADITIONAL IRA','4','787 MORNINGSIDE DRIVE','','FOLSOM','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. Steven Mueller','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','403','N',75,'880',null,null,'N',null,'2020-03-11 20:36:30.0','2020-03-11');
use inv_jhi_typed_qa;
Insert into sdh_asset_position partition(process_date) values (31017221,'250496',403074,null,null,290128,null,null,1149,null,null,null,null,403074,963073,511688,null,null,6126.23,null,'S','2017-07-31 00:00:00.0','9JR 52875 1716',null,'ASHWIN BRANDECKER',null,'47804A130',null,561.01,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1809994,'3563','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0JR','0JR DS','50040','0JR','0JR DS','50040','0JR','0JR DS','','','','','','','','','','','','','0000000000010.92','201','01','N','HI','HI','N','96825-2622','','32050','','','0JR','00000','00000','DEF REP','3563','47804A130','N',0,'Y','N','','N','3','UBS FINANCIAL SERVICES INC. FBO','RACIE G.N.H. BOTELHO','1054-A AWAWAMALU ST','3','','','HONOLULU','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','John/Daniel Miyamasu/Shiu','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','201','N',75,'005','M','250496','N',null,'2020-03-11 18:16:03.0','2020-03-11');
Insert into sdh_asset_position partition(process_date) values (31017222,'250496',403074,null,null,290128,null,null,1145,null,null,null,null,403074,963073,511688,null,null,6126.23,null,'S','2017-07-31 00:00:00.0','9JR 52875 1716',null,'ASHWIN BRANDECKER',null,'47804A130',null,561.01,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1809994,'3563','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0JR','0JR DS','50040','0JR','0JR DS','50040','0JR','0JR DS','','','','','','','','','','','','','0000000000010.92','201','01','N','HI','HI','N','96825-2622','','32050','','','0JR','00000','00000','DEF REP','3563','47804A130','N',0,'Y','N','','N','3','UBS FINANCIAL SERVICES INC. FBO','RACIE G.N.H. BOTELHO','1054-A AWAWAMALU ST','3','','','HONOLULU','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','John/Daniel Miyamasu/Shiu','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','201','N',75,'005','M','250496','N',null,'2020-03-11 20:36:30.0','2020-03-11');
Insert into sdh_asset_position partition(process_date) values (31017223,'250496',403074,null,null,290128,null,null,1148,null,null,null,null,403074,963073,511688,null,null,6126.23,null,'S','2017-07-31 00:00:00.0','9JR 52875 1716',null,'ASHWIN BRANDECKER',null,'47804A130',null,561.01,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1809994,'3563','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0JR','0JR DS','50040','0JR','0JR DS','50040','0JR','0JR DS','','','','','','','','','','','','','0000000000010.92','201','01','N','HI','HI','N','96825-2622','','32050','','','0JR','00000','00000','DEF REP','3563','47804A130','N',0,'Y','N','','N','3','UBS FINANCIAL SERVICES INC. FBO','RACIE G.N.H. BOTELHO','1054-A AWAWAMALU ST','3','','','HONOLULU','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','John/Daniel Miyamasu/Shiu','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','201','N',75,'005','M','250496','N',null,'2020-03-11 19:17:34.0','2020-03-11');


--insert below in sdh_product;
Insert into sdh_product values (1140,1374,null,null,209,1957,null,'3487',null,'z_Small Company R3',null,'47803U475',null,null,null,null,null,21.15,'2018-03-23 00:00:00.0',1388,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JCSHX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1141,789,null,null,261,1957,null,'3488',null,'z_Small Company R4',null,'47803U467',null,null,null,null,null,22.16,'2018-03-23 00:00:00.0',1388,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JCSFX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1142,1374,null,null,304,1957,null,'3489',null,'z_Small Company R5',null,'47803U459',null,null,null,null,null,22.69,'2018-03-23 00:00:00.0',1388,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JCSVX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1143,789,null,null,610,1420,null,'3500',null,'Short Duration Credit Opportunities A',null,'47804B401',null,null,null,null,null,9.51,'2019-10-14 00:00:00.0',null,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JMBAX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1144,954,null,null,767,1420,null,'3502',null,'Short Duration Credit Opportunities C',null,'47805A766',null,null,null,null,null,9.51,'2019-10-14 00:00:00.0',null,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JMBCX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1145,1154,null,null,1128,1420,null,'3503',null,'Short Duration Credit Opportunities I',null,'47804B500',null,null,null,null,null,9.49,'2019-10-14 00:00:00.0',null,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JMBIX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1146,954,null,null,154,1420,null,'3506',null,'z_ Short Duration Credit Opportunities R2',null,'47805A550',null,null,null,null,null,9.5,'2019-10-14 00:00:00.0',2049,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JHDOX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1147,1163,null,null,261,1420,null,'3508',null,'z_ Short Duration Credit Opportunities R4',null,'47805A543',null,null,null,null,null,9.5,'2019-10-14 00:00:00.0',2049,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JHDCX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1148,1408,null,null,1978,1957,null,'351',null,'z_Small Company ADV',null,'47803W307',null,null,null,null,null,22.06,'2018-03-23 00:00:00.0',1388,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JCSDX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1149,1391,null,null,610,1048,null,'3520',null,'Global Income  A',null,'47804B104',null,null,null,null,null,9.07,'2018-06-12 00:00:00.0',null,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JYGAX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1150,1408,null,null,1128,1048,null,'3523',null,'Global Income I',null,'47804B203',null,null,null,null,null,9.06,'2018-06-12 00:00:00.0',null,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JYGIX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1151,1391,null,null,610,1501,null,'3540',null,'Natural Resources A',null,'47804A171',null,null,null,null,null,11.9,'2018-10-19 00:00:00.0',null,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JNRAX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1152,1359,null,null,1128,1501,null,'3543',null,'Natural Resources I',null,'47804A163',null,null,null,null,null,11.87,'2018-10-19 00:00:00.0',null,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JNRIX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1153,3988,null,null,610,1813,null,'3560',null,'Strategic Income Opportunities A',null,'47804A155',null,null,null,null,null,10.62,'2019-10-14 00:00:00.0',null,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JIPAX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1154,1359,null,null,767,1813,null,'3562',null,'Strategic Income Opportunities C',null,'47804A148',null,null,null,null,null,10.62,'2019-10-14 00:00:00.0',null,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JIPCX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1155,3988,null,null,1128,1813,null,'3563',null,'Strategic Income Opportunities I',null,'47804A130',null,null,null,null,null,10.63,'2019-10-14 00:00:00.0',null,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JIPIX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1156,1374,null,null,610,928,null,'3580',null,'Emerging Markets Debt A',null,'47804B708',null,null,null,null,null,9.39,'2019-10-14 00:00:00.0',null,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JMKAX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1157,789,null,null,767,928,null,'3582',null,'Emerging Markets Debt C',null,'41015K276',null,null,null,null,null,9.38,'2019-10-14 00:00:00.0',null,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JMKCX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1158,954,null,null,1128,928,null,'3583',null,'Emerging Markets Debt I',null,'47804B807',null,null,null,null,null,9.4,'2019-10-14 00:00:00.0',null,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JMKIX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1159,1163,null,null,154,928,null,'3586',null,'Emerging Markets Debt R2',null,'47805A659',null,null,null,null,null,9.39,'2019-10-14 00:00:00.0',null,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JHEMX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1160,1408,null,null,261,928,null,'3588',null,'z_ Emerging Markets Debt R4',null,'47805A642',null,null,null,null,null,9.38,'2019-10-14 00:00:00.0',2049,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','JHMDX','2020-03-11 19:17:34.0');
Insert into sdh_product values (1161,1391,null,null,610,1781,null,'36',null,'Balanced A',null,'47803P104',null,null,null,null,null,20.56,'2019-10-14 00:00:00.0',null,'1000002001','2017-07-30 00:00:00.0','1000748001','2019-10-11 00:00:00.0','SVBAX','2020-03-11 19:17:34.0');

--data mockup for day1
+---------------------------------------+--------------------+-----------+------------------------+------------------------+--+
|           d.distributor_key           | r.distributor_uid  | r.src_id  |  d.bus_eff_date_from   |   d.bus_eff_date_to    |
+---------------------------------------+--------------------+-----------+------------------------+------------------------+--+
| ea750e59-5767-4b4c-8a33-86242f71dce0  | 4787               | 402717    | 1980-01-01 00:00:00.0  | 2099-12-31 00:00:00.0  |
| b0de9fa2-4f03-4b9b-9ecb-033da47f071f  | 4787               | 402717    | 1980-01-01 00:00:00.0  | 2020-01-08 00:00:00.0  |
| 4e4ca41b-c6d9-4082-9689-9c3d90f81f56  | 4788               | 402685    | 1980-01-01 00:00:00.0  | 2099-12-31 00:00:00.0  |
| 28517b5b-894d-42b4-b241-69bf3bf5cecf  | 4789               | 402604    | 1980-01-01 00:00:00.0  | 2099-12-31 00:00:00.0  |
| bc9fe697-e6f3-421e-881c-766a94599002  | 4793               | 402517    | 1980-01-01 00:00:00.0  | 2099-12-31 00:00:00.0  |
| 5976c6d0-bf1c-45d8-b524-e5eb37423c62  | 4794               | 402507    | 1980-01-01 00:00:00.0  | 2099-12-31 00:00:00.0  |
| 2d60c5b9-f955-4cce-bd12-f72895061bbd  | 4795               | 402506    | 1980-01-01 00:00:00.0  | 2099-12-31 00:00:00.0  |
| 8b04eeaf-ed29-410e-a28c-a1308fc66b71  | 4798               | 402418    | 1980-01-01 00:00:00.0  | 2099-12-31 00:00:00.0  |
| 8835421e-9eee-48bd-ba36-0f05ad95bdcc  | 4799               | 402744    | 1980-01-01 00:00:00.0  | 2099-12-31 00:00:00.0  |

| 0e78866e-ab7c-48c7-a0fb-88b4733df829  | 3988               | 413364    | 1980-01-01 00:00:00.0  | 2099-12-31 00:00:00.0  |
| 859b9397-4b6b-4871-b5db-875577fe9789  | 3991               | 412242    | 1980-01-01 00:00:00.0  | 2099-12-31 00:00:00.0  |
| 041b0127-11fc-4d6a-9cbf-89eb944afb1d  | 3998               | 412225    | 1980-01-01 00:00:00.0  | 2099-12-31 00:00:00.0  |
| e2b333d7-72f3-4138-aa29-1150c5ef1a37  | 3999               | 412234    | 1980-01-01 00:00:00.0  | 2099-12-31 00:00:00.0  |
| 4de67e9b-e49a-420f-a174-3678013e60dc  | 4006               | 412125    | 1980-01-01 00:00:00.0  | 2099-12-31 00:00:00.0  |
| c394d395-d0be-4fcd-a6a7-f15c62d2c312  | 4017               | 412544    | 1980-01-01 00:00:00.0  | 2099-12-31 00:00:00.0  |
| dba803a1-66f7-45e3-b8e7-525044d01d70  | 4030               | 411728    | 1980-01-01 00:00:00.0  | 2099-12-31 00:00:00.0  |
| e60e7671-a414-4c95-9c03-5d184dd78624  | 4034               | 411807    | 1980-01-01 00:00:00.0  | 2099-12-31 00:00:00.0  |




use inv_jhi_typed_qa;
--new
Insert into sdh_asset_position partition(process_date) values (31017212,'251335',402685,null,null,289096,null,null,1160,null,null,null,null,403074,913133,511629,null,null,0,null,'S','2017-07-31 00:00:00.0','9JU 81474 1716',null,'ASHWIN BRANDECKER',null,'47804A130',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1711250,'3563','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0JU','0JU 45','50040','0JU','0JU 45','50040','0JU','0JU 45','','','','','','','','','','','','','0000000000010.92','510','34','N','ME','ME','N','04107-2639','','','','','0JU','00000','00000','DEF REP','3563','47804A130','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','CATHERINE MILLER','SEP-IRA','4','122 OLD OCEAN HOUSE RD','','CAPE ELIZABETH','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Hagood/Lloyd/Christopher Clarke/Macedo/Niles','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','510','N',75,'885','P','251335','N',null,'2020-03-14 19:17:34.0','2020-03-14');
--invalid stat_cd = 'D' -invalid
Insert into sdh_asset_position partition(process_date) values (31017213,'251335',402604,null,null,289096,null,null,1160,null,null,null,null,403074,913133,511629,null,null,0,null,'S','2017-07-31 00:00:00.0','9JU 81474 1716',null,'ASHWIN BRANDECKER',null,'47804A130',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1711250,'3563','UBS-DSP','DST',' ','',null,'000','000','N','D','50040','0JU','0JU 45','50040','0JU','0JU 45','50040','0JU','0JU 45','','','','','','','','','','','','','0000000000010.92','510','34','N','ME','ME','N','04107-2639','','','','','0JU','00000','00000','DEF REP','3563','47804A130','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','CATHERINE MILLER','SEP-IRA','4','122 OLD OCEAN HOUSE RD','','CAPE ELIZABETH','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Hagood/Lloyd/Christopher Clarke/Macedo/Niles','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','510','N',75,'885','P','251335','N',null,'2020-03-14 20:36:30.0','2020-03-14');
--rep_id = '888888' -invalid
Insert into sdh_asset_position partition(process_date) values (31017214,'251335',402517,null,null,289096,null,null,1152,null,null,null,null,403074,888888,511629,null,null,0,null,'S','2017-07-31 00:00:00.0','9JU 81474 1716',null,'ASHWIN BRANDECKER',null,'47804A130',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1711250,'3563','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0JU','0JU 45','50040','0JU','0JU 45','50040','0JU','0JU 45','','','','','','','','','','','','','0000000000010.92','510','34','N','ME','ME','N','04107-2639','','','','','0JU','00000','00000','DEF REP','3563','47804A130','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','CATHERINE MILLER','SEP-IRA','4','122 OLD OCEAN HOUSE RD','','CAPE ELIZABETH','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Hagood/Lloyd/Christopher Clarke/Macedo/Niles','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','510','N',75,'885','P','251335','N',null,'2020-03-14 18:16:03.0','2020-03-14');
Insert into sdh_asset_position partition(process_date) values (31017215,'251777',402507,null,null,290013,null,null,1155,null,null,null,null,403074,785039,511651,null,null,0,null,'S','2017-07-31 00:00:00.0','9PE 49440 1716',null,'ASHWIN BRANDECKER',null,'47804A734',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,3284924,'3280','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0PE','0PE WB','50040','0PE','0PE WB','50040','0PE','0PE WB','','','','','','','','','','','','','0000000000008.53','203','03','N','TX','TX','N','79424-4860','','','','','0PE','00000','00000','','3280','47804A734','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','JOHN R. MAXWELL','ANN ALLEN MAXWELL COMM PROP','4','5010 91ST STREET #18','','LUBBOCK','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. William F Blake II','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','203','N',75,'015',null,null,'N',null,'2020-03-14 18:16:03.0','2020-03-14');
Insert into sdh_asset_position partition(process_date) values (31017216,'251777',402506,null,null,290013,null,null,1156,null,null,null,null,403074,785039,511651,null,null,0,null,'S','2017-07-31 00:00:00.0','9PE 49440 1716',null,'ASHWIN BRANDECKER',null,'47804A734',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,3284924,'3280','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0PE','0PE WB','50040','0PE','0PE WB','50040','0PE','0PE WB','','','','','','','','','','','','','0000000000008.53','203','03','N','TX','TX','N','79424-4860','','','','','0PE','00000','00000','','3280','47804A734','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','JOHN R. MAXWELL','ANN ALLEN MAXWELL COMM PROP','4','5010 91ST STREET #18','','LUBBOCK','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. William F Blake II','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','203','N',75,'015',null,null,'N',null,'2020-03-14 19:17:34.0','2020-03-14');
Insert into sdh_asset_position partition(process_date) values (31017217,'251777',402506,null,null,290013,null,null,1157,null,null,null,null,403074,785039,511651,null,null,0,null,'S','2017-07-31 00:00:00.0','9PE 49440 1716',null,'ASHWIN BRANDECKER',null,'47804A734',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,3284924,'3280','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0PE','0PE WB','50040','0PE','0PE WB','50040','0PE','0PE WB','','','','','','','','','','','','','0000000000008.53','203','03','N','TX','TX','N','79424-4860','','','','','0PE','00000','00000','','3280','47804A734','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','JOHN R. MAXWELL','ANN ALLEN MAXWELL COMM PROP','4','5010 91ST STREET #18','','LUBBOCK','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. William F Blake II','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','203','N',75,'015',null,null,'N',null,'2020-03-14 20:36:30.0','2020-03-14');
Insert into sdh_asset_position partition(process_date) values (31017218,'251890',402506,null,null,289709,null,null,1158,null,null,null,null,403074,577959,511470,null,null,0,null,'S','2017-07-31 00:00:00.0','9VN 77987 5716',null,'ASHWIN BRANDECKER',null,'47803W505',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1518685,'3630','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0VN','0VN WS','50040','0VN','0VN WS','50040','0VN','0VN WS','','','','','','','','','','','','','0000000000022.29','408','28','N','FL','FL','N','33715-2103','','','','','0VN','00000','00000','SHELL','3630','47803W505','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','THOMAS A WILLIAMSON III','ROLLOVER IRA','4','1100 PINELLAS BAYWAY S','APT J2','SAINT PETERSBURG','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Willcox/Justin Bailey/Steil','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','408','N',75,'880','P','251890','N',null,'2020-03-14 20:36:30.0','2020-03-14');
Insert into sdh_asset_position partition(process_date) values (31017219,'251890',402418,null,null,289709,null,null,1159,null,null,null,null,403074,577959,511470,null,null,0,null,'S','2017-07-31 00:00:00.0','9VN 77987 5716',null,'ASHWIN BRANDECKER',null,'47803W505',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1518685,'3630','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0VN','0VN WS','50040','0VN','0VN WS','50040','0VN','0VN WS','','','','','','','','','','','','','0000000000022.29','408','28','N','FL','FL','N','33715-2103','','','','','0VN','00000','00000','SHELL','3630','47803W505','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','THOMAS A WILLIAMSON III','ROLLOVER IRA','4','1100 PINELLAS BAYWAY S','APT J2','SAINT PETERSBURG','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Willcox/Justin Bailey/Steil','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','408','N',75,'880','P','251890','N',null,'2020-03-14 18:16:03.0','2020-03-14');
Insert into sdh_asset_position partition(process_date) values (31017220,'251890',402744,null,null,289709,null,null,1160,null,null,null,null,403074,577959,511470,null,null,0,null,'S','2017-07-31 00:00:00.0','9VN 77987 5716',null,'ASHWIN BRANDECKER',null,'47803W505',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1518685,'3630','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0VN','0VN WS','50040','0VN','0VN WS','50040','0VN','0VN WS','','','','','','','','','','','','','0000000000022.29','408','28','N','FL','FL','N','33715-2103','','','','','0VN','00000','00000','SHELL','3630','47803W505','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','THOMAS A WILLIAMSON III','ROLLOVER IRA','4','1100 PINELLAS BAYWAY S','APT J2','SAINT PETERSBURG','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Willcox/Justin Bailey/Steil','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','408','N',75,'880','P','251890','N',null,'2020-03-14 19:17:34.0','2020-03-14');
--updates
--part_trade_ind = 'P'
Insert into sdh_asset_position partition(process_date) values (31017202,'250123',403074,null,null,290144,null,null,1144,null,null,null,null,403074,609480,511693,null,null,0,null,'S','2017-07-31 00:00:00.0','9D5 57721 4269',null,'ASHWIN BRANDECKER',null,'47804M878',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,2488847,'3953','UBS-DSP','DST',' ','',null,'000','000','N','D','50040','0D2','0D2 GM','50040','0D2','0D2 GM','50040','0D2','0D2 GM','','','','','','','','','','','','','0000000000010.36','201','01','N','OH','OH','N','45429-3527','','','','','0D2','00000','00000','JAMES G. M','3953','47804M878','N',0,'Y','N','','N','3','UBS FINANCIAL SERVICES INC. FBO','JOAN ULLOTH','3205 WALTHAM AVE','3','','','KETTERING','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. James Gregory Mayeux','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','201','N',75,'005','P',null,'N',null,'2020-03-14 19:17:34.0','2020-03-14');
--filter_status_cd = 'NS'
Insert into sdh_asset_position partition(process_date) values (31017221,'250496',403074,null,null,290128,null,null,1145,null,null,null,null,403074,963073,511688,null,null,6126.23,null,'S','2017-07-31 00:00:00.0','9JR 52875 1716',null,'ASHWIN BRANDECKER',null,'47804A130',null,561.01,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1809994,'3563','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0JR','0JR DS','50040','0JR','0JR DS','50040','0JR','0JR DS','','','','','','','','','','','','','0000000000010.92','201','01','N','HI','HI','N','96825-2622','','32050','','','0JR','00000','00000','DEF REP','3563','47804A130','N',0,'Y','N','','N','3','UBS FINANCIAL SERVICES INC. FBO','RACIE G.N.H. BOTELHO','1054-A AWAWAMALU ST','3','','','HONOLULU','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','John/Daniel Miyamasu/Shiu','','R','NS','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','201','N',75,'005','M','250496','N',null,'2020-03-14 20:36:30.0','2020-03-14');
--stat_cd = 'D'
Insert into sdh_asset_position partition(process_date) values (31017222,'250496',403074,null,null,290128,null,null,1148,null,null,null,null,403074,963073,511688,null,null,6126.23,null,'S','2017-07-31 00:00:00.0','9JR 52875 1716',null,'ASHWIN BRANDECKER',null,'47804A130',null,561.01,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1809994,'3563','UBS-DSP','DST',' ','',null,'000','000','N','D','50040','0JR','0JR DS','50040','0JR','0JR DS','50040','0JR','0JR DS','','','','','','','','','','','','','0000000000010.92','201','01','N','HI','HI','N','96825-2622','','32050','','','0JR','00000','00000','DEF REP','3563','47804A130','N',0,'Y','N','','N','3','UBS FINANCIAL SERVICES INC. FBO','RACIE G.N.H. BOTELHO','1054-A AWAWAMALU ST','3','','','HONOLULU','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','John/Daniel Miyamasu/Shiu','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','201','N',75,'005','M','250496','N',null,'2020-03-14 19:17:34.0','2020-03-14');
--eff_dta='2020-03-14 00:00:00.0' (col22)
Insert into sdh_asset_position partition(process_date) values (31017223,'250496',403074,null,null,290128,null,null,1149,null,null,null,null,403074,963073,511688,null,null,6126.23,null,'S','2020-03-14 00:00:00.0','9JR 52875 1716',null,'ASHWIN BRANDECKER',null,'47804A130',null,561.01,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1809994,'3563','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0JR','0JR DS','50040','0JR','0JR DS','50040','0JR','0JR DS','','','','','','','','','','','','','0000000000010.92','201','01','N','HI','HI','N','96825-2622','','32050','','','0JR','00000','00000','DEF REP','3563','47804A130','N',0,'Y','N','','N','3','UBS FINANCIAL SERVICES INC. FBO','RACIE G.N.H. BOTELHO','1054-A AWAWAMALU ST','3','','','HONOLULU','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','John/Daniel Miyamasu/Shiu','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','201','N',75,'005','M','250496','N',null,'2020-03-14 18:16:03.0','2020-03-14');
----aum_amt = 4888.99(col 19)
Insert into sdh_asset_position partition(process_date) values (31017206,'250987',403074,null,null,289792,null,null,1152,null,null,null,null,403074,755642,511538,null,null,4888.99,null,'S','2017-07-31 00:00:00.0','9SO 61239 9716',null,'ASHWIN BRANDECKER',null,'47803T627',null,1179.042,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,3076178,'487','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0SO','0SO 87','50040','0SO','0SO 87','50040','0SO','0SO 87','','','','','','','','','','','','','0000000000025.56','408','28','N','CA','CA','N','95831-5799','','','','','0SO','00000','00000','STEVEN MUE','487','47803T627','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','WILLIAM GARCIA','ROLLOVER IRA','4','7754 OAK BAY CIRCLE','','SACRAMENTO','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. Steven Mueller','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','408','N',75,'880',null,null,'N',null,'2020-03-14 20:36:30.0','2020-03-14');

--existing
Insert into sdh_asset_position partition(process_date) values (31017200,'250123',403074,null,null,290144,null,null,1140,null,null,null,null,403074,609480,511693,null,null,0,null,'S','2017-07-31 00:00:00.0','9D5 57721 4269',null,'ASHWIN BRANDECKER',null,'47804M878',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,2488847,'3953','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0D2','0D2 GM','50040','0D2','0D2 GM','50040','0D2','0D2 GM','','','','','','','','','','','','','0000000000010.36','201','01','N','OH','OH','N','45429-3527','','','','','0D2','00000','00000','JAMES G. M','3953','47804M878','N',0,'Y','N','','N','3','UBS FINANCIAL SERVICES INC. FBO','JOAN ULLOTH','3205 WALTHAM AVE','3','','','KETTERING','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. James Gregory Mayeux','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','201','N',75,'005',null,null,'N',null,'2020-03-14 20:36:30.0','2020-03-14');
Insert into sdh_asset_position partition(process_date) values (31017207,'250987',403074,null,null,289792,null,null,1153,null,null,null,null,403074,755642,511538,null,null,30136.31,null,'S','2017-07-31 00:00:00.0','9SO 61239 9716',null,'ASHWIN BRANDECKER',null,'47803T627',null,1179.042,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,3076178,'487','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0SO','0SO 87','50040','0SO','0SO 87','50040','0SO','0SO 87','','','','','','','','','','','','','0000000000025.56','408','28','N','CA','CA','N','95831-5799','','','','','0SO','00000','00000','STEVEN MUE','487','47803T627','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','WILLIAM GARCIA','ROLLOVER IRA','4','7754 OAK BAY CIRCLE','','SACRAMENTO','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. Steven Mueller','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','408','N',75,'880',null,null,'N',null,'2020-03-14 18:16:03.0','2020-03-14');


--DAY1--2ND
use inv_jhi_typed_qa;
Insert into sdh_asset_position partition(process_date) values (31017224,'251335',402517,null,null,289096,null,null,1152,null,null,null,null,403074,888888,511629,null,null,3467.00,null,'S','2017-07-31 00:00:00.0','9JU 81474 1716',null,'ASHWIN BRANDECKER',null,'47804A130',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1711250,'3563','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0JU','0JU 45','50040','0JU','0JU 45','50040','0JU','0JU 45','','','','','','','','','','','','','0000000000010.92','510','34','N','ME','ME','N','04107-2639','','','','','0JU','00000','00000','DEF REP','3563','47804A130','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','CATHERINE MILLER','SEP-IRA','4','122 OLD OCEAN HOUSE RD','','CAPE ELIZABETH','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Hagood/Lloyd/Christopher Clarke/Macedo/Niles','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','510','N',75,'885','P','251335','N',null,'2020-03-15 12:16:03.0','2020-03-15');
Insert into sdh_asset_position partition(process_date) values (31017225,'251777',402507,null,null,290013,null,null,1155,null,null,null,null,403074,785039,511651,null,null,3498.67,null,'S','2017-07-31 00:00:00.0','9PE 49440 1716',null,'ASHWIN BRANDECKER',null,'47804A734',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,3284924,'3280','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0PE','0PE WB','50040','0PE','0PE WB','50040','0PE','0PE WB','','','','','','','','','','','','','0000000000008.53','203','03','N','TX','TX','N','79424-4860','','','','','0PE','00000','00000','','3280','47804A734','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','JOHN R. MAXWELL','ANN ALLEN MAXWELL COMM PROP','4','5010 91ST STREET #18','','LUBBOCK','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. William F Blake II','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','203','N',75,'015',null,null,'N',null,'2020-03-15 12:16:03.0','2020-03-15');
Insert into sdh_asset_position partition(process_date) values (31017226,'251777',402506,null,null,290013,null,null,1156,null,null,null,null,403074,785039,511651,null,null,187.11,null,'S','2017-07-31 00:00:00.0','9PE 49440 1716',null,'ASHWIN BRANDECKER',null,'47804A734',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,3284924,'3280','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0PE','0PE WB','50040','0PE','0PE WB','50040','0PE','0PE WB','','','','','','','','','','','','','0000000000008.53','203','03','N','TX','TX','N','79424-4860','','','','','0PE','00000','00000','','3280','47804A734','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','JOHN R. MAXWELL','ANN ALLEN MAXWELL COMM PROP','4','5010 91ST STREET #18','','LUBBOCK','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. William F Blake II','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','203','N',75,'015',null,null,'N',null,'2020-03-15 12:16:03.0','2020-03-15');
Insert into sdh_asset_position partition(process_date) values (31017227,'251777',402506,null,null,290013,null,null,1157,null,null,null,null,403074,785039,511651,null,null,8900.67,null,'S','2017-07-31 00:00:00.0','9PE 49440 1716',null,'ASHWIN BRANDECKER',null,'47804A734',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,3284924,'3280','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0PE','0PE WB','50040','0PE','0PE WB','50040','0PE','0PE WB','','','','','','','','','','','','','0000000000008.53','203','03','N','TX','TX','N','79424-4860','','','','','0PE','00000','00000','','3280','47804A734','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','JOHN R. MAXWELL','ANN ALLEN MAXWELL COMM PROP','4','5010 91ST STREET #18','','LUBBOCK','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. William F Blake II','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','203','N',75,'015',null,null,'N',null,'2020-03-15 12:16:03.0','2020-03-15');
Insert into sdh_asset_position partition(process_date) values (31017228,'251890',402506,null,null,289709,null,null,1158,null,null,null,null,403074,577959,511470,null,null,754.98,null,'S','2017-07-31 00:00:00.0','9VN 77987 5716',null,'ASHWIN BRANDECKER',null,'47803W505',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1518685,'3630','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0VN','0VN WS','50040','0VN','0VN WS','50040','0VN','0VN WS','','','','','','','','','','','','','0000000000022.29','408','28','N','FL','FL','N','33715-2103','','','','','0VN','00000','00000','SHELL','3630','47803W505','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','THOMAS A WILLIAMSON III','ROLLOVER IRA','4','1100 PINELLAS BAYWAY S','APT J2','SAINT PETERSBURG','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Willcox/Justin Bailey/Steil','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','408','N',75,'880','P','251890','N',null,'2020-03-15 12:16:03.0','2020-03-15');
Insert into sdh_asset_position partition(process_date) values (31017229,'251890',402418,null,null,289709,null,null,1159,null,null,null,null,403074,577959,511470,null,null,4775.80,null,'S','2017-07-31 00:00:00.0','9VN 77987 5716',null,'ASHWIN BRANDECKER',null,'47803W505',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1518685,'3630','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0VN','0VN WS','50040','0VN','0VN WS','50040','0VN','0VN WS','','','','','','','','','','','','','0000000000022.29','408','28','N','FL','FL','N','33715-2103','','','','','0VN','00000','00000','SHELL','3630','47803W505','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','THOMAS A WILLIAMSON III','ROLLOVER IRA','4','1100 PINELLAS BAYWAY S','APT J2','SAINT PETERSBURG','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Willcox/Justin Bailey/Steil','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','408','N',75,'880','P','251890','N',null,'2020-03-15 12:16:03.0','2020-03-15');
Insert into sdh_asset_position partition(process_date) values (31017230,'251890',402744,null,null,289709,null,null,1160,null,null,null,null,403074,577959,511470,null,null,3874.09,null,'S','2017-07-31 00:00:00.0','9VN 77987 5716',null,'ASHWIN BRANDECKER',null,'47803W505',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1518685,'3630','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0VN','0VN WS','50040','0VN','0VN WS','50040','0VN','0VN WS','','','','','','','','','','','','','0000000000022.29','408','28','N','FL','FL','N','33715-2103','','','','','0VN','00000','00000','SHELL','3630','47803W505','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','THOMAS A WILLIAMSON III','ROLLOVER IRA','4','1100 PINELLAS BAYWAY S','APT J2','SAINT PETERSBURG','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Willcox/Justin Bailey/Steil','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','408','N',75,'880','P','251890','N',null,'2020-03-15 12:16:03.0','2020-03-15');
--updates
--part_trade_ind = 'P'
Insert into sdh_asset_position partition(process_date) values (31017202,'250123',403074,null,null,290144,null,null,1144,null,null,null,null,403074,609480,511693,null,null,111.35,null,'S','2017-07-31 00:00:00.0','9D5 57721 4269',null,'ASHWIN BRANDECKER',null,'47804M878',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,2488847,'3953','UBS-DSP','DST',' ','',null,'000','000','N','D','50040','0D2','0D2 GM','50040','0D2','0D2 GM','50040','0D2','0D2 GM','','','','','','','','','','','','','0000000000010.36','201','01','N','OH','OH','N','45429-3527','','','','','0D2','00000','00000','JAMES G. M','3953','47804M878','N',0,'Y','N','','N','3','UBS FINANCIAL SERVICES INC. FBO','JOAN ULLOTH','3205 WALTHAM AVE','3','','','KETTERING','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. James Gregory Mayeux','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','201','N',75,'005','P',null,'N',null,'2020-03-15 12:16:03.0','2020-03-15');
--filter_status_cd = 'NS'
Insert into sdh_asset_position partition(process_date) values (31017221,'250496',403074,null,null,290128,null,null,1145,null,null,null,null,403074,963073,511688,null,null,6136.23,null,'S','2017-07-31 00:00:00.0','9JR 52875 1716',null,'ASHWIN BRANDECKER',null,'47804A130',null,561.01,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1809994,'3563','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0JR','0JR DS','50040','0JR','0JR DS','50040','0JR','0JR DS','','','','','','','','','','','','','0000000000010.92','201','01','N','HI','HI','N','96825-2622','','32050','','','0JR','00000','00000','DEF REP','3563','47804A130','N',0,'Y','N','','N','3','UBS FINANCIAL SERVICES INC. FBO','RACIE G.N.H. BOTELHO','1054-A AWAWAMALU ST','3','','','HONOLULU','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','John/Daniel Miyamasu/Shiu','','R','NS','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','201','N',75,'005','M','250496','N',null,'2020-03-15 12:16:03.0','2020-03-15');
--stat_cd = 'D'
Insert into sdh_asset_position partition(process_date) values (31017222,'250496',403074,null,null,290128,null,null,1148,null,null,null,null,403074,963073,511688,null,null,6226.23,null,'S','2017-07-31 00:00:00.0','9JR 52875 1716',null,'ASHWIN BRANDECKER',null,'47804A130',null,561.01,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1809994,'3563','UBS-DSP','DST',' ','',null,'000','000','N','D','50040','0JR','0JR DS','50040','0JR','0JR DS','50040','0JR','0JR DS','','','','','','','','','','','','','0000000000010.92','201','01','N','HI','HI','N','96825-2622','','32050','','','0JR','00000','00000','DEF REP','3563','47804A130','N',0,'Y','N','','N','3','UBS FINANCIAL SERVICES INC. FBO','RACIE G.N.H. BOTELHO','1054-A AWAWAMALU ST','3','','','HONOLULU','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','John/Daniel Miyamasu/Shiu','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','201','N',75,'005','M','250496','N',null,'2020-03-15 12:16:03.0','2020-03-15');
--eff_dta='2020-03-15 00:00:00.0' (col22)
Insert into sdh_asset_position partition(process_date) values (31017223,'250496',403074,null,null,290128,null,null,1149,null,null,null,null,403074,963073,511688,null,null,6128.23,null,'S','2020-03-15 00:00:00.0','9JR 52875 1716',null,'ASHWIN BRANDECKER',null,'47804A130',null,561.01,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1809994,'3563','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0JR','0JR DS','50040','0JR','0JR DS','50040','0JR','0JR DS','','','','','','','','','','','','','0000000000010.92','201','01','N','HI','HI','N','96825-2622','','32050','','','0JR','00000','00000','DEF REP','3563','47804A130','N',0,'Y','N','','N','3','UBS FINANCIAL SERVICES INC. FBO','RACIE G.N.H. BOTELHO','1054-A AWAWAMALU ST','3','','','HONOLULU','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','John/Daniel Miyamasu/Shiu','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','201','N',75,'005','M','250496','N',null,'2020-03-15 12:16:03.0','2020-03-15');
----aum_amt = 5888.99(col 19)
Insert into sdh_asset_position partition(process_date) values (31017206,'250987',403074,null,null,289792,null,null,1152,null,null,null,null,403074,755642,511538,null,null,5888.99,null,'S','2017-07-31 00:00:00.0','9SO 61239 9716',null,'ASHWIN BRANDECKER',null,'47803T627',null,1179.042,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,3076178,'487','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0SO','0SO 87','50040','0SO','0SO 87','50040','0SO','0SO 87','','','','','','','','','','','','','0000000000025.56','408','28','N','CA','CA','N','95831-5799','','','','','0SO','00000','00000','STEVEN MUE','487','47803T627','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','WILLIAM GARCIA','ROLLOVER IRA','4','7754 OAK BAY CIRCLE','','SACRAMENTO','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. Steven Mueller','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','408','N',75,'880',null,null,'N',null,'2020-03-15 12:16:03.0','2020-03-15');

--existing
Insert into sdh_asset_position partition(process_date) values (31017200,'250123',403074,null,null,290144,null,null,1140,null,null,null,null,403074,609480,511693,null,null,0,null,'S','2017-07-31 00:00:00.0','9D5 57721 4269',null,'ASHWIN BRANDECKER',null,'47804M878',null,0,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,2488847,'3953','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0D2','0D2 GM','50040','0D2','0D2 GM','50040','0D2','0D2 GM','','','','','','','','','','','','','0000000000010.36','201','01','N','OH','OH','N','45429-3527','','','','','0D2','00000','00000','JAMES G. M','3953','47804M878','N',0,'Y','N','','N','3','UBS FINANCIAL SERVICES INC. FBO','JOAN ULLOTH','3205 WALTHAM AVE','3','','','KETTERING','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. James Gregory Mayeux','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','201','N',75,'005',null,null,'N',null,'2020-03-15 23:16:03.0','2020-03-15');
Insert into sdh_asset_position partition(process_date) values (31017207,'250987',403074,null,null,289792,null,null,1153,null,null,null,null,403074,755642,511538,null,null,30136.31,null,'S','2017-07-31 00:00:00.0','9SO 61239 9716',null,'ASHWIN BRANDECKER',null,'47803T627',null,1179.042,null,null,null,null,null,'',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,3076178,'487','UBS-DSP','DST',' ','',null,'000','000','N','R','50040','0SO','0SO 87','50040','0SO','0SO 87','50040','0SO','0SO 87','','','','','','','','','','','','','0000000000025.56','408','28','N','CA','CA','N','95831-5799','','','','','0SO','00000','00000','STEVEN MUE','487','47803T627','N',0,'Y','N','','N','4','UBS FINANCIAL SERVICES INC. FBO','WILLIAM GARCIA','ROLLOVER IRA','4','7754 OAK BAY CIRCLE','','SACRAMENTO','','','',null,null,'','','',null,null,null,null,null,null,null,null,'','','','',0,'',' ',' ','','',0,'',' ',' ',null,null,null,null,null,null,'','','','','','Mr. Steven Mueller','','R','ZZ','','2017-07-01 00:00:00.0','','5000001001','2017-07-01 00:00:00.0','5000001001','2017-07-01 00:00:00.0','408','N',75,'880',null,null,'N',null,'2020-03-15 23:16:03.0','2020-03-15');

select  dh_asset_id, process_timestamp
from    (
        select  *,
                row_number() over (partition by dh_asset_id order by process_timestamp  desc)    as rn
        from    inv_jhi_typed_qa.sdh_asset_position     s
        where   dh_asset_id  in (31017200,31017207)
        ) a
where   a.rn = 1