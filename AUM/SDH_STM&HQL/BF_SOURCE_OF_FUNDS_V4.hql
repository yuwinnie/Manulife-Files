--Natural Keys valiation - NoNull //should return no record
select  count(*)
from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_source_of_funds
where   src = 'JHISDH'
and     effective_ts is null;


INFO  : Session is already open
INFO  : Dag name: select  count(*)
from    inv_curated_...null(Stage-1)
INFO  : Status: Running (Executing on YARN cluster with App id application_1584192168763_2866)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      3          3        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 9.54 s
--------------------------------------------------------------------------------
+------+--+
| _c0  |
+------+--+
| 0    |
+------+--+
1 row selected (10.396 seconds)



--Natural Keys valiation - NoDuplicate //should return no record
select  distributor_uid,
        liability_portf_uid,
        effective_ts,
        count(*)
from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_source_of_funds
where   src = 'JHISDH'
and     current_version = 'Y'
group by distributor_uid,
        liability_portf_uid,
        effective_ts
having  count(*) >1;


INFO  : Session is already open
INFO  : Dag name: select  distributor_uid,
        liabil...>1(Stage-1)
INFO  : Status: Running (Executing on YARN cluster with App id application_1584192168763_2847)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      3          3        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 5.57 s
--------------------------------------------------------------------------------
+------------------+----------------------+---------------+------+--+
| distributor_uid  | liability_portf_uid  | effective_ts  | _c3  |
+------------------+----------------------+---------------+------+--+
+------------------+----------------------+---------------+------+--+
No rows selected (6.338 seconds)

--Mandatory Fields Validation //should return no record
select  count(*)
from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_source_of_funds
where   src = 'JHISDH'
and     (valuation_id is null
or      effective_ts is null
or      sys_eff_date_from is null
or      sys_eff_date_to is null
or      distributor_key is null
or      orig_currency_code is null
or      edl_created_ts is null
or      current_version is null
);

INFO  : Session is already open
INFO  : Dag name: select  count(*)
from    inv_curate...null
)(Stage-1)
INFO  : Status: Running (Executing on YARN cluster with App id application_1584192168763_2847)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      3          3        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 5.57 s
--------------------------------------------------------------------------------
+------+--+
| _c0  |
+------+--+
| 0    |
+------+--+
1 row selected (6.432 seconds)



--Null and hard coded Columns Validation //should return no record
select  count(*)
from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_source_of_funds
where   src = 'JHISDH'
and     (security_key is not null
or      src_security_id is not null
or      src_created_ts is not null
or      src_created_tz is not null
or      div_reinvested_orig <> 0
or      chg_due_to_fxrate_orig <> 0
or      asset_transfers_orig <> 0
or      misc_transfers_orig <> 0
or      net_valuation_orig <> 0
or      fx_rate_orig_cad <> 0
or      valuation_amt_cad <> 0
or      subscription_amt_cad <> 0
or      redemption_amt_cad <> 0
or      div_reinvested_cad <> 0
or      chg_due_to_market_cad <> 0
or      chg_due_to_fxrate_cad <> 0
or      transfers_in_cad <> 0
or      transfers_out_cad <> 0
or      asset_transfers_cad <> 0
or      misc_transfers_cad <> 0
or      net_valuation_cad <> 0
or      fx_rate_orig_usd <> 0
or      valuation_amt_usd <> 0
or      subscription_amt_usd <> 0
or      redemption_amt_usd <> 0
or      div_reinvested_usd <> 0
or      chg_due_to_market_usd <> 0
or      chg_due_to_fxrate_usd <> 0
or      transfers_in_usd <> 0
or      transfers_out_usd <> 0
or      asset_transfers_usd <> 0
or      misc_transfers_usd <> 0
or      net_valuation_usd <> 0
or      fx_rate_orig_hkd <> 0
or      valuation_amt_hkd <> 0
or      subscription_amt_hkd <> 0
or      redemption_amt_hkd <> 0
or      div_reinvested_hkd <> 0
or      chg_due_to_market_hkd <> 0
or      chg_due_to_fxrate_hkd <> 0
or      transfers_in_hkd <> 0
or      transfers_out_hkd <> 0
or      asset_transfers_hkd <> 0
or      misc_transfers_hkd <> 0
or      net_valuation_hkd <> 0
or      orig_currency_code <> 'USD'
);


INFO  : Session is already open
INFO  : Dag name: select  count(*)
from    inv_curat...'USD'
)(Stage-1)
INFO  : Status: Running (Executing on YARN cluster with App id application_1584192168763_2866)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      3          3        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 9.55 s
--------------------------------------------------------------------------------
+------+--+
| _c0  |
+------+--+
| 0    |
+------+--+
1 row selected (10.375 seconds)

--Curation query

--////////////////////////day1
    --Day1 Record count
with    CurrentCurationDateTime as (
        select  start_time,
                end_time,
                row_number () over (partition by job_name order by start_time desc) as rank
        from    inv_curated_eod_qa.curation_registry
        where   job_name = 'sdh_curation'
),    
increamentalRecords as (
        select  distinct 
                to_date(effective_dt) as effective_dt,
                nvl(trim(distributor_uid),'<NULL>')       as  distributor_uid,
                nvl(trim(liability_portf_uid),'<NULL>')   as  liability_portf_uid
        from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_investor_holding  h,
                CurrentCurationDateTime                 c
        where   h.current_version = 'Y'
        and     h.src = 'JHISDH' 
        and     h.balance_type = 'TRADE_DATE' 
        and     c.rank = 2
        and     h.edl_curated_ts > c.end_time
),
pre_source as (
        select  to_date(tar.effective_dt) as effective_dt,
                nvl(trim(tar.distributor_uid),'<NULL>')         as  distributor_uid,
                nvl(trim(tar.liability_portf_uid),'<NULL>')     as  liability_portf_uid,
                nvl(tar.valuation_amt_orig,0)       as  valuation_amt_orig,
                nvl(tar.subscription_amt_orig,0)    as  subscription_amt_orig,
                nvl(tar.redemption_amt_orig,0)      as  redemption_amt_orig,
                nvl(tar.transfers_in_orig,0)        as  transfers_in_orig,
                nvl(tar.transfers_out_orig,0)       as  transfers_out_orig
        from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_source_of_funds tar  inner join increamentalRecords inc
            on  tar.effective_dt = inc.effective_dt
            and nvl(trim(tar.distributor_uid),'<NULL>') = inc.distributor_uid
            and nvl(trim(tar.liability_portf_uid),'<NULL>') = inc.liability_portf_uid
),
source_holding  as(
        select  cast(h.effective_ts as timestamp)           as  effective_ts,
                to_date(h.effective_dt)                     as  effective_dt,
                case    when h.distributor_key is null and  h.liability_portfolio_key is null then  '<NULL>'
                else    trim(h.distributor_key)
                end     as  distributor_key,
                case    when h.distributor_key is null and  h.liability_portfolio_key is null then  '<NULL>'
                else    trim(h.liability_portfolio_key)
                end     as liability_portfolio_key,
                case    when h.distributor_uid is null and  h.liability_portf_uid is null then  '<NULL>'
                else    trim(h.distributor_uid)
                end     as  distributor_uid,
                case    when h.distributor_uid is null and  h.liability_portf_uid is null then  '<NULL>'
                else    trim(h.liability_portf_uid)
                end     as liability_portf_uid,
                case    when h.distributor_uid is null and  h.liability_portf_uid is null then  '<NULL>'
                else    '<NotNULL>'
                end     as flagn,
                h.settled_value_orig,
                cast(h.edl_created_ts as timestamp)         as  edl_created_ts
        from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_investor_holding h
        inner join  increamentalRecords                             i
            on      to_date(h.effective_dt) = i.effective_dt
            and     nvl(trim(h.distributor_uid),'<NULL>') = nvl(trim(i.distributor_uid),'<NULL>')
            and     nvl(trim(h.liability_portf_uid),'<NULL>') = nvl(trim(i.liability_portf_uid),'<NULL>')
            and     h.current_version = 'Y'
            and     h.src = 'JHISDH'
            and     h.balance_type = 'TRADE_DATE'
),
source_transaction  as(
        select  to_date(t.trade_date) as trade_date,
                case    when t.distributor_uid is null and  t.liability_portf_uid is null then  '<NULL>'
                else    trim(t.distributor_uid)
                end     as  distributor_uid,
                case    when t.distributor_uid is null and  t.liability_portf_uid is null then  '<NULL>'
                else    trim(t.liability_portf_uid)
                end     as liability_portf_uid,
                case    when t.distributor_uid is null and  t.liability_portf_uid is null then  '<NULL>'
                else    '<NotNULL>'
                end     as flagn,
                nvl(t.trade_amt,0) as trade_amt,
                upper(t.txn_type) as txn_type
        from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_investor_transaction  t
        inner join  increamentalRecords                 i
            on      to_date(t.trade_date) = i.effective_dt
            and     nvl(trim(t.distributor_uid),'<NULL>') = nvl(trim(i.distributor_uid),'<NULL>')
            and     nvl(trim(t.liability_portf_uid),'<NULL>') = nvl(trim(i.liability_portf_uid),'<NULL>')
            and     t.current_version = 'Y'
            and     t.src = 'JHISDH'
            and     t.reason_code = 'Y'
), ----18486
source  as (
        select  
                s.effective_ts,
                s.effective_dt,
                s.distributor_key,
                s.liability_portfolio_key,
                s.distributor_uid           as  distributor_uid,
                s.liability_portf_uid   as  liability_portf_uid,
                s.valuation_amt_orig, 
                --s.edl_created_ts,
                t.trade_amt,
                t.txn_type
        from    (
            select  distinct 
                    effective_ts,
                    effective_dt,
                    distributor_key,
                    liability_portfolio_key,
                    distributor_uid,
                    liability_portf_uid,
                    --edl_created_ts,
                    flagn,
                    sum(settled_value_orig) over (partition by effective_dt,distributor_uid, liability_portf_uid,flagn) as valuation_amt_orig                                          
            from    source_holding
        ) as s
        left outer join source_transaction as t
        on  s.effective_dt = t.trade_date
        and s.distributor_uid = t.distributor_uid
        and s.liability_portf_uid = t.liability_portf_uid
        and s.flagn = t.flagn
)
select 'bf_source_of_funds_Source' as tableName, count(*) from    source
union all
select  'bf_source_of_funds_Target' as tableName, count(*) 
from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_source_of_funds  t,
        CurrentCurationDateTime                                 c
where   src = 'JHISDH'
and     c.rank =1
and     t.edl_curated_ts >= c.start_time 
and     t.edl_curated_ts <= c.end_time 
and     t.current_version = 'Y';

--------------------------------------------------------------------------------
VERTICES: 17/17  [==========================>>] 100%  ELAPSED TIME: 12.18 s
--------------------------------------------------------------------------------
+----------------------------+----------+--+
|       _u1.tablename        | _u1._c1  |
+----------------------------+----------+--+
| bf_source_of_funds_Source  | 4        |
| bf_source_of_funds_Target  | 4        |
+----------------------------+----------+--+
2 rows selected (24.083 seconds)


    --Day1 data validation
with    CurrentCurationDateTime as (
        select  start_time,
                end_time,
                row_number () over (partition by job_name order by start_time desc) as rank
        from    inv_curated_eod_qa.curation_registry
        where   job_name = 'sdh_curation'
),    
increamentalRecords as (
        select  distinct 
                to_date(effective_dt) as effective_dt,
                nvl(trim(distributor_uid),'<NULL>')       as  distributor_uid,
                nvl(trim(liability_portf_uid),'<NULL>')   as  liability_portf_uid
        from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_investor_holding  h,
                CurrentCurationDateTime                 c
        where   h.current_version = 'Y'
        and     h.src = 'JHISDH' 
        and     h.balance_type = 'TRADE_DATE' 
        and     c.rank = 2
        and     h.edl_curated_ts > c.end_time
),
source_holding  as(
        select  cast(h.effective_ts as timestamp)           as  effective_ts,
                to_date(h.effective_dt)                     as  effective_dt,
                case    when h.distributor_key is null and  h.liability_portfolio_key is null then  '<NULL>'
                else    trim(h.distributor_key)
                end     as  distributor_key,
                case    when h.distributor_key is null and  h.liability_portfolio_key is null then  '<NULL>'
                else    trim(h.liability_portfolio_key)
                end     as liability_portfolio_key,
                case    when h.distributor_uid is null and  h.liability_portf_uid is null then  '<NULL>'
                else    trim(h.distributor_uid)
                end     as  distributor_uid,
                case    when h.distributor_uid is null and  h.liability_portf_uid is null then  '<NULL>'
                else    trim(h.liability_portf_uid)
                end     as liability_portf_uid,
                case    when h.distributor_uid is null and  h.liability_portf_uid is null then  '<NULL>'
                else    '<NotNULL>'
                end     as flagn,
                h.settled_value_orig,
                cast(h.edl_created_ts as timestamp)         as  edl_created_ts
        from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_investor_holding h
        inner join  increamentalRecords                             i
            on      to_date(h.effective_dt) = i.effective_dt
            and     nvl(trim(h.distributor_uid),'<NULL>') = nvl(trim(i.distributor_uid),'<NULL>')
            and     nvl(trim(h.liability_portf_uid),'<NULL>') = nvl(trim(i.liability_portf_uid),'<NULL>')
            and     h.current_version = 'Y'
            and     h.src = 'JHISDH'
            and     h.balance_type = 'TRADE_DATE'
),
source_transaction  as(
        select  to_date(t.trade_date) as trade_date,
                case    when t.distributor_uid is null and  t.liability_portf_uid is null then  '<NULL>'
                else    trim(t.distributor_uid)
                end     as  distributor_uid,
                case    when t.distributor_uid is null and  t.liability_portf_uid is null then  '<NULL>'
                else    trim(t.liability_portf_uid)
                end     as liability_portf_uid,
                case    when t.distributor_uid is null and  t.liability_portf_uid is null then  '<NULL>'
                else    '<NotNULL>'
                end     as flagn,
                nvl(t.trade_amt,0) as trade_amt,
                upper(t.txn_type) as txn_type
        from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_investor_transaction  t
        inner join  increamentalRecords                 i
            on      to_date(t.trade_date) = i.effective_dt
            and     nvl(trim(t.distributor_uid),'<NULL>') = nvl(trim(i.distributor_uid),'<NULL>')
            and     nvl(trim(t.liability_portf_uid),'<NULL>') = nvl(trim(i.liability_portf_uid),'<NULL>')
            and     t.current_version = 'Y'
            and     t.src = 'JHISDH'
            and     t.reason_code = 'Y'
), ----18486
source  as (
        select  distinct
                s.effective_ts,
                s.effective_dt,
                s.distributor_key,
                s.liability_portfolio_key,
                s.distributor_uid           as  distributor_uid,
                s.liability_portf_uid   as  liability_portf_uid,
                s.valuation_amt_orig,
                t.trade_amt,
                t.txn_type
        from    (
            select  distinct 
                    effective_ts,
                    effective_dt,
                    distributor_key,
                    liability_portfolio_key,
                    distributor_uid,
                    liability_portf_uid,
                    flagn,
                    sum(settled_value_orig) over (partition by effective_dt,distributor_uid, liability_portf_uid,flagn) as valuation_amt_orig                                          
            from    source_holding
        ) as s
        left outer join source_transaction as t
        on  s.effective_dt = t.trade_date
        and s.distributor_uid = t.distributor_uid
        and s.liability_portf_uid = t.liability_portf_uid
        and s.flagn = t.flagn
),
edlcreatedts as (
select  effective_dt,
        distributor_uid,
        liability_portf_uid,
        edl_created_ts
from    (
            select  to_date(effective_dt) as effective_dt,
                    nvl(trim(distributor_uid),'<NULL>')         as  distributor_uid,
                    nvl(trim(liability_portf_uid),'<NULL>')     as  liability_portf_uid,
                    edl_created_ts,
                    row_number() over(partition by effective_dt,distributor_uid,liability_portf_uid order by edl_created_ts asc) as rn
            from    source_holding
        ) a
where   a.rn = 1
),
source_dis as (
select  distinct 
        s.effective_ts,
        s.effective_dt,
        s.liability_portfolio_key,
        s.distributor_key,
        s.distributor_uid,
        s.liability_portf_uid,
        s.valuation_amt_orig
from    source s
),
source_aggre as (
select  effective_dt,
        distributor_uid,
        liability_portf_uid,
        txn_type,
        sum(trade_amt) as summ
from    source 
group by effective_dt,distributor_uid,liability_portf_uid,txn_type
),
subscription_amt_orig as (
select  effective_dt,
        distributor_uid,
        liability_portf_uid,
        summ    as subscription_amt_orig
from    source_aggre
where   txn_type = 'SUBSCRIPTION_AMT_ORIG'
),
redemption_amt_orig as (
select  effective_dt,
        distributor_uid,
        liability_portf_uid,
        summ    as redemption_amt_orig
from    source_aggre
where   txn_type = 'REDEMPTION_AMT_ORIG'
),
transfers_in_orig as (
select  effective_dt,
        distributor_uid,
        liability_portf_uid,
        summ    as transfers_in_orig
from    source_aggre
where   txn_type = 'TRANSFERS_IN_ORIG'
),
transfers_out_orig   as (
select  effective_dt,
        distributor_uid,
        liability_portf_uid,
        summ    as transfers_out_orig
from    source_aggre
where   txn_type = 'TRANSFERS_OUT_ORIG'
),
source_raw as (
select  s.effective_ts,
        s.effective_dt,
        s.liability_portfolio_key,
        s.distributor_key,
        s.distributor_uid,
        s.liability_portf_uid,
        s.valuation_amt_orig,
        a.subscription_amt_orig,
        r.redemption_amt_orig,
        tin.transfers_in_orig,
        tout.transfers_out_orig,
        e.edl_created_ts
from    source_dis s
left outer join subscription_amt_orig a
    on  s.effective_dt = a.effective_dt
    and s.distributor_uid = a.distributor_uid
    and s.liability_portf_uid = a.liability_portf_uid   
left outer join  redemption_amt_orig        r
    on  s.effective_dt = r.effective_dt
    and s.distributor_uid = r.distributor_uid
    and s.liability_portf_uid = r.liability_portf_uid
left outer join  transfers_in_orig          tin
    on  s.effective_dt = tin.effective_dt
    and s.distributor_uid = tin.distributor_uid
    and s.liability_portf_uid = tin.liability_portf_uid
left outer join  transfers_out_orig         tout
    on  s.effective_dt = tout.effective_dt
    and s.distributor_uid = tout.distributor_uid
    and s.liability_portf_uid = tout.liability_portf_uid
left outer join  edlcreatedts               e
        on  s.effective_dt = e.effective_dt
        and s.distributor_uid = e.distributor_uid
        and s.liability_portf_uid = e.liability_portf_uid
),
source_flag as (
        select  s.effective_ts              as  src_effective_ts,
                s.effective_dt              as  src_effective_dt,
                s.liability_portfolio_key   as  src_liability_portfolio_key,
                s.distributor_key           as  src_distributor_key,
                s.distributor_uid           as  src_distributor_uid,
                s.liability_portf_uid       as  src_liability_portf_uid,
                s.valuation_amt_orig        as  src_valuation_amt_orig,
                s.subscription_amt_orig     as  src_subscription_amt_orig,
                s.redemption_amt_orig       as  src_redemption_amt_orig,
                s.transfers_in_orig         as  src_transfers_in_orig,
                s.transfers_out_orig        as  src_transfers_out_orig,      
                s.edl_created_ts            as  src_edl_created_ts,
        
                t.effective_ts              as  tar_effective_ts,
                to_date(t.effective_dt)     as  tar_effective_dt,
                t.liability_portfolio_key   as  tar_liability_portfolio_key,
                t.distributor_key           as  tar_distributor_key,
                t.distributor_uid           as  tar_distributor_uid,
                t.liability_portf_uid       as  tar_liability_portf_uid,
                t.valuation_amt_orig        as  tar_valuation_amt_orig,
                t.subscription_amt_orig     as  tar_subscription_amt_orig,
                t.redemption_amt_orig       as  tar_redemption_amt_orig,
                t.transfers_in_orig         as  tar_transfers_in_orig,
                t.transfers_out_orig        as  tar_transfers_out_orig,
                t.edl_created_ts            as  tar_edl_created_ts,
                t.sys_eff_date_from         as  tar_sys_eff_date_from,
                t.sys_eff_date_to           as  tar_sys_eff_date_to,
                t.current_version           as  tar_current_version,
                
                case    when    t.effective_dt is null  then    'new'
                when    s.effective_dt is not null and t.effective_dt is not null   then    'updated'
                else                                            'existing'
                end                         as flag
        from    source_raw s   left outer join
        (   select *
            from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_bf_source_of_funds
            where   src = 'JHISDH'
            and     current_version = 'Y') t
        on      s.effective_dt = to_date(t.effective_dt)
        and     nvl(trim(s.distributor_uid),'<NULL>') = nvl(trim(t.distributor_uid),'<NULL>')
        and     nvl(trim(s.liability_portf_uid),'<NULL>') = nvl(trim(t.liability_portf_uid),'<NULL>')
where   s.effective_dt is not null
)select flag,count(*) from source_flag group by flag;
--------------------------------------------------------------------------------
VERTICES: 79/79  [==========================>>] 100%  ELAPSED TIME: 15.96 s
--------------------------------------------------------------------------------
+----------+------+--+
|   flag   | _c1  |
+----------+------+--+
| new      | 1    |
| updated  | 3    |
+----------+------+--+
2 rows selected (22.88 seconds)


with    CurrentCurationDateTime as (
        select  start_time,
                end_time,
                row_number () over (partition by job_name order by start_time desc) as rank
        from    inv_curated_eod_qa.curation_registry
        where   job_name = 'sdh_curation'
),    
increamentalRecords as (
        select  distinct 
                to_date(effective_dt) as effective_dt,
                nvl(trim(distributor_uid),'<NULL>')       as  distributor_uid,
                nvl(trim(liability_portf_uid),'<NULL>')   as  liability_portf_uid
        from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_investor_holding  h,
                CurrentCurationDateTime                 c
        where   h.current_version = 'Y'
        and     h.src = 'JHISDH' 
        and     h.balance_type = 'TRADE_DATE' 
        and     c.rank = 2
        and     h.edl_curated_ts > c.end_time
),
source_holding  as(
        select  cast(h.effective_ts as timestamp)           as  effective_ts,
                to_date(h.effective_dt)                     as  effective_dt,
                case    when h.distributor_key is null and  h.liability_portfolio_key is null then  '<NULL>'
                else    trim(h.distributor_key)
                end     as  distributor_key,
                case    when h.distributor_key is null and  h.liability_portfolio_key is null then  '<NULL>'
                else    trim(h.liability_portfolio_key)
                end     as liability_portfolio_key,
                case    when h.distributor_uid is null and  h.liability_portf_uid is null then  '<NULL>'
                else    trim(h.distributor_uid)
                end     as  distributor_uid,
                case    when h.distributor_uid is null and  h.liability_portf_uid is null then  '<NULL>'
                else    trim(h.liability_portf_uid)
                end     as liability_portf_uid,
                case    when h.distributor_uid is null and  h.liability_portf_uid is null then  '<NULL>'
                else    '<NotNULL>'
                end     as flagn,
                h.settled_value_orig,
                cast(h.edl_created_ts as timestamp)         as  edl_created_ts
        from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_investor_holding h
        inner join  increamentalRecords                             i
            on      to_date(h.effective_dt) = i.effective_dt
            and     nvl(trim(h.distributor_uid),'<NULL>') = nvl(trim(i.distributor_uid),'<NULL>')
            and     nvl(trim(h.liability_portf_uid),'<NULL>') = nvl(trim(i.liability_portf_uid),'<NULL>')
            and     h.current_version = 'Y'
            and     h.src = 'JHISDH'
            and     h.balance_type = 'TRADE_DATE'
),
source_transaction  as(
        select  to_date(t.trade_date) as trade_date,
                case    when t.distributor_uid is null and  t.liability_portf_uid is null then  '<NULL>'
                else    trim(t.distributor_uid)
                end     as  distributor_uid,
                case    when t.distributor_uid is null and  t.liability_portf_uid is null then  '<NULL>'
                else    trim(t.liability_portf_uid)
                end     as liability_portf_uid,
                case    when t.distributor_uid is null and  t.liability_portf_uid is null then  '<NULL>'
                else    '<NotNULL>'
                end     as flagn,
                nvl(t.trade_amt,0) as trade_amt,
                upper(t.txn_type) as txn_type
        from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_investor_transaction  t
        inner join  increamentalRecords                 i
            on      to_date(t.trade_date) = i.effective_dt
            and     nvl(trim(t.distributor_uid),'<NULL>') = nvl(trim(i.distributor_uid),'<NULL>')
            and     nvl(trim(t.liability_portf_uid),'<NULL>') = nvl(trim(i.liability_portf_uid),'<NULL>')
            and     t.current_version = 'Y'
            and     t.src = 'JHISDH'
            and     t.reason_code = 'Y'
), ----18486
source  as (
        select  distinct
                s.effective_ts,
                s.effective_dt,
                s.distributor_key,
                s.liability_portfolio_key,
                s.distributor_uid           as  distributor_uid,
                s.liability_portf_uid   as  liability_portf_uid,
                s.valuation_amt_orig,
                t.trade_amt,
                t.txn_type
        from    (
            select  distinct 
                    effective_ts,
                    effective_dt,
                    distributor_key,
                    liability_portfolio_key,
                    distributor_uid,
                    liability_portf_uid,
                    flagn,
                    sum(settled_value_orig) over (partition by effective_dt,distributor_uid, liability_portf_uid,flagn) as valuation_amt_orig                                          
            from    source_holding
        ) as s
        left outer join source_transaction as t
        on  s.effective_dt = t.trade_date
        and s.distributor_uid = t.distributor_uid
        and s.liability_portf_uid = t.liability_portf_uid
        and s.flagn = t.flagn
),
edlcreatedts as (
select  effective_dt,
        distributor_uid,
        liability_portf_uid,
        edl_created_ts
from    (
            select  to_date(effective_dt) as effective_dt,
                    nvl(trim(distributor_uid),'<NULL>')         as  distributor_uid,
                    nvl(trim(liability_portf_uid),'<NULL>')     as  liability_portf_uid,
                    edl_created_ts,
                    row_number() over(partition by effective_dt,distributor_uid,liability_portf_uid order by edl_created_ts asc) as rn
            from    source_holding
        ) a
where   a.rn = 1
),
source_dis as (
select  distinct 
        s.effective_ts,
        s.effective_dt,
        s.liability_portfolio_key,
        s.distributor_key,
        s.distributor_uid,
        s.liability_portf_uid,
        s.valuation_amt_orig
from    source s
),
source_aggre as (
select  effective_dt,
        distributor_uid,
        liability_portf_uid,
        txn_type,
        sum(trade_amt) as summ
from    source 
group by effective_dt,distributor_uid,liability_portf_uid,txn_type
),
subscription_amt_orig as (
select  effective_dt,
        distributor_uid,
        liability_portf_uid,
        summ    as subscription_amt_orig
from    source_aggre
where   txn_type = 'SUBSCRIPTION_AMT_ORIG'
),
redemption_amt_orig as (
select  effective_dt,
        distributor_uid,
        liability_portf_uid,
        summ    as redemption_amt_orig
from    source_aggre
where   txn_type = 'REDEMPTION_AMT_ORIG'
),
transfers_in_orig as (
select  effective_dt,
        distributor_uid,
        liability_portf_uid,
        summ    as transfers_in_orig
from    source_aggre
where   txn_type = 'TRANSFERS_IN_ORIG'
),
transfers_out_orig   as (
select  effective_dt,
        distributor_uid,
        liability_portf_uid,
        summ    as transfers_out_orig
from    source_aggre
where   txn_type = 'TRANSFERS_OUT_ORIG'
),
source_raw as (
select  s.effective_ts,
        s.effective_dt,
        s.liability_portfolio_key,
        s.distributor_key,
        s.distributor_uid,
        s.liability_portf_uid,
        s.valuation_amt_orig,
        a.subscription_amt_orig,
        r.redemption_amt_orig,
        tin.transfers_in_orig,
        tout.transfers_out_orig,
        e.edl_created_ts
from    source_dis s
left outer join subscription_amt_orig a
    on  s.effective_dt = a.effective_dt
    and s.distributor_uid = a.distributor_uid
    and s.liability_portf_uid = a.liability_portf_uid   
left outer join  redemption_amt_orig        r
    on  s.effective_dt = r.effective_dt
    and s.distributor_uid = r.distributor_uid
    and s.liability_portf_uid = r.liability_portf_uid
left outer join  transfers_in_orig          tin
    on  s.effective_dt = tin.effective_dt
    and s.distributor_uid = tin.distributor_uid
    and s.liability_portf_uid = tin.liability_portf_uid
left outer join  transfers_out_orig         tout
    on  s.effective_dt = tout.effective_dt
    and s.distributor_uid = tout.distributor_uid
    and s.liability_portf_uid = tout.liability_portf_uid
left outer join  edlcreatedts               e
        on  s.effective_dt = e.effective_dt
        and s.distributor_uid = e.distributor_uid
        and s.liability_portf_uid = e.liability_portf_uid
),
source_flag as (
        select  s.effective_ts              as  src_effective_ts,
                s.effective_dt              as  src_effective_dt,
                s.liability_portfolio_key   as  src_liability_portfolio_key,
                s.distributor_key           as  src_distributor_key,
                s.distributor_uid           as  src_distributor_uid,
                s.liability_portf_uid       as  src_liability_portf_uid,
                s.valuation_amt_orig        as  src_valuation_amt_orig,
                s.subscription_amt_orig     as  src_subscription_amt_orig,
                s.redemption_amt_orig       as  src_redemption_amt_orig,
                s.transfers_in_orig         as  src_transfers_in_orig,
                s.transfers_out_orig        as  src_transfers_out_orig,      
                s.edl_created_ts            as  src_edl_created_ts,
        
                t.effective_ts              as  tar_effective_ts,
                to_date(t.effective_dt)     as  tar_effective_dt,
                t.liability_portfolio_key   as  tar_liability_portfolio_key,
                t.distributor_key           as  tar_distributor_key,
                t.distributor_uid           as  tar_distributor_uid,
                t.liability_portf_uid       as  tar_liability_portf_uid,
                t.valuation_amt_orig        as  tar_valuation_amt_orig,
                t.subscription_amt_orig     as  tar_subscription_amt_orig,
                t.redemption_amt_orig       as  tar_redemption_amt_orig,
                t.transfers_in_orig         as  tar_transfers_in_orig,
                t.transfers_out_orig        as  tar_transfers_out_orig,
                t.edl_created_ts            as  tar_edl_created_ts,
                t.sys_eff_date_from         as  tar_sys_eff_date_from,
                t.sys_eff_date_to           as  tar_sys_eff_date_to,
                t.current_version           as  tar_current_version,
                
                case    when    t.effective_dt is null  then    'new'
                when    s.effective_dt is not null and t.effective_dt is not null   then    'updated'
                else                                            'existing'
                end                         as flag
        from    source_raw s   left outer join
        (   select *
            from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_bf_source_of_funds
            where   src = 'JHISDH'
            and     current_version = 'Y') t
        on      s.effective_dt = to_date(t.effective_dt)
        and     nvl(trim(s.distributor_uid),'<NULL>') = nvl(trim(t.distributor_uid),'<NULL>')
        and     nvl(trim(s.liability_portf_uid),'<NULL>') = nvl(trim(t.liability_portf_uid),'<NULL>')
where   s.effective_dt is not null
),
source_master as(
-------------new records-------------
select  src_effective_ts            as  effective_ts,
        src_effective_dt            as  effective_dt,
        src_liability_portfolio_key as  liability_portfolio_key,
        src_distributor_key         as  distributor_key,
        src_distributor_uid         as  distributor_uid,
        src_liability_portf_uid     as  liability_portf_uid,
        src_valuation_amt_orig      as  valuation_amt_orig,
        src_subscription_amt_orig   as  subscription_amt_orig,
        src_redemption_amt_orig     as  redemption_amt_orig,
        src_transfers_in_orig       as  transfers_in_orig,
        src_transfers_out_orig      as  transfers_out_orig,
        cast('1980-01-01 00:00:00.0' as timestamp)     as     sys_eff_date_from,
        cast('2099-12-31 00:00:00.0' as timestamp)     as     sys_eff_date_to,  
        src_edl_created_ts          as  edl_created_ts,  
        'Y'                         as  current_version,
        flag
from    source_flag
where   flag = 'new'

union all
-------------updated records / new inserted-------------
select  src_effective_ts            as  effective_ts,
        src_effective_dt            as  effective_dt,
        src_liability_portfolio_key as  liability_portfolio_key,
        src_distributor_key         as  distributor_key,
        src_distributor_uid         as  distributor_uid,
        src_liability_portf_uid     as  liability_portf_uid,
        src_valuation_amt_orig      as  valuation_amt_orig,
        src_subscription_amt_orig   as  subscription_amt_orig,
        src_redemption_amt_orig     as  redemption_amt_orig,
        src_transfers_in_orig       as  transfers_in_orig,
        src_transfers_out_orig      as  transfers_out_orig,
        cast(concat(to_date(c.start_time),' 00:00:00') as timestamp)   as  sys_eff_date_from,
        cast('2099-12-31 00:00:00.0' as timestamp)  as  sys_eff_date_to,
        src_edl_created_ts          as  edl_created_ts,  
        'Y'                         as  current_version,
        flag    
from    source_flag,
        CurrentCurationDateTime c
where   flag = 'updated'
and     c.rank =1

union all
-------------updated records / Expired records------------- 
select  tar_effective_ts            as  effective_ts,
        tar_effective_dt            as  effective_dt,
        tar_liability_portfolio_key as  liability_portfolio_key,
        tar_distributor_key         as  distributor_key,
        tar_distributor_uid         as  distributor_uid,
        tar_liability_portf_uid     as  liability_portf_uid,
        tar_valuation_amt_orig      as  valuation_amt_orig,
        tar_subscription_amt_orig   as  subscription_amt_orig,
        tar_redemption_amt_orig     as  redemption_amt_orig,
        tar_transfers_in_orig       as  transfers_in_orig,
        tar_transfers_out_orig      as  transfers_out_orig,
        tar_sys_eff_date_from       as  sys_eff_date_from,
        cast(concat(to_date(c.start_time),' 00:00:00') as timestamp)   as  sys_eff_date_to,
        tar_edl_created_ts          as  edl_created_ts,
        'N'                         as  current_version,
        flag    
from    source_flag,
        CurrentCurationDateTime c
where   flag = 'updated'
and     c.rank =1
)
select  distinct flag,current_version, 
        count(*) over (partition by flag,current_version ) as flag_cnt
from    source_master;

--------------------------------------------------------------------------------
VERTICES: 240/240  [==========================>>] 100%  ELAPSED TIME: 20.84 s
--------------------------------------------------------------------------------
+----------+------------------+-----------+--+
|   flag   | current_version  | flag_cnt  |
+----------+------------------+-----------+--+
| new      | Y                | 1         |
| updated  | N                | 3         |
| updated  | Y                | 3         |
+----------+------------------+-----------+--+
3 rows selected (39.955 seconds)

with    CurrentCurationDateTime as (
        select  start_time,
                end_time,
                row_number () over (partition by job_name order by start_time desc) as rank
        from    inv_curated_eod_qa.curation_registry
        where   job_name = 'sdh_curation'
),    
increamentalRecords as (
        select  distinct 
                to_date(effective_dt) as effective_dt,
                nvl(trim(distributor_uid),'<NULL>')       as  distributor_uid,
                nvl(trim(liability_portf_uid),'<NULL>')   as  liability_portf_uid
        from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_investor_holding  h,
                CurrentCurationDateTime                 c
        where   h.current_version = 'Y'
        and     h.src = 'JHISDH' 
        and     h.balance_type = 'TRADE_DATE' 
        and     c.rank = 2
        and     h.edl_curated_ts > c.end_time
),
source_holding  as(
        select  cast(h.effective_ts as timestamp)           as  effective_ts,
                to_date(h.effective_dt)                     as  effective_dt,
                case    when h.distributor_key is null and  h.liability_portfolio_key is null then  '<NULL>'
                else    trim(h.distributor_key)
                end     as  distributor_key,
                case    when h.distributor_key is null and  h.liability_portfolio_key is null then  '<NULL>'
                else    trim(h.liability_portfolio_key)
                end     as liability_portfolio_key,
                case    when h.distributor_uid is null and  h.liability_portf_uid is null then  '<NULL>'
                else    trim(h.distributor_uid)
                end     as  distributor_uid,
                case    when h.distributor_uid is null and  h.liability_portf_uid is null then  '<NULL>'
                else    trim(h.liability_portf_uid)
                end     as liability_portf_uid,
                case    when h.distributor_uid is null and  h.liability_portf_uid is null then  '<NULL>'
                else    '<NotNULL>'
                end     as flagn,
                h.settled_value_orig,
                cast(h.edl_created_ts as timestamp)         as  edl_created_ts
        from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_investor_holding h
        inner join  increamentalRecords                             i
            on      to_date(h.effective_dt) = i.effective_dt
            and     nvl(trim(h.distributor_uid),'<NULL>') = nvl(trim(i.distributor_uid),'<NULL>')
            and     nvl(trim(h.liability_portf_uid),'<NULL>') = nvl(trim(i.liability_portf_uid),'<NULL>')
            and     h.current_version = 'Y'
            and     h.src = 'JHISDH'
            and     h.balance_type = 'TRADE_DATE'
),
source_transaction  as(
        select  to_date(t.trade_date) as trade_date,
                case    when t.distributor_uid is null and  t.liability_portf_uid is null then  '<NULL>'
                else    trim(t.distributor_uid)
                end     as  distributor_uid,
                case    when t.distributor_uid is null and  t.liability_portf_uid is null then  '<NULL>'
                else    trim(t.liability_portf_uid)
                end     as liability_portf_uid,
                case    when t.distributor_uid is null and  t.liability_portf_uid is null then  '<NULL>'
                else    '<NotNULL>'
                end     as flagn,
                nvl(t.trade_amt,0) as trade_amt,
                upper(t.txn_type) as txn_type
        from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_investor_transaction  t
        inner join  increamentalRecords                 i
            on      to_date(t.trade_date) = i.effective_dt
            and     nvl(trim(t.distributor_uid),'<NULL>') = nvl(trim(i.distributor_uid),'<NULL>')
            and     nvl(trim(t.liability_portf_uid),'<NULL>') = nvl(trim(i.liability_portf_uid),'<NULL>')
            and     t.current_version = 'Y'
            and     t.src = 'JHISDH'
            and     t.reason_code = 'Y'
), ----18486
source  as (
        select  distinct
                s.effective_ts,
                s.effective_dt,
                s.distributor_key,
                s.liability_portfolio_key,
                s.distributor_uid           as  distributor_uid,
                s.liability_portf_uid   as  liability_portf_uid,
                s.valuation_amt_orig,
                t.trade_amt,
                t.txn_type
        from    (
            select  distinct 
                    effective_ts,
                    effective_dt,
                    distributor_key,
                    liability_portfolio_key,
                    distributor_uid,
                    liability_portf_uid,
                    flagn,
                    sum(settled_value_orig) over (partition by effective_dt,distributor_uid, liability_portf_uid,flagn) as valuation_amt_orig                                          
            from    source_holding
        ) as s
        left outer join source_transaction as t
        on  s.effective_dt = t.trade_date
        and s.distributor_uid = t.distributor_uid
        and s.liability_portf_uid = t.liability_portf_uid
        and s.flagn = t.flagn
),
edlcreatedts as (
select  effective_dt,
        distributor_uid,
        liability_portf_uid,
        edl_created_ts
from    (
            select  to_date(effective_dt) as effective_dt,
                    nvl(trim(distributor_uid),'<NULL>')         as  distributor_uid,
                    nvl(trim(liability_portf_uid),'<NULL>')     as  liability_portf_uid,
                    edl_created_ts,
                    row_number() over(partition by effective_dt,distributor_uid,liability_portf_uid order by edl_created_ts asc) as rn
            from    source_holding
        ) a
where   a.rn = 1
),
source_dis as (
select  distinct 
        s.effective_ts,
        s.effective_dt,
        s.liability_portfolio_key,
        s.distributor_key,
        s.distributor_uid,
        s.liability_portf_uid,
        s.valuation_amt_orig
from    source s
),
source_aggre as (
select  effective_dt,
        distributor_uid,
        liability_portf_uid,
        txn_type,
        sum(trade_amt) as summ
from    source 
group by effective_dt,distributor_uid,liability_portf_uid,txn_type
),
subscription_amt_orig as (
select  effective_dt,
        distributor_uid,
        liability_portf_uid,
        summ    as subscription_amt_orig
from    source_aggre
where   txn_type = 'SUBSCRIPTION_AMT_ORIG'
),
redemption_amt_orig as (
select  effective_dt,
        distributor_uid,
        liability_portf_uid,
        summ    as redemption_amt_orig
from    source_aggre
where   txn_type = 'REDEMPTION_AMT_ORIG'
),
transfers_in_orig as (
select  effective_dt,
        distributor_uid,
        liability_portf_uid,
        summ    as transfers_in_orig
from    source_aggre
where   txn_type = 'TRANSFERS_IN_ORIG'
),
transfers_out_orig   as (
select  effective_dt,
        distributor_uid,
        liability_portf_uid,
        summ    as transfers_out_orig
from    source_aggre
where   txn_type = 'TRANSFERS_OUT_ORIG'
),
source_raw as (
select  s.effective_ts,
        s.effective_dt,
        s.liability_portfolio_key,
        s.distributor_key,
        s.distributor_uid,
        s.liability_portf_uid,
        s.valuation_amt_orig,
        a.subscription_amt_orig,
        r.redemption_amt_orig,
        tin.transfers_in_orig,
        tout.transfers_out_orig,
        e.edl_created_ts
from    source_dis s
left outer join subscription_amt_orig a
    on  s.effective_dt = a.effective_dt
    and s.distributor_uid = a.distributor_uid
    and s.liability_portf_uid = a.liability_portf_uid   
left outer join  redemption_amt_orig        r
    on  s.effective_dt = r.effective_dt
    and s.distributor_uid = r.distributor_uid
    and s.liability_portf_uid = r.liability_portf_uid
left outer join  transfers_in_orig          tin
    on  s.effective_dt = tin.effective_dt
    and s.distributor_uid = tin.distributor_uid
    and s.liability_portf_uid = tin.liability_portf_uid
left outer join  transfers_out_orig         tout
    on  s.effective_dt = tout.effective_dt
    and s.distributor_uid = tout.distributor_uid
    and s.liability_portf_uid = tout.liability_portf_uid
left outer join  edlcreatedts               e
        on  s.effective_dt = e.effective_dt
        and s.distributor_uid = e.distributor_uid
        and s.liability_portf_uid = e.liability_portf_uid
),
source_flag as (
        select  s.effective_ts              as  src_effective_ts,
                s.effective_dt              as  src_effective_dt,
                s.liability_portfolio_key   as  src_liability_portfolio_key,
                s.distributor_key           as  src_distributor_key,
                s.distributor_uid           as  src_distributor_uid,
                s.liability_portf_uid       as  src_liability_portf_uid,
                s.valuation_amt_orig        as  src_valuation_amt_orig,
                s.subscription_amt_orig     as  src_subscription_amt_orig,
                s.redemption_amt_orig       as  src_redemption_amt_orig,
                s.transfers_in_orig         as  src_transfers_in_orig,
                s.transfers_out_orig        as  src_transfers_out_orig,      
                s.edl_created_ts            as  src_edl_created_ts,
        
                t.effective_ts              as  tar_effective_ts,
                to_date(t.effective_dt)     as  tar_effective_dt,
                t.liability_portfolio_key   as  tar_liability_portfolio_key,
                t.distributor_key           as  tar_distributor_key,
                t.distributor_uid           as  tar_distributor_uid,
                t.liability_portf_uid       as  tar_liability_portf_uid,
                t.valuation_amt_orig        as  tar_valuation_amt_orig,
                t.subscription_amt_orig     as  tar_subscription_amt_orig,
                t.redemption_amt_orig       as  tar_redemption_amt_orig,
                t.transfers_in_orig         as  tar_transfers_in_orig,
                t.transfers_out_orig        as  tar_transfers_out_orig,
                t.edl_created_ts            as  tar_edl_created_ts,
                t.edl_curated_ts            as  tar_edl_curated_ts,
                t.sys_eff_date_from         as  tar_sys_eff_date_from,
                t.sys_eff_date_to           as  tar_sys_eff_date_to,
                t.current_version           as  tar_current_version,
                
                case    when    t.effective_dt is null  then    'new'
                when    s.effective_dt is not null and t.effective_dt is not null   then    'updated'
                else                                            'existing'
                end                         as flag
        from    source_raw s   left outer join
        (   select *
            from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_bf_source_of_funds
            where   src = 'JHISDH'
            and     current_version = 'Y') t
        on      s.effective_dt = to_date(t.effective_dt)
        and     nvl(trim(s.distributor_uid),'<NULL>') = nvl(trim(t.distributor_uid),'<NULL>')
        and     nvl(trim(s.liability_portf_uid),'<NULL>') = nvl(trim(t.liability_portf_uid),'<NULL>')
where   s.effective_dt is not null
),
source_master as(
-------------new records-------------
select  src_effective_ts            as  effective_ts,
        src_effective_dt            as  effective_dt,
        src_liability_portfolio_key as  liability_portfolio_key,
        src_distributor_key         as  distributor_key,
        src_distributor_uid         as  distributor_uid,
        src_liability_portf_uid     as  liability_portf_uid,
        src_valuation_amt_orig      as  valuation_amt_orig,
        src_subscription_amt_orig   as  subscription_amt_orig,
        src_redemption_amt_orig     as  redemption_amt_orig,
        src_transfers_in_orig       as  transfers_in_orig,
        src_transfers_out_orig      as  transfers_out_orig,
        cast('1980-01-01 00:00:00.0' as timestamp)     as     sys_eff_date_from,
        cast('2099-12-31 00:00:00.0' as timestamp)     as     sys_eff_date_to,  
        src_edl_created_ts          as  edl_created_ts,  
        src_edl_created_ts          as  edl_curated_ts,    
        'Y'                         as  current_version,
        flag
from    source_flag
where   flag = 'new'

union all
-------------updated records / new inserted-------------
select  src_effective_ts            as  effective_ts,
        src_effective_dt            as  effective_dt,
        src_liability_portfolio_key as  liability_portfolio_key,
        src_distributor_key         as  distributor_key,
        src_distributor_uid         as  distributor_uid,
        src_liability_portf_uid     as  liability_portf_uid,
        src_valuation_amt_orig      as  valuation_amt_orig,
        src_subscription_amt_orig   as  subscription_amt_orig,
        src_redemption_amt_orig     as  redemption_amt_orig,
        src_transfers_in_orig       as  transfers_in_orig,
        src_transfers_out_orig      as  transfers_out_orig,
        cast(concat(to_date(c.start_time),' 00:00:00') as timestamp)   as  sys_eff_date_from,
        cast('2099-12-31 00:00:00.0' as timestamp)  as  sys_eff_date_to,
        src_edl_created_ts          as  edl_created_ts,  
        src_edl_created_ts          as  edl_curated_ts,  
        'Y'                         as  current_version,
        flag    
from    source_flag,
        CurrentCurationDateTime c
where   flag = 'updated'
and     c.rank =1

union all
-------------updated records / Expired records------------- 
select  tar_effective_ts            as  effective_ts,
        tar_effective_dt            as  effective_dt,
        tar_liability_portfolio_key as  liability_portfolio_key,
        tar_distributor_key         as  distributor_key,
        tar_distributor_uid         as  distributor_uid,
        tar_liability_portf_uid     as  liability_portf_uid,
        tar_valuation_amt_orig      as  valuation_amt_orig,
        tar_subscription_amt_orig   as  subscription_amt_orig,
        tar_redemption_amt_orig     as  redemption_amt_orig,
        tar_transfers_in_orig       as  transfers_in_orig,
        tar_transfers_out_orig      as  transfers_out_orig,
        tar_sys_eff_date_from       as  sys_eff_date_from,
        cast(concat(to_date(c.start_time),' 00:00:00') as timestamp)   as  sys_eff_date_to,
        tar_edl_created_ts          as  edl_created_ts, 
        tar_edl_curated_ts          as  edl_curated_ts, 
        'N'                         as  current_version,
        flag    
from    source_flag,
        CurrentCurationDateTime c
where   flag = 'updated'
and     c.rank =1
)
select  count(*)
from    source_master source
where   source.flag ='updated' and source.current_version = 'N'
and     exists(
        select  *
        from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_source_of_funds   target
        where   target.src = 'JHISDH'
        and     source.edl_curated_ts = target.edl_curated_ts
        and     source.current_version = target.current_version
        and     source.effective_ts = target.effective_ts
        and     source.effective_dt = target.effective_dt        
        and     source.sys_eff_date_from = target.sys_eff_date_from 
        and     source.sys_eff_date_to = target.sys_eff_date_to
        and     nvl(trim(source.distributor_key),'<NULL>') = nvl(trim(target.distributor_key),'<NULL>')
        and     nvl(trim(source.liability_portfolio_key),'<NULL>') = nvl(trim(target.liability_portfolio_key),'<NULL>')
        and     nvl(trim(source.distributor_uid),'<NULL>') = nvl(trim(target.distributor_uid),'<NULL>')
        and     nvl(trim(source.liability_portf_uid),'<NULL>') = nvl(trim(target.liability_portf_uid),'<NULL>')
        and     nvl(source.valuation_amt_orig,0) = nvl(target.valuation_amt_orig,0)
        and     nvl(source.subscription_amt_orig,0) = nvl(target.subscription_amt_orig,0)
        and     nvl(source.redemption_amt_orig,0) = nvl(target.redemption_amt_orig,0)
        and     nvl(source.transfers_in_orig,0) = nvl(target.transfers_in_orig,0)
        and     nvl(source.transfers_out_orig,0) = nvl(target.transfers_out_orig,0)
        and     source.edl_created_ts = target.edl_created_ts);

--------------------------------------------------------------------------------
VERTICES: 240/240  [==========================>>] 100%  ELAPSED TIME: 22.52 s
--------------------------------------------------------------------------------
+------+--+
| _c0  |
+------+--+
| 3    |
+------+--+
1 row selected (53.399 seconds)
        
        
        
select  count(*)
from    source_master source
where   ((source.flag ='new' and source.current_version = 'Y') or    (source.flag ='updated' and source.current_version = 'Y'))
and     exists(
        select  *
        from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_source_of_funds   target,
                CurrentCurationDateTime                              c
        where   target.src = 'JHISDH'
        and     c.rank = 1
        and     target.edl_curated_ts >= c.start_time
        and     target.edl_curated_ts <= c.end_time
        and     source.current_version = target.current_version
        and     source.effective_ts = target.effective_ts
        and     source.effective_dt = target.effective_dt        
        and     source.sys_eff_date_from = target.sys_eff_date_from 
        and     source.sys_eff_date_to = target.sys_eff_date_to
        and     nvl(trim(source.distributor_key),'<NULL>') = nvl(trim(target.distributor_key),'<NULL>')
        and     nvl(trim(source.liability_portfolio_key),'<NULL>') = nvl(trim(target.liability_portfolio_key),'<NULL>')
        and     nvl(trim(source.distributor_uid),'<NULL>') = nvl(trim(target.distributor_uid),'<NULL>')
        and     nvl(trim(source.liability_portf_uid),'<NULL>') = nvl(trim(target.liability_portf_uid),'<NULL>')
        and     nvl(source.valuation_amt_orig,0) = nvl(target.valuation_amt_orig,0)
        and     nvl(source.subscription_amt_orig,0) = nvl(target.subscription_amt_orig,0)
        and     nvl(source.redemption_amt_orig,0) = nvl(target.redemption_amt_orig,0)
        and     nvl(source.transfers_in_orig,0) = nvl(target.transfers_in_orig,0)
        and     nvl(source.transfers_out_orig,0) = nvl(target.transfers_out_orig,0)
        and     source.edl_created_ts = target.edl_created_ts
      ) ;
--------------------------------------------------------------------------------
VERTICES: 242/242  [==========================>>] 100%  ELAPSED TIME: 18.58 s
--------------------------------------------------------------------------------
+------+--+
| _c0  |
+------+--+
| 4    |
+------+--+
1 row selected (40.195 seconds)

