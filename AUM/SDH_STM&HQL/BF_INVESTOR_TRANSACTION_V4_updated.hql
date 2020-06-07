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
source_raw as (
    select  *
    from    (
            select  *,
                    row_number () over (partition by trans_id order by process_timestamp desc) as rn
            from    inv_jhi_typed_qa.sdh_transaction s
            ) a
    where   a.rn = 1
    and     a.process_date ='2020-03-15'
),
source_raw_filter as(
    select  process_dt,
            cast(s.trans_id as string)          as trans_id,
            cast(firm_id as string)             as firm_id,
            nvl(trim(ta_acct_nbr), '<NULL>')    as ta_acct_nbr,
            cast(share_qty as decimal(32,8))    as share_qty,
            to_date(trade_dt)                   as trade_dt,
            nvl(trim(trade_class), '<NULL>')    as trade_class,
            cast(trn_amount as decimal(24,2))   as trn_amount,
            nvl(trim(stat_cd), '<NULL>')        as stat_cd,
            concat(nvl(s.label_id1,'NULL'),'_',nvl(s.label_id2,'NULL'),'_',nvl(s.label_id3,'NULL'),'_',nvl(s.label_id4,'NULL'),'_',nvl(s.label_id5,'NULL')) as transaction_note,
            prod_id,
            s.process_timestamp,
            case    when t.trans_id is null then 'N'
                    else                         'Y'
            end                                 as reason_code
    from    source_raw s left outer join 
            (
                select  cast(trans_id as string)    as trans_id
                from    source_raw t inner join product p                        
                        on t.prod_id = p.prod_id
                where   (upper(trim(t.part_trade_ind)) <> 'M' or t.part_trade_ind is null)
                and     (upper(trim(t.no_sales_rpt_flag)) <> 'Y' or t.no_sales_rpt_flag is null)    
                and     upper(trim(t.stat_cd)) <> 'D'
                and     t.rep_id not in (1445688, 1870105,1379603,1917045,1917059,1917069,1917078,
                                        1917173,1917298,1917299,1917300,1763852,1400601,1899863,1888320,1900083,
                                        1338205,1460849,1301941,1888679,666310,1917876,1917915,666309,1416535,1381732)
                and     (   t.label_id1 <> 'RR' and
                            t.label_id2 <> 'RR' and
                            t.label_id3 <> 'RR' and
                            t.label_id4 <> 'RR' and
                            t.label_id5 <> 'RR')
            ) t
    on      s.trans_id = t.trans_id
),
source as (        --raw source records
        select  cast(concat(to_date(s.process_dt),' 00:00:00') as timestamp) as execution_ts,
                cast(s.trans_id as string) as trans_id,
                case when   unix_timestamp(concat(s.trade_dt,' 00:00:00')) - unix_timestamp(d.bus_eff_date_from) >=  0 and
                            unix_timestamp(concat(s.trade_dt,' 00:00:00')) - unix_timestamp(d.bus_eff_date_to)   <=  0
                            then    nvl(trim(d.distributor_key),'11111111-1111-1111-1111-111111111111')
                else        '11111111-1111-1111-1111-111111111111'
                end                                                         as  distributor_key,            
                case when   unix_timestamp(concat(s.trade_dt,' 00:00:00')) - unix_timestamp(i.bus_eff_date_from) >=  0 and
                            unix_timestamp(concat(s.trade_dt,' 00:00:00')) - unix_timestamp(i.bus_eff_date_to)   <=  0
                    then    nvl(trim(i.account_key), dummya.account_key)
                else        dummya.account_key
                end                                                         as  account_key,
                case when   unix_timestamp(concat(s.trade_dt,' 00:00:00')) - unix_timestamp(d.bus_eff_date_from) >=  0 and
                            unix_timestamp(concat(s.trade_dt,' 00:00:00')) - unix_timestamp(d.bus_eff_date_to)   <=  0
                            then    nvl(trim(d.distributor_uid),dummyd.distributor_uid)
                else        dummyd.distributor_uid
                end                                                         as  distributor_uid,
                case when   unix_timestamp(concat(s.trade_dt,' 00:00:00')) - unix_timestamp(i.bus_eff_date_from) >=  0 and
                            unix_timestamp(concat(s.trade_dt,' 00:00:00')) - unix_timestamp(i.bus_eff_date_to)   <=  0
                    then    nvl(trim(i.account_uid), 'JHISDH_DUMMY')
                else        'JHISDH_DUMMY'
                end                                                         as  account_uid,
                s.share_qty,
                s.trade_dt,
                s.process_dt,
                r.target_value,
                s.trade_class,
                case when   s.share_qty = 0 or s.share_qty is null then 0
                    else    cast(s.trn_amount/s.share_qty as decimal(32,8))    
                end                                                         as trade_price,
                s.trn_amount,
                s.reason_code,
                s.transaction_note,
                s.liability_portfolio_key,
                nvl(trim(p.liability_portf_uid),'<NULL>')                   as  liability_portf_uid,
                s.process_timestamp,
                s.firm_id
        from    (
            select  a.*,
                    case when   unix_timestamp(concat(a.trade_dt,' 00:00:00')) - unix_timestamp(l.bus_eff_date_from) >=  0 and
                                unix_timestamp(concat(a.trade_dt,' 00:00:00')) - unix_timestamp(l.bus_eff_date_to)   <=  0
                        then    nvl(trim(l.liability_portfolio_key),'00000000-0000-0000-0000-000000000000')
                    else        '00000000-0000-0000-0000-000000000000'
                    end                                                 as  liability_portfolio_key
            from    source_raw_filter a
            left    outer join inv_curated_eod_qa.rel_liability_port_systems   l
                on  nvl(a.prod_id,'<NULL>') = nvl(trim(l.related_system_code),'<NULL>')
                and l.related_system_name = 'JHISDH')                   s
        left outer join inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bd_investor_account            i   --retrieve account_key/account_uid
            on  concat('JHISDH_',nvl(trim(s.ta_acct_nbr),'DUMMY')) = trim(i.account_uid)
            and i.current_version = 'Y'
        left outer join dummy_account                                   dummya
        left outer join (
                select  d.distributor_key,
                        r.distributor_uid,
                        r.src_id,
                        d.bus_eff_date_from,
                        d.bus_eff_date_to
                from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bd_distributor d inner join inv_curated_eod_qa.20200315_bkp_SDHDAY1_rel_distributor r 
                    on  trim(d.distributor_uid) = trim(r.distributor_uid)
                    and r.src = 'JHISDH'
                    and d.distributor_key not in ('9a202039-e5c2-43fc-8c97-f32a2a18ae19','cdc4bcbf-0a88-4592-9278-aa039240b173')
                        )                                               d
            on  trim(cast(s.firm_id as string)) = trim(d.src_id)
        left outer join dummy_distributor                               dummyd
        left outer join inv_curated_eod_qa.bd_liability_portfolio       p
            on  trim(s.liability_portfolio_key) = trim(p.liability_portfolio_key)
            and p.current_version = 'Y'
        left outer join inv_curated_eod_qa.ref_general_mapping  r--retrieve txn_type
                    on      nvl(trim(s.trade_class), '<NULL>') = nvl(trim(r.source_value), '<NULL>')  
                    and     r.classifier_code = 'JHI'
                    and     r.src = 'SDH'
                    and     r.current_version = 'Y'
),
source_flag        as (
select  s.process_dt                as  src_execution_ts,
        s.trans_id                  as  src_src_txn_id,
        s.distributor_key           as  src_distributor_key,
        s.account_key               as  src_account_key,
        s.distributor_uid           as  src_distributor_uid,
        s.account_uid               as  src_account_uid,
        s.share_qty                 as  src_trade_qty,
        s.trade_dt                  as  src_trade_date,
        to_date(s.process_dt)       as  src_settle_date,
        s.target_value              as  src_txn_type,
        s.trade_class               as  src_src_txn_type,
        s.trade_price               as  src_trade_price,
        s.trn_amount                as  src_trade_amt,
        s.trn_amount                as  src_settle_amt_orig,
        s.reason_code               as  src_reason_code,
        s.transaction_note          as  src_transaction_note,
        s.liability_portfolio_key   as  src_liability_portfolio_key,
        s.liability_portf_uid       as  src_liability_portf_uid,
        s.process_timestamp         as  src_process_timestamp,
        
        t.execution_ts              as  tar_execution_ts,
        t.src_txn_id                as  tar_src_txn_id,
        t.distributor_key           as  tar_distributor_key,
        t.account_key               as  tar_account_key,
        t.distributor_uid           as  tar_distributor_uid,
        t.account_uid               as  tar_account_uid,
        t.trade_qty                 as  tar_trade_qty,
        to_date(t.trade_date)       as  tar_trade_date,
        to_date(t.settle_date)      as  tar_settle_date,
        t.txn_type                  as  tar_txn_type,
        t.src_txn_type              as  tar_src_txn_type,
        t.trade_price               as  tar_trade_price,
        t.trade_amt                 as  tar_trade_amt,
        t.settle_amt_orig           as  tar_settle_amt_orig,
        t.reason_code               as  tar_reason_code,
        t.transaction_note          as  tar_transaction_note,
        t.liability_portfolio_key   as  tar_liability_portfolio_key,
        t.liability_portf_uid       as  tar_liability_portf_uid,
        t.sys_eff_date_from         as  tar_sys_eff_date_from,
        t.sys_eff_date_to           as  tar_sys_eff_date_to,
        t.edl_created_ts            as  tar_edl_created_ts,
        t.edl_curated_ts            as  tar_edl_curated_ts,
        t.current_version           as  tar_current_version,
        
        case    when    t.src_txn_id is null    then   'new'
                else                                    'updated'
        end                         as flag      
        
from    source s left outer join
    (   select  *
        from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_bf_investor_transaction t
        where   src = 'JHISDH'
        and     current_version = 'Y') t
on      nvl(trim(s.trans_id),  '<NULL>') = nvl(trim(t.src_txn_id),  '<NULL>') 
),
source_master    as (

-------------new records-------------
select  src_execution_ts            as  execution_ts,
        src_src_txn_id              as  src_txn_id,
        src_distributor_key         as  distributor_key,
        src_account_key             as  account_key,
        src_distributor_uid         as  distributor_uid,
        src_account_uid             as  account_uid,
        src_trade_qty               as  trade_qty,
        src_trade_date              as  trade_date,
        src_settle_date             as  settle_date,
        src_txn_type                as  txn_type,
        src_src_txn_type            as  src_txn_type,
        src_trade_price             as  trade_price,
        src_trade_amt               as  trade_amt,
        src_settle_amt_orig         as  settle_amt_orig,
        src_reason_code             as  reason_code,
        src_transaction_note        as  transaction_note,
        src_liability_portfolio_key as  liability_portfolio_key,
        src_liability_portf_uid     as  liability_portf_uid,    
        cast('1980-01-01 00:00:00.0' as timestamp)     as     sys_eff_date_from,
        cast('2099-12-31 00:00:00.0' as timestamp)     as     sys_eff_date_to,  
        src_process_timestamp       as  edl_created_ts, 
        src_process_timestamp       as  edl_curated_ts, --should be curation date
        'Y'                         as  current_version,
        flag
from    source_flag
where   flag = 'new'

union all

-------------Existing records / insert new records-------------
select  src_execution_ts            as  execution_ts,
        src_src_txn_id              as  src_txn_id,
        src_distributor_key         as  distributor_key,
        src_account_key             as  account_key,
        src_distributor_uid         as  distributor_uid,
        src_account_uid             as  account_uid,
        src_trade_qty               as  trade_qty,
        src_trade_date              as  trade_date,
        src_settle_date             as  settle_date,
        src_txn_type                as  txn_type,
        src_src_txn_type            as  src_txn_type,
        src_trade_price             as  trade_price,
        src_trade_amt               as  trade_amt,
        src_settle_amt_orig         as  settle_amt_orig,
        src_reason_code             as  reason_code,
        src_transaction_note        as  transaction_note,
        src_liability_portfolio_key as  liability_portfolio_key,
        src_liability_portf_uid     as  liability_portf_uid,    
        cast(concat(to_date(src_process_timestamp),' 00:00:00') as timestamp)   as  sys_eff_date_from,
        cast('2099-12-31 00:00:00.0' as timestamp)                              as  sys_eff_date_to,
        src_process_timestamp       as  edl_created_ts,
        src_process_timestamp       as  edl_curated_ts, --should be curation date
        'Y'                         as  current_version,
        flag    
from    source_flag
where   flag = 'updated' 

union all
-------------updated records / Expired records-------------       
select  tar_execution_ts            as  execution_ts,
        tar_src_txn_id              as  src_txn_id,
        tar_distributor_key         as  distributor_key,
        tar_account_key             as  account_key,
        tar_distributor_uid         as  distributor_uid,
        tar_account_uid             as  account_uid,
        tar_trade_qty               as  trade_qty,
        to_date(tar_trade_date)     as  trade_date,
        tar_settle_date             as  settle_date,
        tar_txn_type                as  txn_type,
        tar_src_txn_type            as  src_txn_type,
        tar_trade_price             as  trade_price,
        tar_trade_amt               as  trade_amt,
        tar_settle_amt_orig         as  settle_amt_orig,
        tar_reason_code             as  reason_code,
        tar_transaction_note        as  transaction_note,
        tar_liability_portfolio_key as  liability_portfolio_key,
        tar_liability_portf_uid     as  liability_portf_uid,
        tar_sys_eff_date_from       as  sys_eff_date_from,
        cast(concat(to_date(src_process_timestamp),' 00:00:00') as timestamp) as sys_eff_date_to,
        tar_edl_created_ts          as  edl_created_ts,
        tar_edl_curated_ts          as  edl_curated_ts,
        'N'                         as  current_version,
        flag    
from    source_flag
where   flag = 'updated'

union all
-------------existing records-------------    
select  tar_execution_ts            as  execution_ts,
        tar_src_txn_id              as  src_txn_id,
        tar_distributor_key         as  distributor_key,
        tar_account_key             as  account_key,
        tar_distributor_uid         as  distributor_uid,
        tar_account_uid             as  account_uid,
        tar_trade_qty               as  trade_qty,
        tar_trade_date              as  trade_date,
        tar_settle_date             as  settle_date,
        tar_txn_type                as  txn_type,
        tar_src_txn_type            as  src_txn_type,
        tar_trade_price             as  trade_price,
        tar_trade_amt               as  trade_amt,
        tar_settle_amt_orig         as  settle_amt_orig,
        tar_reason_code             as  reason_code,
        tar_transaction_note        as  transaction_note,
        tar_liability_portfolio_key as  liability_portfolio_key,
        tar_liability_portf_uid     as  liability_portf_uid,
        tar_sys_eff_date_from       as  sys_eff_date_from,
        tar_sys_eff_date_to         as  sys_eff_date_to,
        tar_edl_created_ts          as  edl_created_ts,
        tar_edl_curated_ts          as  edl_curated_ts,
        tar_current_version         as  current_version,
        flag    
from    source_flag
where   flag = 'existing'
)
select  reason_code,flag,count(*)
from    source_master where current_version = 'Y'
group by reason_code,flag;

+--------------+----------+------------------+------+--+
| reason_code  |   flag   | current_version  | _c3  |
+--------------+----------+------------------+------+--+
| Y            | updated  | Y                | 8    |
| Y            | updated  | N                | 8    |
+--------------+----------+------------------+------+--+
2 rows selected (51.637 seconds)


select  count(*)    
from    source_master   source
where   source.current_version = 'N' --new/updated records, 19 is expected
and     exists (
        select  *
        from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_investor_transaction  target
        where   target.src = 'JHISDH'
        and     target.current_version = 'N'
        and     nvl(trim(source.src_txn_id),  '<NULL>') = nvl(trim(target.src_txn_id),  '<NULL>')
        and     cast(source.execution_ts as timestamp) = cast(target.execution_ts as timestamp)
        and     cast(source.sys_eff_date_from as timestamp) = cast(target.sys_eff_date_from as timestamp)
        and     cast(source.sys_eff_date_to as timestamp) = cast(target.sys_eff_date_to as timestamp)
        and     nvl(trim(source.distributor_key),  '<NULL>') = nvl(trim(target.distributor_key),  '<NULL>')
        and     nvl(trim(source.account_key),'<NULL>') = nvl(trim(target.account_key),'<NULL>')
        and     nvl(trim(source.distributor_uid),'<NULL>') = nvl(trim(target.distributor_uid),'<NULL>')
        and     nvl(trim(source.account_uid),'<NULL>') = nvl(trim(target.account_uid),'<NULL>')
        and     nvl(source.trade_qty, 999999.99) = nvl(target.trade_qty, 999999.99)
        and     cast(source.trade_date as date) = cast(target.trade_date as date)
        and     cast(source.settle_date as date) = cast(target.settle_date as date)
        and     nvl(trim(source.txn_type),'<NULL>') = nvl(trim(target.txn_type),'<NULL>')
        and     nvl(trim(source.src_txn_type), '<NULL>') = nvl(trim(target.src_txn_type), '<NULL>')
        and     nvl(source.trade_price, 999999.99) = nvl(target.trade_price, 999999.99)
        and     nvl(source.trade_amt, 999999.99) = nvl(target.trade_amt, 999999.99)
        and     nvl(source.settle_amt_orig, 999999.99) = nvl(target.settle_amt_orig, 999999.99)
        and     nvl(trim(source.reason_code),'<NULL>') = nvl(trim(target.reason_code),'<NULL>')
        and     nvl(trim(source.transaction_note), '<NULL>') = nvl(trim(target.transaction_note), '<NULL>')
        and     target.settle_amt_hkd =0
        and     target.settle_amt_cad =0
        and     target.settle_amt_usd =0
        and     nvl(trim(source.liability_portfolio_key),'<NULL>') = nvl(trim(target.liability_portfolio_key),'<NULL>')
        and     nvl(trim(source.liability_portf_uid), '<NULL>') = nvl(trim(target.liability_portf_uid), '<NULL>')
        and     source.edl_created_ts = target.edl_created_ts
        and     source.edl_curated_ts = target.edl_curated_ts
        and     source.current_version = target.current_version);
--------------------------------------------------------------------------------
VERTICES: 78/78  [==========================>>] 100%  ELAPSED TIME: 53.51 s
--------------------------------------------------------------------------------
+------+--+
| _c0  |
+------+--+
| 8    |
+------+--+
1 row selected (72.799 seconds)



--select reason_code,flag,current_version, count(*) from source_master group by reason_code,flag,current_version order by flag, reason_code;
select  count(*) 
from    source_master   source
where   source.current_version = 'Y' --new/updated records, 19 is expected
and exists(
        select  *
        from    inv_curated_eod_qa.20200315_bkp_SDHDAY1_2_bf_investor_transaction  target,
                CurrentCurationDateTime                     c
        where   target.src = 'JHISDH'
        and     c.rank = 1
        and     target.edl_curated_ts >= c.start_time
        and     target.edl_curated_ts <= c.end_time
        and      nvl(trim(source.src_txn_id),  '<NULL>') = nvl(trim(target.src_txn_id),  '<NULL>')
        and     cast(source.execution_ts as timestamp) = cast(target.execution_ts as timestamp)
        and     cast(source.sys_eff_date_from as timestamp) = cast(target.sys_eff_date_from as timestamp)
        and     cast(source.sys_eff_date_to as timestamp) = cast(target.sys_eff_date_to as timestamp)
        and     nvl(trim(source.distributor_key),  '<NULL>') = nvl(trim(target.distributor_key),  '<NULL>')
        and     nvl(trim(source.account_key),'<NULL>') = nvl(trim(target.account_key),'<NULL>')
        and     nvl(trim(source.distributor_uid),'<NULL>') = nvl(trim(target.distributor_uid),'<NULL>')
        and     nvl(trim(source.account_uid),'<NULL>') = nvl(trim(target.account_uid),'<NULL>')
        and     nvl(source.trade_qty, 999999.99) = nvl(target.trade_qty, 999999.99)
        and     cast(source.trade_date as date) = cast(target.trade_date as date)
        and     cast(source.settle_date as date) = cast(target.settle_date as date)
        and     nvl(trim(source.txn_type),'<NULL>') = nvl(trim(target.txn_type),'<NULL>')
        and     nvl(trim(source.src_txn_type), '<NULL>') = nvl(trim(target.src_txn_type), '<NULL>')
        and     nvl(source.trade_price, 999999.99) = nvl(target.trade_price, 999999.99)
        and     nvl(source.trade_amt, 999999.99) = nvl(target.trade_amt, 999999.99)
        and     nvl(source.settle_amt_orig, 999999.99) = nvl(target.settle_amt_orig, 999999.99)
        and     nvl(trim(source.reason_code),'<NULL>') = nvl(trim(target.reason_code),'<NULL>')
        and     nvl(trim(source.transaction_note), '<NULL>') = nvl(trim(target.transaction_note), '<NULL>')
        and     target.settle_amt_hkd =0
        and     target.settle_amt_cad =0
        and     target.settle_amt_usd =0
        and     nvl(trim(source.liability_portfolio_key),'<NULL>') = nvl(trim(target.liability_portfolio_key),'<NULL>')
        and     nvl(trim(source.liability_portf_uid), '<NULL>') = nvl(trim(target.liability_portf_uid), '<NULL>')
        and     source.edl_created_ts = target.edl_created_ts
        and     source.current_version = target.current_version);  
--------------------------------------------------------------------------------
VERTICES: 80/80  [==========================>>] 100%  ELAPSED TIME: 51.69 s
--------------------------------------------------------------------------------
+------+--+
| _c0  |
+------+--+
| 8    |
+------+--+
1 row selected (62.708 seconds)
