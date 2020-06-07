set hive.vectorized.execution.enabled        =true;
set hive.execution.engine                    =tez;
set hive.vectorized.execution.reduce.enabled =true;
set hive.cbo.enable                          =true;
set hive.compute.query.using.stats           =true;
set hive.stats.fetch.column.stats            =true;
set hive.stats.fetch.partition.stats         =true;
set hive.exec.parallel                       =true;
set hive.optimize.bucketmapjoin              =true;
set hive.optimize.bucketmapjoin.sortedmerge  =true;
set hive.exec.compress.intermediate          =true;
set hive.exec.compress.output                =true;
set hive.support.quoted.identifiers          =none;
set hive.auto.convert.join                   =true;
set hive.exec.compress.output                =true;
set mapred.output.compression.type           =BLOCK;
set hive.exec.dynamic.partition              =true;
set hive.exec.dynamic.partition.mode         =nonstrict;
set hive.merge.mapfiles                      =false;
set tez.task.resource.memory.mb              =4096;
set hive.tez.container.size                  =8192;
set hive.tez.java.opts                       =-Xmx5120m;
set hive.optimize.ppd                        =true;
set hive.exec.reducers.bytes.per.reducer     =33554432;
set hive.optimize.sort.dynamic.partition     =true;
set hive.merge.mapredfiles                   =true;
set hive.stats.autogather                    =true;
set hive.exec.max.dynamic.partitions         =9999;
set hive.exec.max.dynamic.partitions.pernode =9999;

set hivevar:src='JHISDH';

CREATE TABLE IF NOT EXISTS ${CURATED_DB}.BF_SOURCE_OF_FUNDS
  (
    VALUATION_ID                             STRING,
    EFFECTIVE_TS                             TIMESTAMP,
    SYS_EFF_DATE_FROM                        TIMESTAMP,
    SYS_EFF_DATE_TO                          TIMESTAMP,
    LIABILITY_PORTFOLIO_KEY                  STRING,
    DISTRIBUTOR_KEY                          STRING,
    SECURITY_KEY                             STRING,
    DISTRIBUTOR_UID                          STRING,
    LIABILITY_PORTF_UID                      STRING,
    SRC_SECURITY_ID                          STRING,
    ORIG_CURRENCY_CODE                       STRING,
    VALUATION_AMT_ORIG                       DECIMAL(24,2),
    SUBSCRIPTION_AMT_ORIG                    DECIMAL(24,2),
    REDEMPTION_AMT_ORIG                      DECIMAL(24,2),
    DIV_REINVESTED_ORIG                      DECIMAL(24,2),
    CHG_DUE_TO_MARKET_ORIG                   DECIMAL(24,2),
    CHG_DUE_TO_FXRATE_ORIG                   DECIMAL(24,2),
    TRANSFERS_IN_ORIG                        DECIMAL(24,2),
    TRANSFERS_OUT_ORIG                       DECIMAL(24,2),
    ASSET_TRANSFERS_ORIG                     DECIMAL(24,2),
    MISC_TRANSFERS_ORIG                      DECIMAL(24,2),
    NET_VALUATION_ORIG                       DECIMAL(24,2),
    FX_RATE_ORIG_CAD                         DECIMAL(24,12),
    VALUATION_AMT_CAD                        DECIMAL(24,2),
    SUBSCRIPTION_AMT_CAD                     DECIMAL(24,2),
    REDEMPTION_AMT_CAD                       DECIMAL(24,2),
    DIV_REINVESTED_CAD                       DECIMAL(24,2),
    CHG_DUE_TO_MARKET_CAD                    DECIMAL(24,2),
    CHG_DUE_TO_FXRATE_CAD                    DECIMAL(24,2),
    TRANSFERS_IN_CAD                         DECIMAL(24,2),
    TRANSFERS_OUT_CAD                        DECIMAL(24,2),
    ASSET_TRANSFERS_CAD                      DECIMAL(24,2),
    MISC_TRANSFERS_CAD                       DECIMAL(24,2),
    NET_VALUATION_CAD                        DECIMAL(24,2),
    FX_RATE_ORIG_USD                         DECIMAL(24,12),
    VALUATION_AMT_USD                        DECIMAL(24,2),
    SUBSCRIPTION_AMT_USD                     DECIMAL(24,2),
    REDEMPTION_AMT_USD                       DECIMAL(24,2),
    DIV_REINVESTED_USD                       DECIMAL(24,2),
    CHG_DUE_TO_MARKET_USD                    DECIMAL(24,2),
    CHG_DUE_TO_FXRATE_USD                    DECIMAL(24,2),
    TRANSFERS_IN_USD                         DECIMAL(24,2),
    TRANSFERS_OUT_USD                        DECIMAL(24,2),
    ASSET_TRANSFERS_USD                      DECIMAL(24,2),
    MISC_TRANSFERS_USD                       DECIMAL(24,2),
    NET_VALUATION_USD                        DECIMAL(24,2),
    FX_RATE_ORIG_HKD                         DECIMAL(24,12),
    VALUATION_AMT_HKD                        DECIMAL(24,2),
    SUBSCRIPTION_AMT_HKD                     DECIMAL(24,2),
    REDEMPTION_AMT_HKD                       DECIMAL(24,2),
    DIV_REINVESTED_HKD                       DECIMAL(24,2),
    CHG_DUE_TO_MARKET_HKD                    DECIMAL(24,2),
    CHG_DUE_TO_FXRATE_HKD                    DECIMAL(24,2),
    TRANSFERS_IN_HKD                         DECIMAL(24,2),
    TRANSFERS_OUT_HKD                        DECIMAL(24,2),
    ASSET_TRANSFERS_HKD                      DECIMAL(24,2),
    MISC_TRANSFERS_HKD                       DECIMAL(24,2),
    NET_VALUATION_HKD                        DECIMAL(24,2),
    SRC_CREATED_TS                           TIMESTAMP,
    SRC_CREATED_TZ                           STRING,
    EDL_CREATED_TS                           TIMESTAMP,
    CURRENT_VERSION                          STRING,
    EDL_CURATED_TS                           TIMESTAMP
  )
PARTITIONED BY (EFFECTIVE_DT DATE, SRC STRING)
STORED AS ORC
;

DROP TABLE IF EXISTS ${TEMP_DB}.temp_bf_investor_holding_sof_jhisdh PURGE;

CREATE TABLE ${TEMP_DB}.temp_bf_investor_holding_sof_jhisdh
STORED AS ORC
TBLPROPERTIES ('auto.purge'='true')
AS
SELECT
    invhol.EFFECTIVE_TS,
    invhol.DISTRIBUTOR_KEY,
    invhol.DISTRIBUTOR_UID,
    invhol.LIABILITY_PORTFOLIO_KEY,
    invhol.LIABILITY_PORTF_UID,
    SUM(COALESCE(invhol.SETTLED_VALUE_ORIG,0))                                         AS SETTLED_VALUE_ORIG,
    MIN(invhol.EDL_CREATED_TS)                                                         AS EDL_CREATED_TS
FROM
    (
        SELECT DISTINCT
            hol.EFFECTIVE_TS,
            hol.DISTRIBUTOR_UID,
            hol.LIABILITY_PORTF_UID
        FROM ${CURATED_DB}.BF_INVESTOR_HOLDING hol
        WHERE hol.SRC = ${src}
            AND hol.EDL_CREATED_TS > ${LAST_CURATION_TS} AND hol.EDL_CREATED_TS <= ${CURATION_START_TIME}
            AND hol.CURRENT_VERSION = 'Y'
            AND BALANCE_TYPE = 'TRADE_DATE'
    ) X
INNER JOIN (SELECT * FROM ${CURATED_DB}.BF_INVESTOR_HOLDING WHERE SRC = ${src} AND CURRENT_VERSION = 'Y' AND BALANCE_TYPE = 'TRADE_DATE') invhol
    ON (X.EFFECTIVE_TS = invhol.EFFECTIVE_TS
        AND X.DISTRIBUTOR_UID <=> invhol.DISTRIBUTOR_UID
        AND X.LIABILITY_PORTF_UID <=> invhol.LIABILITY_PORTF_UID)
GROUP BY invhol.EFFECTIVE_TS, invhol.DISTRIBUTOR_KEY, invhol.DISTRIBUTOR_UID, invhol.LIABILITY_PORTFOLIO_KEY, invhol.LIABILITY_PORTF_UID
;

DROP TABLE IF EXISTS ${TEMP_DB}.temp_bf_investor_transaction_sof_jhisdh PURGE;

CREATE TABLE ${TEMP_DB}.temp_bf_investor_transaction_sof_jhisdh
STORED AS ORC
TBLPROPERTIES ('auto.purge'='true')
AS
SELECT
    txn.TRADE_DATE,
    hol.DISTRIBUTOR_UID,
    hol.LIABILITY_PORTF_UID,
    SUM(CASE WHEN txn.TXN_TYPE = 'SUBSCRIPTION_AMT_ORIG' THEN COALESCE(txn.TRADE_AMT,0) ELSE 0 END)
                                                                                       AS SUBSCRIPTION_AMT_ORIG,
    SUM(CASE WHEN txn.TXN_TYPE = 'REDEMPTION_AMT_ORIG' THEN COALESCE(txn.TRADE_AMT,0) ELSE 0 END)
                                                                                       AS REDEMPTION_AMT_ORIG,
    SUM(CASE WHEN txn.TXN_TYPE = 'TRANSFERS_IN_ORIG' THEN COALESCE(txn.TRADE_AMT,0) ELSE 0 END)
                                                                                       AS TRANSFERS_IN_ORIG,
    SUM(CASE WHEN txn.TXN_TYPE = 'TRANSFERS_OUT_ORIG' THEN COALESCE(txn.TRADE_AMT,0) ELSE 0 END)
                                                                                       AS TRANSFERS_OUT_ORIG
FROM ${TEMP_DB}.temp_bf_investor_holding_sof_jhisdh hol
    INNER JOIN (SELECT * FROM ${CURATED_DB}.BF_INVESTOR_TRANSACTION WHERE SRC = ${src} AND CURRENT_VERSION = 'Y' AND REASON_CODE = 'Y') txn
        ON (txn.DISTRIBUTOR_UID <=> hol.DISTRIBUTOR_UID
            AND txn.LIABILITY_PORTF_UID <=> hol.LIABILITY_PORTF_UID
            AND txn.TRADE_DATE = hol.EFFECTIVE_TS)
GROUP BY txn.TRADE_DATE, hol.DISTRIBUTOR_UID, hol.LIABILITY_PORTF_UID
;

DROP TABLE IF EXISTS ${TEMP_DB}.inc_bf_source_of_funds_jhisdh PURGE;

CREATE TABLE ${TEMP_DB}.inc_bf_source_of_funds_jhisdh
STORED AS ORC
TBLPROPERTIES ('auto.purge'='true')
AS
SELECT
    CAST(NULL AS STRING)                                                               AS VALUATION_ID,
    CAST(hol.EFFECTIVE_TS AS TIMESTAMP)                                                AS EFFECTIVE_TS,
    to_date(to_utc_timestamp(current_date,'America/Montreal'))                         AS SYS_EFF_DATE_FROM,
    CAST('2099-12-31 00:00:00' AS TIMESTAMP)                                           AS SYS_EFF_DATE_TO,
    CAST(hol.LIABILITY_PORTFOLIO_KEY AS STRING)                                        AS LIABILITY_PORTFOLIO_KEY,
    CAST(hol.DISTRIBUTOR_KEY AS STRING)                                                AS DISTRIBUTOR_KEY,
    CAST(NULL AS STRING)                                                               AS SECURITY_KEY,
    CAST(hol.DISTRIBUTOR_UID AS STRING)                                                AS DISTRIBUTOR_UID,
    CAST(hol.LIABILITY_PORTF_UID AS STRING)                                            AS LIABILITY_PORTF_UID,
    CAST(NULL AS STRING)                                                               AS SRC_SECURITY_ID,
    CAST('USD' AS STRING)                                                              AS ORIG_CURRENCY_CODE,
    CAST(hol.SETTLED_VALUE_ORIG AS DECIMAL(24,2))                                      AS VALUATION_AMT_ORIG,
    CAST(COALESCE(txn.SUBSCRIPTION_AMT_ORIG, 0) AS DECIMAL(24,2))                      AS SUBSCRIPTION_AMT_ORIG,
    CAST(COALESCE(txn.REDEMPTION_AMT_ORIG, 0) AS DECIMAL(24,2))                        AS REDEMPTION_AMT_ORIG,
    CAST(0 AS DECIMAL(24,2))                                                           AS DIV_REINVESTED_ORIG,
    CAST(NULL AS DECIMAL(24,2))                                                        AS CHG_DUE_TO_MARKET_ORIG,
    CAST(0 AS DECIMAL(24,2))                                                           AS CHG_DUE_TO_FXRATE_ORIG,
    CAST(COALESCE(txn.TRANSFERS_IN_ORIG, 0) AS DECIMAL(24,2))                          AS TRANSFERS_IN_ORIG,
    CAST(COALESCE(txn.TRANSFERS_OUT_ORIG, 0) AS DECIMAL(24,2))                         AS TRANSFERS_OUT_ORIG,
    CAST(0 AS DECIMAL(24,2))                                                           AS ASSET_TRANSFERS_ORIG,
    CAST(0 AS DECIMAL(24,2))                                                           AS MISC_TRANSFERS_ORIG,
    CAST(0 AS DECIMAL(24,2))                                                           AS NET_VALUATION_ORIG,
    CAST(0 AS DECIMAL(24,12))                                                          AS FX_RATE_ORIG_CAD,
    CAST(0 AS DECIMAL(24,2))                                                           AS VALUATION_AMT_CAD,
    CAST(0 AS DECIMAL(24,2))                                                           AS SUBSCRIPTION_AMT_CAD,
    CAST(0 AS DECIMAL(24,2))                                                           AS REDEMPTION_AMT_CAD,
    CAST(0 AS DECIMAL(24,2))                                                           AS DIV_REINVESTED_CAD,
    CAST(0 AS DECIMAL(24,2))                                                           AS CHG_DUE_TO_MARKET_CAD,
    CAST(0 AS DECIMAL(24,2))                                                           AS CHG_DUE_TO_FXRATE_CAD,
    CAST(0 AS DECIMAL(24,2))                                                           AS TRANSFERS_IN_CAD,
    CAST(0 AS DECIMAL(24,2))                                                           AS TRANSFERS_OUT_CAD,
    CAST(0 AS DECIMAL(24,2))                                                           AS ASSET_TRANSFERS_CAD,
    CAST(0 AS DECIMAL(24,2))                                                           AS MISC_TRANSFERS_CAD,
    CAST(0 AS DECIMAL(24,2))                                                           AS NET_VALUATION_CAD,
    CAST(0 AS DECIMAL(24,12))                                                          AS FX_RATE_ORIG_USD,
    CAST(0 AS DECIMAL(24,2))                                                           AS VALUATION_AMT_USD,
    CAST(0 AS DECIMAL(24,2))                                                           AS SUBSCRIPTION_AMT_USD,
    CAST(0 AS DECIMAL(24,2))                                                           AS REDEMPTION_AMT_USD,
    CAST(0 AS DECIMAL(24,2))                                                           AS DIV_REINVESTED_USD,
    CAST(0 AS DECIMAL(24,2))                                                           AS CHG_DUE_TO_MARKET_USD,
    CAST(0 AS DECIMAL(24,2))                                                           AS CHG_DUE_TO_FXRATE_USD,
    CAST(0 AS DECIMAL(24,2))                                                           AS TRANSFERS_IN_USD,
    CAST(0 AS DECIMAL(24,2))                                                           AS TRANSFERS_OUT_USD,
    CAST(0 AS DECIMAL(24,2))                                                           AS ASSET_TRANSFERS_USD,
    CAST(0 AS DECIMAL(24,2))                                                           AS MISC_TRANSFERS_USD,
    CAST(0 AS DECIMAL(24,2))                                                           AS NET_VALUATION_USD,
    CAST(0 AS DECIMAL(24,12))                                                          AS FX_RATE_ORIG_HKD,
    CAST(0 AS DECIMAL(24,2))                                                           AS VALUATION_AMT_HKD,
    CAST(0 AS DECIMAL(24,2))                                                           AS SUBSCRIPTION_AMT_HKD,
    CAST(0 AS DECIMAL(24,2))                                                           AS REDEMPTION_AMT_HKD,
    CAST(0 AS DECIMAL(24,2))                                                           AS DIV_REINVESTED_HKD,
    CAST(0 AS DECIMAL(24,2))                                                           AS CHG_DUE_TO_MARKET_HKD,
    CAST(0 AS DECIMAL(24,2))                                                           AS CHG_DUE_TO_FXRATE_HKD,
    CAST(0 AS DECIMAL(24,2))                                                           AS TRANSFERS_IN_HKD,
    CAST(0 AS DECIMAL(24,2))                                                           AS TRANSFERS_OUT_HKD,
    CAST(0 AS DECIMAL(24,2))                                                           AS ASSET_TRANSFERS_HKD,
    CAST(0 AS DECIMAL(24,2))                                                           AS MISC_TRANSFERS_HKD,
    CAST(0 AS DECIMAL(24,2))                                                           AS NET_VALUATION_HKD,
    CAST(NULL AS TIMESTAMP)                                                            AS SRC_CREATED_TS,
    CAST(NULL AS STRING)                                                               AS SRC_CREATED_TZ,
    CAST(hol.EDL_CREATED_TS AS TIMESTAMP)                                              AS EDL_CREATED_TS,
    CAST('Y' AS STRING)                                                                AS CURRENT_VERSION,
    to_utc_timestamp(current_timestamp,'America/Montreal')                             AS EDL_CURATED_TS,
    CAST(hol.EFFECTIVE_TS AS DATE)                                                     AS EFFECTIVE_DT,
    CAST(${src} AS STRING)                                                             AS SRC
FROM ${TEMP_DB}.temp_bf_investor_holding_sof_jhisdh hol
    LEFT JOIN ${TEMP_DB}.temp_bf_investor_transaction_sof_jhisdh txn
        ON (txn.DISTRIBUTOR_UID <=> hol.DISTRIBUTOR_UID
            AND txn.LIABILITY_PORTF_UID <=> hol.LIABILITY_PORTF_UID
            AND txn.TRADE_DATE = hol.EFFECTIVE_TS)
;

DROP TABLE IF EXISTS ${TEMP_DB}.temp_bf_source_of_funds_jhisdh PURGE;

CREATE TABLE ${TEMP_DB}.temp_bf_source_of_funds_jhisdh
  (
    inc_VALUATION_ID                         STRING,
    inc_EFFECTIVE_TS                         TIMESTAMP,
    inc_SYS_EFF_DATE_FROM                    TIMESTAMP,
    inc_SYS_EFF_DATE_TO                      TIMESTAMP,
    inc_LIABILITY_PORTFOLIO_KEY              STRING,
    inc_DISTRIBUTOR_KEY                      STRING,
    inc_SECURITY_KEY                         STRING,
    inc_DISTRIBUTOR_UID                      STRING,
    inc_LIABILITY_PORTF_UID                  STRING,
    inc_SRC_SECURITY_ID                      STRING,
    inc_ORIG_CURRENCY_CODE                   STRING,
    inc_VALUATION_AMT_ORIG                   DECIMAL(24,2),
    inc_SUBSCRIPTION_AMT_ORIG                DECIMAL(24,2),
    inc_REDEMPTION_AMT_ORIG                  DECIMAL(24,2),
    inc_DIV_REINVESTED_ORIG                  DECIMAL(24,2),
    inc_CHG_DUE_TO_MARKET_ORIG               DECIMAL(24,2),
    inc_CHG_DUE_TO_FXRATE_ORIG               DECIMAL(24,2),
    inc_TRANSFERS_IN_ORIG                    DECIMAL(24,2),
    inc_TRANSFERS_OUT_ORIG                   DECIMAL(24,2),
    inc_ASSET_TRANSFERS_ORIG                 DECIMAL(24,2),
    inc_MISC_TRANSFERS_ORIG                  DECIMAL(24,2),
    inc_NET_VALUATION_ORIG                   DECIMAL(24,2),
    inc_FX_RATE_ORIG_CAD                     DECIMAL(24,12),
    inc_VALUATION_AMT_CAD                    DECIMAL(24,2),
    inc_SUBSCRIPTION_AMT_CAD                 DECIMAL(24,2),
    inc_REDEMPTION_AMT_CAD                   DECIMAL(24,2),
    inc_DIV_REINVESTED_CAD                   DECIMAL(24,2),
    inc_CHG_DUE_TO_MARKET_CAD                DECIMAL(24,2),
    inc_CHG_DUE_TO_FXRATE_CAD                DECIMAL(24,2),
    inc_TRANSFERS_IN_CAD                     DECIMAL(24,2),
    inc_TRANSFERS_OUT_CAD                    DECIMAL(24,2),
    inc_ASSET_TRANSFERS_CAD                  DECIMAL(24,2),
    inc_MISC_TRANSFERS_CAD                   DECIMAL(24,2),
    inc_NET_VALUATION_CAD                    DECIMAL(24,2),
    inc_FX_RATE_ORIG_USD                     DECIMAL(24,12),
    inc_VALUATION_AMT_USD                    DECIMAL(24,2),
    inc_SUBSCRIPTION_AMT_USD                 DECIMAL(24,2),
    inc_REDEMPTION_AMT_USD                   DECIMAL(24,2),
    inc_DIV_REINVESTED_USD                   DECIMAL(24,2),
    inc_CHG_DUE_TO_MARKET_USD                DECIMAL(24,2),
    inc_CHG_DUE_TO_FXRATE_USD                DECIMAL(24,2),
    inc_TRANSFERS_IN_USD                     DECIMAL(24,2),
    inc_TRANSFERS_OUT_USD                    DECIMAL(24,2),
    inc_ASSET_TRANSFERS_USD                  DECIMAL(24,2),
    inc_MISC_TRANSFERS_USD                   DECIMAL(24,2),
    inc_NET_VALUATION_USD                    DECIMAL(24,2),
    inc_FX_RATE_ORIG_HKD                     DECIMAL(24,12),
    inc_VALUATION_AMT_HKD                    DECIMAL(24,2),
    inc_SUBSCRIPTION_AMT_HKD                 DECIMAL(24,2),
    inc_REDEMPTION_AMT_HKD                   DECIMAL(24,2),
    inc_DIV_REINVESTED_HKD                   DECIMAL(24,2),
    inc_CHG_DUE_TO_MARKET_HKD                DECIMAL(24,2),
    inc_CHG_DUE_TO_FXRATE_HKD                DECIMAL(24,2),
    inc_TRANSFERS_IN_HKD                     DECIMAL(24,2),
    inc_TRANSFERS_OUT_HKD                    DECIMAL(24,2),
    inc_ASSET_TRANSFERS_HKD                  DECIMAL(24,2),
    inc_MISC_TRANSFERS_HKD                   DECIMAL(24,2),
    inc_NET_VALUATION_HKD                    DECIMAL(24,2),
    inc_SRC_CREATED_TS                       TIMESTAMP,
    inc_SRC_CREATED_TZ                       STRING,
    inc_EDL_CREATED_TS                       TIMESTAMP,
    inc_CURRENT_VERSION                      STRING,
    inc_EDL_CURATED_TS                       TIMESTAMP,
    inc_EFFECTIVE_DT                         DATE,
    inc_SRC                                  STRING,
    cur_VALUATION_ID                         STRING,
    cur_EFFECTIVE_TS                         TIMESTAMP,
    cur_SYS_EFF_DATE_FROM                    TIMESTAMP,
    cur_SYS_EFF_DATE_TO                      TIMESTAMP,
    cur_LIABILITY_PORTFOLIO_KEY              STRING,
    cur_DISTRIBUTOR_KEY                      STRING,
    cur_SECURITY_KEY                         STRING,
    cur_DISTRIBUTOR_UID                      STRING,
    cur_LIABILITY_PORTF_UID                  STRING,
    cur_SRC_SECURITY_ID                      STRING,
    cur_ORIG_CURRENCY_CODE                   STRING,
    cur_VALUATION_AMT_ORIG                   DECIMAL(24,2),
    cur_SUBSCRIPTION_AMT_ORIG                DECIMAL(24,2),
    cur_REDEMPTION_AMT_ORIG                  DECIMAL(24,2),
    cur_DIV_REINVESTED_ORIG                  DECIMAL(24,2),
    cur_CHG_DUE_TO_MARKET_ORIG               DECIMAL(24,2),
    cur_CHG_DUE_TO_FXRATE_ORIG               DECIMAL(24,2),
    cur_TRANSFERS_IN_ORIG                    DECIMAL(24,2),
    cur_TRANSFERS_OUT_ORIG                   DECIMAL(24,2),
    cur_ASSET_TRANSFERS_ORIG                 DECIMAL(24,2),
    cur_MISC_TRANSFERS_ORIG                  DECIMAL(24,2),
    cur_NET_VALUATION_ORIG                   DECIMAL(24,2),
    cur_FX_RATE_ORIG_CAD                     DECIMAL(24,12),
    cur_VALUATION_AMT_CAD                    DECIMAL(24,2),
    cur_SUBSCRIPTION_AMT_CAD                 DECIMAL(24,2),
    cur_REDEMPTION_AMT_CAD                   DECIMAL(24,2),
    cur_DIV_REINVESTED_CAD                   DECIMAL(24,2),
    cur_CHG_DUE_TO_MARKET_CAD                DECIMAL(24,2),
    cur_CHG_DUE_TO_FXRATE_CAD                DECIMAL(24,2),
    cur_TRANSFERS_IN_CAD                     DECIMAL(24,2),
    cur_TRANSFERS_OUT_CAD                    DECIMAL(24,2),
    cur_ASSET_TRANSFERS_CAD                  DECIMAL(24,2),
    cur_MISC_TRANSFERS_CAD                   DECIMAL(24,2),
    cur_NET_VALUATION_CAD                    DECIMAL(24,2),
    cur_FX_RATE_ORIG_USD                     DECIMAL(24,12),
    cur_VALUATION_AMT_USD                    DECIMAL(24,2),
    cur_SUBSCRIPTION_AMT_USD                 DECIMAL(24,2),
    cur_REDEMPTION_AMT_USD                   DECIMAL(24,2),
    cur_DIV_REINVESTED_USD                   DECIMAL(24,2),
    cur_CHG_DUE_TO_MARKET_USD                DECIMAL(24,2),
    cur_CHG_DUE_TO_FXRATE_USD                DECIMAL(24,2),
    cur_TRANSFERS_IN_USD                     DECIMAL(24,2),
    cur_TRANSFERS_OUT_USD                    DECIMAL(24,2),
    cur_ASSET_TRANSFERS_USD                  DECIMAL(24,2),
    cur_MISC_TRANSFERS_USD                   DECIMAL(24,2),
    cur_NET_VALUATION_USD                    DECIMAL(24,2),
    cur_FX_RATE_ORIG_HKD                     DECIMAL(24,12),
    cur_VALUATION_AMT_HKD                    DECIMAL(24,2),
    cur_SUBSCRIPTION_AMT_HKD                 DECIMAL(24,2),
    cur_REDEMPTION_AMT_HKD                   DECIMAL(24,2),
    cur_DIV_REINVESTED_HKD                   DECIMAL(24,2),
    cur_CHG_DUE_TO_MARKET_HKD                DECIMAL(24,2),
    cur_CHG_DUE_TO_FXRATE_HKD                DECIMAL(24,2),
    cur_TRANSFERS_IN_HKD                     DECIMAL(24,2),
    cur_TRANSFERS_OUT_HKD                    DECIMAL(24,2),
    cur_ASSET_TRANSFERS_HKD                  DECIMAL(24,2),
    cur_MISC_TRANSFERS_HKD                   DECIMAL(24,2),
    cur_NET_VALUATION_HKD                    DECIMAL(24,2),
    cur_SRC_CREATED_TS                       TIMESTAMP,
    cur_SRC_CREATED_TZ                       STRING,
    cur_EDL_CREATED_TS                       TIMESTAMP,
    cur_CURRENT_VERSION                      STRING,
    cur_EDL_CURATED_TS                       TIMESTAMP,
    cur_EFFECTIVE_DT                         DATE,
    cur_SRC                                  STRING
  )
PARTITIONED BY (FLAG STRING)
STORED AS ORC
TBLPROPERTIES ('auto.purge'='true')
;

INSERT OVERWRITE TABLE ${TEMP_DB}.temp_bf_source_of_funds_jhisdh PARTITION (FLAG)
SELECT
    I.VALUATION_ID                                                                     AS inc_VALUATION_ID,
    I.EFFECTIVE_TS                                                                     AS inc_EFFECTIVE_TS,
    I.SYS_EFF_DATE_FROM                                                                AS inc_SYS_EFF_DATE_FROM,
    I.SYS_EFF_DATE_TO                                                                  AS inc_SYS_EFF_DATE_TO,
    I.LIABILITY_PORTFOLIO_KEY                                                          AS inc_LIABILITY_PORTFOLIO_KEY,
    I.DISTRIBUTOR_KEY                                                                  AS inc_DISTRIBUTOR_KEY,
    I.SECURITY_KEY                                                                     AS inc_SECURITY_KEY,
    I.DISTRIBUTOR_UID                                                                  AS inc_DISTRIBUTOR_UID,
    I.LIABILITY_PORTF_UID                                                              AS inc_LIABILITY_PORTF_UID,
    I.SRC_SECURITY_ID                                                                  AS inc_SRC_SECURITY_ID,
    I.ORIG_CURRENCY_CODE                                                               AS inc_ORIG_CURRENCY_CODE,
    I.VALUATION_AMT_ORIG                                                               AS inc_VALUATION_AMT_ORIG,
    I.SUBSCRIPTION_AMT_ORIG                                                            AS inc_SUBSCRIPTION_AMT_ORIG,
    I.REDEMPTION_AMT_ORIG                                                              AS inc_REDEMPTION_AMT_ORIG,
    I.DIV_REINVESTED_ORIG                                                              AS inc_DIV_REINVESTED_ORIG,
    I.CHG_DUE_TO_MARKET_ORIG                                                           AS inc_CHG_DUE_TO_MARKET_ORIG,
    I.CHG_DUE_TO_FXRATE_ORIG                                                           AS inc_CHG_DUE_TO_FXRATE_ORIG,
    I.TRANSFERS_IN_ORIG                                                                AS inc_TRANSFERS_IN_ORIG,
    I.TRANSFERS_OUT_ORIG                                                               AS inc_TRANSFERS_OUT_ORIG,
    I.ASSET_TRANSFERS_ORIG                                                             AS inc_ASSET_TRANSFERS_ORIG,
    I.MISC_TRANSFERS_ORIG                                                              AS inc_MISC_TRANSFERS_ORIG,
    I.NET_VALUATION_ORIG                                                               AS inc_NET_VALUATION_ORIG,
    I.FX_RATE_ORIG_CAD                                                                 AS inc_FX_RATE_ORIG_CAD,
    I.VALUATION_AMT_CAD                                                                AS inc_VALUATION_AMT_CAD,
    I.SUBSCRIPTION_AMT_CAD                                                             AS inc_SUBSCRIPTION_AMT_CAD,
    I.REDEMPTION_AMT_CAD                                                               AS inc_REDEMPTION_AMT_CAD,
    I.DIV_REINVESTED_CAD                                                               AS inc_DIV_REINVESTED_CAD,
    I.CHG_DUE_TO_MARKET_CAD                                                            AS inc_CHG_DUE_TO_MARKET_CAD,
    I.CHG_DUE_TO_FXRATE_CAD                                                            AS inc_CHG_DUE_TO_FXRATE_CAD,
    I.TRANSFERS_IN_CAD                                                                 AS inc_TRANSFERS_IN_CAD,
    I.TRANSFERS_OUT_CAD                                                                AS inc_TRANSFERS_OUT_CAD,
    I.ASSET_TRANSFERS_CAD                                                              AS inc_ASSET_TRANSFERS_CAD,
    I.MISC_TRANSFERS_CAD                                                               AS inc_MISC_TRANSFERS_CAD,
    I.NET_VALUATION_CAD                                                                AS inc_NET_VALUATION_CAD,
    I.FX_RATE_ORIG_USD                                                                 AS inc_FX_RATE_ORIG_USD,
    I.VALUATION_AMT_USD                                                                AS inc_VALUATION_AMT_USD,
    I.SUBSCRIPTION_AMT_USD                                                             AS inc_SUBSCRIPTION_AMT_USD,
    I.REDEMPTION_AMT_USD                                                               AS inc_REDEMPTION_AMT_USD,
    I.DIV_REINVESTED_USD                                                               AS inc_DIV_REINVESTED_USD,
    I.CHG_DUE_TO_MARKET_USD                                                            AS inc_CHG_DUE_TO_MARKET_USD,
    I.CHG_DUE_TO_FXRATE_USD                                                            AS inc_CHG_DUE_TO_FXRATE_USD,
    I.TRANSFERS_IN_USD                                                                 AS inc_TRANSFERS_IN_USD,
    I.TRANSFERS_OUT_USD                                                                AS inc_TRANSFERS_OUT_USD,
    I.ASSET_TRANSFERS_USD                                                              AS inc_ASSET_TRANSFERS_USD,
    I.MISC_TRANSFERS_USD                                                               AS inc_MISC_TRANSFERS_USD,
    I.NET_VALUATION_USD                                                                AS inc_NET_VALUATION_USD,
    I.FX_RATE_ORIG_HKD                                                                 AS inc_FX_RATE_ORIG_HKD,
    I.VALUATION_AMT_HKD                                                                AS inc_VALUATION_AMT_HKD,
    I.SUBSCRIPTION_AMT_HKD                                                             AS inc_SUBSCRIPTION_AMT_HKD,
    I.REDEMPTION_AMT_HKD                                                               AS inc_REDEMPTION_AMT_HKD,
    I.DIV_REINVESTED_HKD                                                               AS inc_DIV_REINVESTED_HKD,
    I.CHG_DUE_TO_MARKET_HKD                                                            AS inc_CHG_DUE_TO_MARKET_HKD,
    I.CHG_DUE_TO_FXRATE_HKD                                                            AS inc_CHG_DUE_TO_FXRATE_HKD,
    I.TRANSFERS_IN_HKD                                                                 AS inc_TRANSFERS_IN_HKD,
    I.TRANSFERS_OUT_HKD                                                                AS inc_TRANSFERS_OUT_HKD,
    I.ASSET_TRANSFERS_HKD                                                              AS inc_ASSET_TRANSFERS_HKD,
    I.MISC_TRANSFERS_HKD                                                               AS inc_MISC_TRANSFERS_HKD,
    I.NET_VALUATION_HKD                                                                AS inc_NET_VALUATION_HKD,
    I.SRC_CREATED_TS                                                                   AS inc_SRC_CREATED_TS,
    I.SRC_CREATED_TZ                                                                   AS inc_SRC_CREATED_TZ,
    I.EDL_CREATED_TS                                                                   AS inc_EDL_CREATED_TS,
    I.CURRENT_VERSION                                                                  AS inc_CURRENT_VERSION,
    I.EDL_CURATED_TS                                                                   AS inc_EDL_CURATED_TS,
    I.EFFECTIVE_DT                                                                     AS inc_EFFECTIVE_DT,
    I.SRC                                                                              AS inc_SRC,
    C.VALUATION_ID                                                                     AS cur_VALUATION_ID,
    C.EFFECTIVE_TS                                                                     AS cur_EFFECTIVE_TS,
    C.SYS_EFF_DATE_FROM                                                                AS cur_SYS_EFF_DATE_FROM,
    C.SYS_EFF_DATE_TO                                                                  AS cur_SYS_EFF_DATE_TO,
    C.LIABILITY_PORTFOLIO_KEY                                                          AS cur_LIABILITY_PORTFOLIO_KEY,
    C.DISTRIBUTOR_KEY                                                                  AS cur_DISTRIBUTOR_KEY,
    C.SECURITY_KEY                                                                     AS cur_SECURITY_KEY,
    C.DISTRIBUTOR_UID                                                                  AS cur_DISTRIBUTOR_UID,
    C.LIABILITY_PORTF_UID                                                              AS cur_LIABILITY_PORTF_UID,
    C.SRC_SECURITY_ID                                                                  AS cur_SRC_SECURITY_ID,
    C.ORIG_CURRENCY_CODE                                                               AS cur_ORIG_CURRENCY_CODE,
    C.VALUATION_AMT_ORIG                                                               AS cur_VALUATION_AMT_ORIG,
    C.SUBSCRIPTION_AMT_ORIG                                                            AS cur_SUBSCRIPTION_AMT_ORIG,
    C.REDEMPTION_AMT_ORIG                                                              AS cur_REDEMPTION_AMT_ORIG,
    C.DIV_REINVESTED_ORIG                                                              AS cur_DIV_REINVESTED_ORIG,
    C.CHG_DUE_TO_MARKET_ORIG                                                           AS cur_CHG_DUE_TO_MARKET_ORIG,
    C.CHG_DUE_TO_FXRATE_ORIG                                                           AS cur_CHG_DUE_TO_FXRATE_ORIG,
    C.TRANSFERS_IN_ORIG                                                                AS cur_TRANSFERS_IN_ORIG,
    C.TRANSFERS_OUT_ORIG                                                               AS cur_TRANSFERS_OUT_ORIG,
    C.ASSET_TRANSFERS_ORIG                                                             AS cur_ASSET_TRANSFERS_ORIG,
    C.MISC_TRANSFERS_ORIG                                                              AS cur_MISC_TRANSFERS_ORIG,
    C.NET_VALUATION_ORIG                                                               AS cur_NET_VALUATION_ORIG,
    C.FX_RATE_ORIG_CAD                                                                 AS cur_FX_RATE_ORIG_CAD,
    C.VALUATION_AMT_CAD                                                                AS cur_VALUATION_AMT_CAD,
    C.SUBSCRIPTION_AMT_CAD                                                             AS cur_SUBSCRIPTION_AMT_CAD,
    C.REDEMPTION_AMT_CAD                                                               AS cur_REDEMPTION_AMT_CAD,
    C.DIV_REINVESTED_CAD                                                               AS cur_DIV_REINVESTED_CAD,
    C.CHG_DUE_TO_MARKET_CAD                                                            AS cur_CHG_DUE_TO_MARKET_CAD,
    C.CHG_DUE_TO_FXRATE_CAD                                                            AS cur_CHG_DUE_TO_FXRATE_CAD,
    C.TRANSFERS_IN_CAD                                                                 AS cur_TRANSFERS_IN_CAD,
    C.TRANSFERS_OUT_CAD                                                                AS cur_TRANSFERS_OUT_CAD,
    C.ASSET_TRANSFERS_CAD                                                              AS cur_ASSET_TRANSFERS_CAD,
    C.MISC_TRANSFERS_CAD                                                               AS cur_MISC_TRANSFERS_CAD,
    C.NET_VALUATION_CAD                                                                AS cur_NET_VALUATION_CAD,
    C.FX_RATE_ORIG_USD                                                                 AS cur_FX_RATE_ORIG_USD,
    C.VALUATION_AMT_USD                                                                AS cur_VALUATION_AMT_USD,
    C.SUBSCRIPTION_AMT_USD                                                             AS cur_SUBSCRIPTION_AMT_USD,
    C.REDEMPTION_AMT_USD                                                               AS cur_REDEMPTION_AMT_USD,
    C.DIV_REINVESTED_USD                                                               AS cur_DIV_REINVESTED_USD,
    C.CHG_DUE_TO_MARKET_USD                                                            AS cur_CHG_DUE_TO_MARKET_USD,
    C.CHG_DUE_TO_FXRATE_USD                                                            AS cur_CHG_DUE_TO_FXRATE_USD,
    C.TRANSFERS_IN_USD                                                                 AS cur_TRANSFERS_IN_USD,
    C.TRANSFERS_OUT_USD                                                                AS cur_TRANSFERS_OUT_USD,
    C.ASSET_TRANSFERS_USD                                                              AS cur_ASSET_TRANSFERS_USD,
    C.MISC_TRANSFERS_USD                                                               AS cur_MISC_TRANSFERS_USD,
    C.NET_VALUATION_USD                                                                AS cur_NET_VALUATION_USD,
    C.FX_RATE_ORIG_HKD                                                                 AS cur_FX_RATE_ORIG_HKD,
    C.VALUATION_AMT_HKD                                                                AS cur_VALUATION_AMT_HKD,
    C.SUBSCRIPTION_AMT_HKD                                                             AS cur_SUBSCRIPTION_AMT_HKD,
    C.REDEMPTION_AMT_HKD                                                               AS cur_REDEMPTION_AMT_HKD,
    C.DIV_REINVESTED_HKD                                                               AS cur_DIV_REINVESTED_HKD,
    C.CHG_DUE_TO_MARKET_HKD                                                            AS cur_CHG_DUE_TO_MARKET_HKD,
    C.CHG_DUE_TO_FXRATE_HKD                                                            AS cur_CHG_DUE_TO_FXRATE_HKD,
    C.TRANSFERS_IN_HKD                                                                 AS cur_TRANSFERS_IN_HKD,
    C.TRANSFERS_OUT_HKD                                                                AS cur_TRANSFERS_OUT_HKD,
    C.ASSET_TRANSFERS_HKD                                                              AS cur_ASSET_TRANSFERS_HKD,
    C.MISC_TRANSFERS_HKD                                                               AS cur_MISC_TRANSFERS_HKD,
    C.NET_VALUATION_HKD                                                                AS cur_NET_VALUATION_HKD,
    C.SRC_CREATED_TS                                                                   AS cur_SRC_CREATED_TS,
    C.SRC_CREATED_TZ                                                                   AS cur_SRC_CREATED_TZ,
    C.EDL_CREATED_TS                                                                   AS cur_EDL_CREATED_TS,
    C.CURRENT_VERSION                                                                  AS cur_CURRENT_VERSION,
    C.EDL_CURATED_TS                                                                   AS cur_EDL_CURATED_TS,
    C.EFFECTIVE_DT                                                                     AS cur_EFFECTIVE_DT,
    C.SRC                                                                              AS cur_SRC,
    CASE
        WHEN C.EFFECTIVE_TS IS NULL THEN 'N'
        WHEN C.EFFECTIVE_TS IS NOT NULL AND I.EFFECTIVE_TS IS NULL THEN 'E'
        ELSE 'C'
    END                                                                                AS FLAG
FROM ${TEMP_DB}.inc_bf_source_of_funds_jhisdh I
    FULL OUTER JOIN (SELECT * FROM ${CURATED_DB}.BF_SOURCE_OF_FUNDS WHERE SRC = ${src}
                        AND EFFECTIVE_DT IN (SELECT DISTINCT EFFECTIVE_DT FROM ${TEMP_DB}.inc_bf_source_of_funds_jhisdh)) C
        ON (I.EFFECTIVE_TS = C.EFFECTIVE_TS
            AND I.DISTRIBUTOR_UID <=> C.DISTRIBUTOR_UID
            AND I.LIABILITY_PORTF_UID <=> C.LIABILITY_PORTF_UID
            AND I.CURRENT_VERSION = C.CURRENT_VERSION)
;

INSERT OVERWRITE TABLE ${CURATED_DB}.BF_SOURCE_OF_FUNDS PARTITION (EFFECTIVE_DT, SRC)
SELECT
    ${CURATED_DB}.Guid()                                                               AS VALUATION_ID,
    inc_EFFECTIVE_TS                                                                   AS EFFECTIVE_TS,
    IF(flag = 'N', CAST('1980-01-01 00:00:00' AS TIMESTAMP), inc_SYS_EFF_DATE_FROM)    AS SYS_EFF_DATE_FROM,
    inc_SYS_EFF_DATE_TO                                                                AS SYS_EFF_DATE_TO,
    inc_LIABILITY_PORTFOLIO_KEY                                                        AS LIABILITY_PORTFOLIO_KEY,
    inc_DISTRIBUTOR_KEY                                                                AS DISTRIBUTOR_KEY,
    inc_SECURITY_KEY                                                                   AS SECURITY_KEY,
    inc_DISTRIBUTOR_UID                                                                AS DISTRIBUTOR_UID,
    inc_LIABILITY_PORTF_UID                                                            AS LIABILITY_PORTF_UID,
    inc_SRC_SECURITY_ID                                                                AS SRC_SECURITY_ID,
    inc_ORIG_CURRENCY_CODE                                                             AS ORIG_CURRENCY_CODE,
    inc_VALUATION_AMT_ORIG                                                             AS VALUATION_AMT_ORIG,
    inc_SUBSCRIPTION_AMT_ORIG                                                          AS SUBSCRIPTION_AMT_ORIG,
    inc_REDEMPTION_AMT_ORIG                                                            AS REDEMPTION_AMT_ORIG,
    inc_DIV_REINVESTED_ORIG                                                            AS DIV_REINVESTED_ORIG,
    inc_CHG_DUE_TO_MARKET_ORIG                                                         AS CHG_DUE_TO_MARKET_ORIG,
    inc_CHG_DUE_TO_FXRATE_ORIG                                                         AS CHG_DUE_TO_FXRATE_ORIG,
    inc_TRANSFERS_IN_ORIG                                                              AS TRANSFERS_IN_ORIG,
    inc_TRANSFERS_OUT_ORIG                                                             AS TRANSFERS_OUT_ORIG,
    inc_ASSET_TRANSFERS_ORIG                                                           AS ASSET_TRANSFERS_ORIG,
    inc_MISC_TRANSFERS_ORIG                                                            AS MISC_TRANSFERS_ORIG,
    inc_NET_VALUATION_ORIG                                                             AS NET_VALUATION_ORIG,
    inc_FX_RATE_ORIG_CAD                                                               AS FX_RATE_ORIG_CAD,
    inc_VALUATION_AMT_CAD                                                              AS VALUATION_AMT_CAD,
    inc_SUBSCRIPTION_AMT_CAD                                                           AS SUBSCRIPTION_AMT_CAD,
    inc_REDEMPTION_AMT_CAD                                                             AS REDEMPTION_AMT_CAD,
    inc_DIV_REINVESTED_CAD                                                             AS DIV_REINVESTED_CAD,
    inc_CHG_DUE_TO_MARKET_CAD                                                          AS CHG_DUE_TO_MARKET_CAD,
    inc_CHG_DUE_TO_FXRATE_CAD                                                          AS CHG_DUE_TO_FXRATE_CAD,
    inc_TRANSFERS_IN_CAD                                                               AS TRANSFERS_IN_CAD,
    inc_TRANSFERS_OUT_CAD                                                              AS TRANSFERS_OUT_CAD,
    inc_ASSET_TRANSFERS_CAD                                                            AS ASSET_TRANSFERS_CAD,
    inc_MISC_TRANSFERS_CAD                                                             AS MISC_TRANSFERS_CAD,
    inc_NET_VALUATION_CAD                                                              AS NET_VALUATION_CAD,
    inc_FX_RATE_ORIG_USD                                                               AS FX_RATE_ORIG_USD,
    inc_VALUATION_AMT_USD                                                              AS VALUATION_AMT_USD,
    inc_SUBSCRIPTION_AMT_USD                                                           AS SUBSCRIPTION_AMT_USD,
    inc_REDEMPTION_AMT_USD                                                             AS REDEMPTION_AMT_USD,
    inc_DIV_REINVESTED_USD                                                             AS DIV_REINVESTED_USD,
    inc_CHG_DUE_TO_MARKET_USD                                                          AS CHG_DUE_TO_MARKET_USD,
    inc_CHG_DUE_TO_FXRATE_USD                                                          AS CHG_DUE_TO_FXRATE_USD,
    inc_TRANSFERS_IN_USD                                                               AS TRANSFERS_IN_USD,
    inc_TRANSFERS_OUT_USD                                                              AS TRANSFERS_OUT_USD,
    inc_ASSET_TRANSFERS_USD                                                            AS ASSET_TRANSFERS_USD,
    inc_MISC_TRANSFERS_USD                                                             AS MISC_TRANSFERS_USD,
    inc_NET_VALUATION_USD                                                              AS NET_VALUATION_USD,
    inc_FX_RATE_ORIG_HKD                                                               AS FX_RATE_ORIG_HKD,
    inc_VALUATION_AMT_HKD                                                              AS VALUATION_AMT_HKD,
    inc_SUBSCRIPTION_AMT_HKD                                                           AS SUBSCRIPTION_AMT_HKD,
    inc_REDEMPTION_AMT_HKD                                                             AS REDEMPTION_AMT_HKD,
    inc_DIV_REINVESTED_HKD                                                             AS DIV_REINVESTED_HKD,
    inc_CHG_DUE_TO_MARKET_HKD                                                          AS CHG_DUE_TO_MARKET_HKD,
    inc_CHG_DUE_TO_FXRATE_HKD                                                          AS CHG_DUE_TO_FXRATE_HKD,
    inc_TRANSFERS_IN_HKD                                                               AS TRANSFERS_IN_HKD,
    inc_TRANSFERS_OUT_HKD                                                              AS TRANSFERS_OUT_HKD,
    inc_ASSET_TRANSFERS_HKD                                                            AS ASSET_TRANSFERS_HKD,
    inc_MISC_TRANSFERS_HKD                                                             AS MISC_TRANSFERS_HKD,
    inc_NET_VALUATION_HKD                                                              AS NET_VALUATION_HKD,
    inc_SRC_CREATED_TS                                                                 AS SRC_CREATED_TS,
    inc_SRC_CREATED_TZ                                                                 AS SRC_CREATED_TZ,
    inc_EDL_CREATED_TS                                                                 AS EDL_CREATED_TS,
    inc_CURRENT_VERSION                                                                AS CURRENT_VERSION,
    inc_EDL_CURATED_TS                                                                 AS EDL_CURATED_TS,
    inc_EFFECTIVE_DT                                                                   AS EFFECTIVE_DT,
    inc_SRC                                                                            AS SRC
FROM ${TEMP_DB}.temp_bf_source_of_funds_jhisdh
WHERE flag IN ('N', 'C')

UNION ALL

SELECT
    cur_VALUATION_ID                                                                   AS VALUATION_ID,
    cur_EFFECTIVE_TS                                                                   AS EFFECTIVE_TS,
    cur_SYS_EFF_DATE_FROM                                                              AS SYS_EFF_DATE_FROM,
    CASE WHEN flag = 'C' THEN inc_SYS_EFF_DATE_FROM
         ELSE cur_SYS_EFF_DATE_TO END                                                  AS SYS_EFF_DATE_TO,
    cur_LIABILITY_PORTFOLIO_KEY                                                        AS LIABILITY_PORTFOLIO_KEY,
    cur_DISTRIBUTOR_KEY                                                                AS DISTRIBUTOR_KEY,
    cur_SECURITY_KEY                                                                   AS SECURITY_KEY,
    cur_DISTRIBUTOR_UID                                                                AS DISTRIBUTOR_UID,
    cur_LIABILITY_PORTF_UID                                                            AS LIABILITY_PORTF_UID,
    cur_SRC_SECURITY_ID                                                                AS SRC_SECURITY_ID,
    cur_ORIG_CURRENCY_CODE                                                             AS ORIG_CURRENCY_CODE,
    cur_VALUATION_AMT_ORIG                                                             AS VALUATION_AMT_ORIG,
    cur_SUBSCRIPTION_AMT_ORIG                                                          AS SUBSCRIPTION_AMT_ORIG,
    cur_REDEMPTION_AMT_ORIG                                                            AS REDEMPTION_AMT_ORIG,
    cur_DIV_REINVESTED_ORIG                                                            AS DIV_REINVESTED_ORIG,
    cur_CHG_DUE_TO_MARKET_ORIG                                                         AS CHG_DUE_TO_MARKET_ORIG,
    cur_CHG_DUE_TO_FXRATE_ORIG                                                         AS CHG_DUE_TO_FXRATE_ORIG,
    cur_TRANSFERS_IN_ORIG                                                              AS TRANSFERS_IN_ORIG,
    cur_TRANSFERS_OUT_ORIG                                                             AS TRANSFERS_OUT_ORIG,
    cur_ASSET_TRANSFERS_ORIG                                                           AS ASSET_TRANSFERS_ORIG,
    cur_MISC_TRANSFERS_ORIG                                                            AS MISC_TRANSFERS_ORIG,
    cur_NET_VALUATION_ORIG                                                             AS NET_VALUATION_ORIG,
    cur_FX_RATE_ORIG_CAD                                                               AS FX_RATE_ORIG_CAD,
    cur_VALUATION_AMT_CAD                                                              AS VALUATION_AMT_CAD,
    cur_SUBSCRIPTION_AMT_CAD                                                           AS SUBSCRIPTION_AMT_CAD,
    cur_REDEMPTION_AMT_CAD                                                             AS REDEMPTION_AMT_CAD,
    cur_DIV_REINVESTED_CAD                                                             AS DIV_REINVESTED_CAD,
    cur_CHG_DUE_TO_MARKET_CAD                                                          AS CHG_DUE_TO_MARKET_CAD,
    cur_CHG_DUE_TO_FXRATE_CAD                                                          AS CHG_DUE_TO_FXRATE_CAD,
    cur_TRANSFERS_IN_CAD                                                               AS TRANSFERS_IN_CAD,
    cur_TRANSFERS_OUT_CAD                                                              AS TRANSFERS_OUT_CAD,
    cur_ASSET_TRANSFERS_CAD                                                            AS ASSET_TRANSFERS_CAD,
    cur_MISC_TRANSFERS_CAD                                                             AS MISC_TRANSFERS_CAD,
    cur_NET_VALUATION_CAD                                                              AS NET_VALUATION_CAD,
    cur_FX_RATE_ORIG_USD                                                               AS FX_RATE_ORIG_USD,
    cur_VALUATION_AMT_USD                                                              AS VALUATION_AMT_USD,
    cur_SUBSCRIPTION_AMT_USD                                                           AS SUBSCRIPTION_AMT_USD,
    cur_REDEMPTION_AMT_USD                                                             AS REDEMPTION_AMT_USD,
    cur_DIV_REINVESTED_USD                                                             AS DIV_REINVESTED_USD,
    cur_CHG_DUE_TO_MARKET_USD                                                          AS CHG_DUE_TO_MARKET_USD,
    cur_CHG_DUE_TO_FXRATE_USD                                                          AS CHG_DUE_TO_FXRATE_USD,
    cur_TRANSFERS_IN_USD                                                               AS TRANSFERS_IN_USD,
    cur_TRANSFERS_OUT_USD                                                              AS TRANSFERS_OUT_USD,
    cur_ASSET_TRANSFERS_USD                                                            AS ASSET_TRANSFERS_USD,
    cur_MISC_TRANSFERS_USD                                                             AS MISC_TRANSFERS_USD,
    cur_NET_VALUATION_USD                                                              AS NET_VALUATION_USD,
    cur_FX_RATE_ORIG_HKD                                                               AS FX_RATE_ORIG_HKD,
    cur_VALUATION_AMT_HKD                                                              AS VALUATION_AMT_HKD,
    cur_SUBSCRIPTION_AMT_HKD                                                           AS SUBSCRIPTION_AMT_HKD,
    cur_REDEMPTION_AMT_HKD                                                             AS REDEMPTION_AMT_HKD,
    cur_DIV_REINVESTED_HKD                                                             AS DIV_REINVESTED_HKD,
    cur_CHG_DUE_TO_MARKET_HKD                                                          AS CHG_DUE_TO_MARKET_HKD,
    cur_CHG_DUE_TO_FXRATE_HKD                                                          AS CHG_DUE_TO_FXRATE_HKD,
    cur_TRANSFERS_IN_HKD                                                               AS TRANSFERS_IN_HKD,
    cur_TRANSFERS_OUT_HKD                                                              AS TRANSFERS_OUT_HKD,
    cur_ASSET_TRANSFERS_HKD                                                            AS ASSET_TRANSFERS_HKD,
    cur_MISC_TRANSFERS_HKD                                                             AS MISC_TRANSFERS_HKD,
    cur_NET_VALUATION_HKD                                                              AS NET_VALUATION_HKD,
    cur_SRC_CREATED_TS                                                                 AS SRC_CREATED_TS,
    cur_SRC_CREATED_TZ                                                                 AS SRC_CREATED_TZ,
    cur_EDL_CREATED_TS                                                                 AS EDL_CREATED_TS,
    CASE WHEN flag = 'C' THEN 'N'
         ELSE cur_CURRENT_VERSION END                                                  AS CURRENT_VERSION,
    cur_EDL_CURATED_TS                                                                 AS EDL_CURATED_TS,
    cur_EFFECTIVE_DT                                                                   AS EFFECTIVE_DT,
    cur_SRC                                                                            AS SRC
FROM ${TEMP_DB}.temp_bf_source_of_funds_jhisdh
WHERE flag IN ('C', 'E')
;
