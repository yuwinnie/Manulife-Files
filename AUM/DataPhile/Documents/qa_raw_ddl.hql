DROP TABLE IF EXISTS inv_aum_raw_qa.dpl_gngnco ;
DROP TABLE IF EXISTS inv_aum_raw_qa.dpl_actran ;
DROP TABLE IF EXISTS inv_aum_raw_qa.dpl_acpos  ;
DROP TABLE IF EXISTS inv_aum_raw_qa.dpl_esmgr  ;
DROP TABLE IF EXISTS inv_aum_raw_qa.dpl_gncode ;
DROP TABLE IF EXISTS inv_aum_raw_qa.dpl_gnexch ;
DROP TABLE IF EXISTS inv_aum_raw_qa.dpl_mfacct ;
DROP TABLE IF EXISTS inv_aum_raw_qa.dpl_mfbr   ;
DROP TABLE IF EXISTS inv_aum_raw_qa.dpl_mfcl   ;
DROP TABLE IF EXISTS inv_aum_raw_qa.dpl_mfclac ;
DROP TABLE IF EXISTS inv_aum_raw_qa.dpl_mfclcl ;
DROP TABLE IF EXISTS inv_aum_raw_qa.dpl_mfrr   ;
DROP TABLE IF EXISTS inv_aum_raw_qa.dpl_mfrrus ;
DROP TABLE IF EXISTS inv_aum_raw_qa.dpl_mfsc   ;
DROP TABLE IF EXISTS inv_aum_raw_qa.dpl_tpcont ;
DROP TABLE IF EXISTS inv_aum_raw_qa.dpl_tpkptl ;


CREATE TABLE inv_aum_raw_qa.test_i (
 addr char(1)
, city varchar(20)
, pc char(10)
)
COMMENT 'Data from test'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
LOCATION "/apps/inv_qa/aum/raw/dpl/PUB.test_i/";

CREATE EXTERNAL TABLE inv_aum_raw_qa.dpl_actran (stl_dt STRING
, last_time STRING
, acct STRING
, last_dt STRING
, proc_dt STRING
, for_acctng STRING
, ref_num STRING
, cusip STRING
, qty STRING
, prSTRING STRING
, settled STRING
, tran_code STRING
, descr STRING
, rr STRING
, last_usr STRING
, seq STRING
, amt STRING
, stmt_sort STRING
, time_seq STRING
, oi_num STRING
, trd_ref STRING
, STRING_calc STRING
, STRING_val STRING
, cost STRING
, exch_rt STRING
, on_stmt STRING
, trd_dt STRING
, post_type STRING
, fund_type STRING
, broker_code STRING
)
COMMENT 'Data from actran'
PARTITIONED BY (`process_timestamp` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
LOCATION "/apps/inv_qa/aum/raw/dpl/PUB.actran/";


CREATE EXTERNAL TABLE inv_aum_raw_qa.dpl_acpos (acct STRING
, cusip STRING
, rr STRING
, pend_qty STRING
, cur_cost STRING
, cur_qty STRING
, seg_qty STRING
, memo_only STRING
, memo_qty STRING
, memo_cost STRING
, short STRING
, last_usr STRING
, last_dt STRING
, last_time STRING
, mkt_val STRING
, ln_val STRING
, sfk_qty STRING
, sgreq STRING
, nm_loan_val_house STRING
, nm_loan_val_reg STRING
, nm_mkt_val STRING
, nm_qty STRING
, undr_cusip STRING
, nm_sfk_qty STRING
, off_qty STRING
, nm_wmv STRING
, nm_loan_val_custom STRING
, cost_stat STRING
, cost_stat_ovrd STRING
, cost_stat_ovrd_usr STRING
, cost_stat_ovrd_dt STRING
, cost_stat_calc STRING
, cost_stat_asof_dt STRING
, cost_stat_asof_dt_calc STRING
, cost_stat_asof_dt_ovrd STRING
, cost_stat_ovrd_memo STRING
, broker_code STRING
)
COMMENT 'Data from actran'
PARTITIONED BY (`process_timestamp` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
LOCATION "/apps/inv_qa/aum/raw/dpl/PUB.acpos/";

CREATE EXTERNAL TABLE inv_aum_raw_qa.dpl_esmgr (fund_code STRING
, fund_type STRING
, trd_acct STRING
, box_acct STRING
, last_dt STRING
, last_time STRING
, last_usr STRING
, mkt_code STRING
, name STRING
, init_dest STRING
, batch_dest STRING
, send_nfu STRING
, send_transfer STRING
, approved STRING
, bucket_s STRING
, bucket_w STRING
, nfu_group STRING
, epa STRING
, url STRING
, french_name STRING
, broker_code STRING
)
COMMENT 'Data from esmgr'
PARTITIONED BY (`process_timestamp` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
LOCATION "/apps/inv_qa/aum/raw/dpl/PUB.esmgr/";

CREATE EXTERNAL TABLE inv_aum_raw_qa.dpl_gncode (code STRING
, code_value STRING
, last_dt STRING
, descr STRING
, subsystem STRING
, last_time STRING
, last_usr STRING
, table_name STRING
, dec_value STRING
, broker_code STRING
)
COMMENT 'Data from gncode'
PARTITIONED BY (`process_timestamp` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
LOCATION "/apps/inv_qa/aum/raw/dpl/PUB.gncode/";

CREATE EXTERNAL TABLE inv_aum_raw_qa.dpl_gnexch (rate STRING
, buy_fds STRING
, sell_fds STRING
, tran_type STRING
, exch_dt STRING
, max_amt STRING
, last_dt STRING
, last_time STRING
, last_usr STRING
, exch_time STRING
, broker_code STRING
)
COMMENT 'Data from gnexch'
PARTITIONED BY (`process_timestamp` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
LOCATION "/apps/inv_qa/aum/raw/dpl/PUB.gnexch/";

CREATE EXTERNAL TABLE inv_aum_raw_qa.dpl_mfacct (acct STRING
, fund_type STRING
, last_dt STRING
, last_time STRING
, last_usr STRING
, code STRING
, suspense STRING
, descr STRING
, broker_code STRING
)
COMMENT 'Data from mfacct'
PARTITIONED BY (`process_timestamp` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
LOCATION "/apps/inv_qa/aum/raw/dpl/PUB.mfacct/";


CREATE EXTERNAL TABLE inv_aum_raw_qa.dpl_mfbr (branch STRING
, last_dt STRING
, last_time STRING
, last_usr STRING
, short_name STRING
, addr STRING
, ph_fax STRING
, ph_work STRING
, city STRING
, country STRING
, province STRING
, pcode STRING
, ibcode STRING
, ph_tollfree STRING
, bank_id STRING
, oth_br_code STRING
, residence STRING
, bridge_enabled STRING
, behaviour_group_id STRING
, broker_code STRING
) 
COMMENT 'Data from mfbr'
PARTITIONED BY (`process_timestamp` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
LOCATION "/apps/inv_qa/aum/raw/dpl/PUB.mfbr/";


CREATE EXTERNAL TABLE inv_aum_raw_qa.dpl_mfcl (client STRING
, rr STRING
, residence STRING
, nrt STRING
, stat STRING
, np41 STRING
, branch STRING
, last_dt STRING
, open_dt STRING
, last_time STRING
, last_usr STRING
, close_dt STRING
, osc_exempt STRING
, consoldt STRING
, employee STRING
, deceased_dt STRING
, np41b STRING
, np41c STRING
, recip_type STRING
, client_id STRING
, house_id STRING
, citizen_iso STRING
, psd1 STRING
, psd2 STRING
, presumed_us STRING
, bank_crlimit STRING
, ni54a STRING
, ni54b STRING
, ni54c STRING
, ni54d STRING
, ni54e STRING
, corp_id STRING
, corp_type STRING
, monitor STRING
, client_sub_type STRING
, neq_id STRING
, etax STRING
, tax_hold STRING
, dormant STRING
, giin STRING
, irs_cost_method STRING
, consent_to_share STRING
, alternate_id STRING
, classification STRING
, broker_code STRING
)
COMMENT 'Data from mfcl'
PARTITIONED BY (`process_timestamp` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
LOCATION "/apps/inv_qa/aum/raw/dpl/PUB.mfcl/";


CREATE EXTERNAL TABLE inv_aum_raw_qa.dpl_mfclac (acct STRING
, stat STRING
, del_code STRING
, comm_type STRING
, comm_amt STRING
, fund_type STRING
, acct_type STRING
, del_inst STRING
, sss_loc STRING
, sss_fins STRING
, sss_dep STRING
, last_seg_dt STRING
, cons_lvl STRING
, sss_acct STRING
, sss_name STRING
, debit_STRING STRING
, credit_STRING STRING
, class STRING
, cr_limit STRING
, cr_check STRING
, opt_appr STRING
, net_rr STRING
, acct_sub STRING
, client STRING
, discr STRING
, appr_by STRING
, open_dt STRING
, last_dt STRING
, last_time STRING
, last_usr STRING
, close_dt STRING
, last_stmt STRING
, last_trd_dt STRING
, last_STRING STRING
, delinq STRING
, proj_cash STRING
, cur_cash STRING
, mkt_val STRING
, pmt_code STRING
, firm_lv STRING
, rr STRING
, open_bal_mth STRING
, open_bal_yr STRING
, open_bk_mth STRING
, open_bk_yr STRING
, trades_ok STRING
, last_act_dt STRING
, evaluate STRING
, oi_ctrl STRING
, reg_code STRING
, `buypwr-rtm` STRING
, `equity-rts` STRING
, dir_part STRING
, residence STRING
, port_type STRING
, ctrl_codes STRING
, dcs_cuid STRING
, dcs_unit_code STRING
, np41 STRING
, inst_code STRING
, trader_code STRING
, asset_chg STRING
, ticket_base STRING
, close_reason STRING
, stmt_cost STRING
, np41b STRING
, np41c STRING
, reason_code STRING
, reason STRING
, pension_reg STRING
, relation STRING
, ifb_code STRING
, us_clg_agency STRING
, nasd_exec_bkr STRING
, nasd_clg_bkr STRING
, nasd_num_bkr STRING
, gg_acct STRING
, gg_code STRING
, rrif_bd STRING
, comm_no_min STRING
, rrif_open_dt STRING
, nm_cash STRING
, nm_loan_val_house STRING
, nm_loan_val_reg STRING
, nm_mkt_val STRING
, trustee STRING
, itf STRING
, success_rights STRING
, use_uno_hist STRING
, div_code STRING
, kyc_stat STRING
, nm_cap_reqd_reg STRING
, nm_wmv STRING
, dest_rt STRING
, nm_cap_reqd_house STRING
, nm_cap_reqd_custom STRING
, nm_loan_val_custom STRING
, ibcode STRING
, dtc_agent_id STRING
, dtc_inst_id STRING
, dtc_bk_acct STRING
, ni54a STRING
, ni54b STRING
, ni54c STRING
, ni54d STRING
, ni54e STRING
, esir_num STRING
, dtc_part_num STRING
, addt_party_id STRING
, addt_party_acct STRING
, addt_party_instr1 STRING
, addt_party_instr2 STRING
, incept_dt STRING
, rrsp_non_res_dt STRING
, crest_id STRING
, crest_comments STRING
, leveraged STRING
, dsl_cuid STRING
, robj_code STRING
, last_roi_dt STRING
, plan_end_dt STRING
, contri_end_dt STRING
, initial_tran_dt STRING
, kyc_upd_dt STRING
, monthly_stmt STRING
, crm2_incept_dt STRING
, lead_acct STRING
, cp_related STRING
, counterparty STRING
, crm2_eligible STRING
, broker_code STRING
)
COMMENT 'Data from mfclac'
PARTITIONED BY (`process_timestamp` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
LOCATION "/apps/inv_qa/aum/raw/dpl/PUB.mfclac/";

CREATE EXTERNAL TABLE inv_aum_raw_qa.dpl_mfclcl (class STRING
, descr STRING
, cl_type STRING
, last_dt STRING
, last_time STRING
, last_usr STRING
, table_type STRING
, rpt_typ STRING
, seg_proc STRING
, reg_type STRING
, recip_type STRING
, broker_code STRING
)
COMMENT 'Data from mfclclss'
PARTITIONED BY (`process_timestamp` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
LOCATION "/apps/inv_qa/aum/raw/dpl/PUB.mfclclss/";

CREATE EXTERNAL TABLE inv_aum_raw_qa.dpl_mfrr (rr STRING
, last_name STRING
, first_name STRING
, addr1 STRING
, addr2 STRING
, pcode STRING
, ph_home STRING
, ph_work STRING
, open_dt STRING
, close_dt STRING
, branch STRING
, last_dt STRING
, last_time STRING
, last_usr STRING
, trades_ok STRING
, short_name STRING
, stmt_copies STRING
, ph_fax STRING
, other_id STRING
, hold_mail STRING
, edit_grp STRING
, email STRING
, city STRING
, country STRING
, province STRING
, ph_tollfree STRING
, language STRING
, long_name STRING
, rr_group STRING
, job_code STRING
, reserve_acct STRING
, holdback_pct STRING
, comm_acct STRING
, trd_name STRING
, website STRING
, roi_method STRING
, authorized_list_code STRING
, under_supervision STRING
, bm STRING
, rr_type STRING
, share_office STRING
, other_business STRING
, other_business_type STRING
, broker_code STRING
)
COMMENT 'Data from mfrr'
PARTITIONED BY (`process_timestamp` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
LOCATION "/apps/inv_qa/aum/raw/dpl/PUB.mfrr/";

CREATE EXTERNAL TABLE inv_aum_raw_qa.dpl_mfrrus (usr STRING
, rr STRING
, last_dt STRING
, last_time STRING
, last_usr STRING
, sys_gen STRING
, broker_code STRING
)
COMMENT 'Data from mfrrus'
PARTITIONED BY (`process_timestamp` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
LOCATION "/apps/inv_qa/aum/raw/dpl/PUB.mfrrus/";

CREATE EXTERNAL TABLE inv_aum_raw_qa.dpl_mfsc (cusip STRING
, short_name STRING
, symbol STRING
, sec_code STRING
, port_sort STRING
, fund_type STRING
, stl_override STRING
, last_trd_prc STRING
, stl_dt STRING
, trailers STRING
, prc_factor STRING
, country STRING
, mkt_code STRING
, st_code STRING
, reg_code STRING
, dual_pay STRING
, bbs_elig STRING
, vse_elig STRING
, dtc_elig STRING
, mdw_elig STRING
, euro_elig STRING
, rrsp_elig STRING
, name_chg STRING
, recommend STRING
, pymt_freq STRING
, div_days STRING
, priv_days STRING
, STRING_rate STRING
, maturity_dt STRING
, accr_dt STRING
, mkt_maker STRING
, trades_opt STRING
, last_usr STRING
, last_dt STRING
, last_time STRING
, stl_days STRING
, class STRING
, cdn_STRING_calc STRING
, coupons STRING
, num_decimals STRING
, exp_dt STRING
, strike_prc STRING
, undr_cusip STRING
, stat STRING
, added_dt STRING
, delisted_dt STRING
, bid_prc STRING
, ask_prc STRING
, osc_exempt STRING
, seg_pri STRING
, seg_pri_or STRING
, mgn_code STRING
, opt_call STRING
, trades_ok STRING
, bucket STRING
, stmt_prc STRING
, hold_prc STRING
, prc_dt STRING
, prc_chg_pdg STRING
, man_prc STRING
, cds_elig STRING
, cns_elig STRING
, sec_agents STRING
, sec_comment STRING
, certificated STRING
, tax_code STRING
, mbs_pr_amt STRING
, mbs_pool STRING
, disc_note STRING
, fixed_rate STRING
, cds_subtype STRING
, cds_debt_sub STRING
, unit_prc STRING
, last_act_dt STRING
, cds_stat STRING
, prc_source STRING
, qsc_exempt STRING
, nids_elig STRING
, dcs_ecs_elig STRING
, first_cpn_dt STRING
, dsl_stat STRING
, racode STRING
, qssp_elig STRING
, ccpc_elig STRING
, css_elig STRING
, mf_cat STRING
, red_mgn STRING
, redeemable STRING
, convertible STRING
, deliverables STRING
, fully_foreign_dt STRING
, industry STRING
, sector STRING
, sub_sector STRING
, region STRING
, aa_data_dt STRING
, aa_data_src STRING
, seg_fund STRING
, isin STRING
, monitor STRING
, cert_num STRING
, reg_type STRING
, STRING_cmpd_period STRING
, day_count_basis STRING
, pymt_freq_units STRING
, cds_day_count STRING
, port_class STRING
, pymt_day STRING
, industry_grp STRING
, gics_gvkey STRING
, sub_industry STRING
, undr_symbol STRING
, gics_iid STRING
, callable STRING
, drs_elig STRING
, billing_source STRING
, firm_lv_lng STRING
, firm_lv_sht STRING
, prc_stat STRING
, days_prc_cur STRING
, pymt_count_basis STRING
, broker_code STRING
)
COMMENT 'Data from mfsc'
PARTITIONED BY (`process_timestamp` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
LOCATION "/apps/inv_qa/aum/raw/dpl/PUB.mfsc/";

CREATE EXTERNAL TABLE inv_aum_raw_qa.dpl_tpcont (acct STRING
, fill_prc_STRING STRING
, fill_qty STRING
, principal STRING
, mkt_code STRING
, hold_mail STRING
, cusip STRING
, buy STRING
, comm_clt_ovrd STRING
, trailers STRING
, kp_tlr_lines STRING
, other_side STRING
, exch_rate STRING
, trd_dt STRING
, stl_dt STRING
, rr STRING
, trd_num STRING
, inv_tsfr STRING
, comm_cust STRING
, comm_no_min STRING
, accr_STRING STRING
, sec_fee STRING
, ny_tax STRING
, exch_amt STRING
, processed STRING
, qty STRING
, gross STRING
, net STRING
, fund_type STRING
, comm_full STRING
, cxl STRING
, other_tax STRING
, other_tax_type STRING
, comm_split STRING
, comm_basic STRING
, comm_prefig STRING
, fill_prc_ext STRING
, client_side STRING
, comm_rr STRING
, comm_cents STRING
, comm_thru STRING
, opt_trd_type STRING
, proc_dt STRING
, prSTRINGed STRING
, cxld STRING
, last_dt STRING
, last_time STRING
, last_usr STRING
, entr_dt STRING
, entr_time STRING
, entr_usr STRING
, ref_num STRING
, cxl_proc_dt STRING
, cxl_trd_num STRING
, oi_num STRING
, comm_rr_net STRING
, bucket STRING
, comm_trd_ct STRING
, notes STRING
, comm_rr_fx STRING
, comm_rr_net_fx STRING
, inst_code STRING
, order_dt STRING
, order_num STRING
, cc75 STRING
, cc77 STRING
, broker_code STRING
)
COMMENT 'Data from tpcont'
PARTITIONED BY (`process_timestamp` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
LOCATION "/apps/inv_qa/aum/raw/dpl/PUB.tpcont/";

CREATE EXTERNAL TABLE inv_aum_raw_qa.dpl_tpkptl (proc_dt STRING
, trd_num STRING
, last_dt STRING
, tlr_num STRING
, tlr_text STRING
, client_side STRING
, last_time STRING
, last_usr STRING
, broker_code STRING
)
COMMENT 'Data from tpkptl'
PARTITIONED BY (`process_timestamp` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
LOCATION "/apps/inv_qa/aum/raw/dpl/PUB.tpkptl/";

