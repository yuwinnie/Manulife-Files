DROP TABLE IF EXISTS inv_aum_typed_qa.dpl_gngnco ;
DROP TABLE IF EXISTS inv_aum_typed_qa.dpl_actran ;
DROP TABLE IF EXISTS inv_aum_typed_qa.dpl_acpos  ;
DROP TABLE IF EXISTS inv_aum_typed_qa.dpl_esmgr  ;
DROP TABLE IF EXISTS inv_aum_typed_qa.dpl_gncode ;
DROP TABLE IF EXISTS inv_aum_typed_qa.dpl_gnexch ;
DROP TABLE IF EXISTS inv_aum_typed_qa.dpl_mfacct ;
DROP TABLE IF EXISTS inv_aum_typed_qa.dpl_mfbr   ;
DROP TABLE IF EXISTS inv_aum_typed_qa.dpl_mfcl   ;
DROP TABLE IF EXISTS inv_aum_typed_qa.dpl_mfclac ;
DROP TABLE IF EXISTS inv_aum_typed_qa.dpl_mfclcl ;
DROP TABLE IF EXISTS inv_aum_typed_qa.dpl_mfrr   ;
DROP TABLE IF EXISTS inv_aum_typed_qa.dpl_mfrrus ;
DROP TABLE IF EXISTS inv_aum_typed_qa.dpl_mfsc   ;
DROP TABLE IF EXISTS inv_aum_typed_qa.dpl_tpcont ;
DROP TABLE IF EXISTS inv_aum_typed_qa.dpl_tpkptl ;


CREATE TABLE inv_aum_typed_qa.dpl_gngnco (addr STRING
, city STRING
, pc STRING
, ph STRING
, prov STRING
, name STRING
, company STRING
, last_usr STRING
, last_dt DATE
, last_time STRING
, fscl_yrnd STRING
, rtnd_earn INT
, fst_bal INT
, lst_bal INT
, fst_pl INT
, lst_pl INT
, crnt_fscl INT
, short_name STRING
, gst_ref STRING
, db_name STRING
, cr_rpts_cur STRING
, post_delay INT
, primary_mkt STRING
, consoldt STRING
, stmt_on STRING
, fund_type STRING
, language STRING
, addr2 STRING
, broker_code STRING
, process_timestamp timestamp)
PARTITIONED BY (process_date DATE)
STORED AS ORC;

CREATE TABLE inv_aum_typed_qa.dpl_actran (stl_dt DATE
, last_time STRING
, acct STRING
, last_dt DATE
, proc_dt DATE
, for_acctng STRING
, ref_num STRING
, cusip STRING
, qty DECIMAL(20,5)
, print STRING
, settled STRING
, tran_code STRING
, descr STRING
, rr STRING
, last_usr STRING
, seq INT
, amt DECIMAL(20,5)
, stmt_sort INT
, time_seq INT
, oi_num STRING
, trd_ref STRING
, int_calc STRING
, int_val DECIMAL(38,2)
, cost DECIMAL(17,2)
, exch_rt DECIMAL(21,6)
, on_stmt STRING
, trd_dt DATE
, post_type STRING
, fund_type STRING
, broker_code STRING
, process_timestamp timestamp
)
PARTITIONED BY (process_date DATE)
STORED AS ORC
;


CREATE TABLE inv_aum_typed_qa.dpl_acpos (acct STRING
, cusip STRING
, rr STRING
, pend_qty DECIMAL(19,4)
, cur_cost DECIMAL(17,2)
, cur_qty DECIMAL(19,4)
, seg_qty DECIMAL(19,4)
, memo_only STRING
, memo_qty DECIMAL(19,4)
, memo_cost DECIMAL(17,2)
, short STRING
, last_usr STRING
, last_dt DATE
, last_time STRING
, mkt_val DECIMAL(17,2)
, ln_val DECIMAL(17,2)
, sfk_qty DECIMAL(19,4)
, sgreq STRING
, nm_loan_val_house STRING
, nm_loan_val_reg STRING
, nm_mkt_val STRING
, nm_qty STRING
, undr_cusip STRING
, nm_sfk_qty STRING
, off_qty DECIMAL(19,4)
, nm_wmv STRING
, nm_loan_val_custom STRING
, cost_stat STRING
, cost_stat_ovrd STRING
, cost_stat_ovrd_usr STRING
, cost_stat_ovrd_dt DATE
, cost_stat_calc STRING
, cost_stat_asof_dt DATE
, cost_stat_asof_dt_calc DATE
, cost_stat_asof_dt_ovrd DATE
, cost_stat_ovrd_memo STRING
, broker_code STRING
, process_timestamp timestamp
)
PARTITIONED BY (process_date DATE)
STORED AS ORC;

CREATE TABLE inv_aum_typed_qa.dpl_esmgr (fund_code STRING
, fund_type STRING
, trd_acct STRING
, box_acct STRING
, last_dt DATE
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
, process_timestamp timestamp
)
PARTITIONED BY (process_date DATE)
STORED AS ORC;

CREATE TABLE inv_aum_typed_qa.dpl_gncode (code STRING
, code_value STRING
, last_dt DATE
, descr STRING
, subsystem STRING
, last_time STRING
, last_usr STRING
, table_name STRING
, dec_value DECIMAL(23,8)
, broker_code STRING
, process_timestamp timestamp
)
PARTITIONED BY (process_date DATE)
STORED AS ORC;

CREATE TABLE inv_aum_typed_qa.dpl_gnexch (rate DECIMAL(23,8)
, buy_fds STRING
, sell_fds STRING
, tran_type STRING
, exch_dt DATE
, max_amt DECIMAL(17,2)
, last_dt DATE
, last_time STRING
, last_usr STRING
, exch_time STRING
, broker_code STRING
, process_timestamp timestamp
)
PARTITIONED BY (process_date DATE)
STORED AS ORC;

CREATE TABLE inv_aum_typed_qa.dpl_mfacct (acct STRING
, fund_type STRING
, last_dt DATE
, last_time STRING
, last_usr STRING
, code STRING
, suspense STRING
, descr STRING
, broker_code STRING
, process_timestamp timestamp
)
PARTITIONED BY (process_date DATE)
STORED AS ORC;

CREATE TABLE inv_aum_typed_qa.dpl_mfbr (branch STRING
, last_dt DATE
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
, process_timestamp timestamp
) 
PARTITIONED BY (process_date DATE)
STORED AS ORC;


CREATE TABLE inv_aum_typed_qa.dpl_mfcl (client INT
, rr STRING
, residence STRING
, nrt STRING
, stat STRING
, np41 STRING
, branch STRING
, last_dt DATE
, open_dt DATE
, last_time STRING
, last_usr STRING
, close_dt DATE
, osc_exempt STRING
, consoldt STRING
, employee STRING
, deceased_dt DATE
, np41b STRING
, np41c STRING
, recip_type STRING
, client_id STRING
, house_id STRING
, citizen_iso STRING
, psd1 STRING
, psd2 STRING
, presumed_us STRING
, bank_crlimit DECIMAL(17,2)
, ni54a STRING
, ni54b STRING
, ni54c STRING
, ni54d STRING
, ni54e DATE
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
, process_timestamp timestamp
)
PARTITIONED BY (process_date DATE)
STORED AS ORC;


CREATE TABLE inv_aum_typed_qa.dpl_mfclac (acct STRING
, stat STRING
, del_code STRING
, comm_type STRING
, comm_amt DECIMAL(17,4)
, fund_type STRING
, acct_type STRING
, del_inst STRING
, sss_loc STRING
, sss_fins STRING
, sss_dep STRING
, last_seg_dt DATE
, cons_lvl INT
, sss_acct STRING
, sss_name STRING
, debit_int STRING
, credit_int STRING
, class STRING
, cr_limit INT
, cr_check STRING
, opt_appr STRING
, net_rr DECIMAL(19,4)
, acct_sub STRING
, client INT
, discr STRING
, appr_by STRING
, open_dt DATE
, last_dt DATE
, last_time STRING
, last_usr STRING
, close_dt DATE
, last_stmt DATE
, last_trd_dt DATE
, last_int DATE
, delinq STRING
, proj_cash DECIMAL(17,2)
, cur_cash DECIMAL(17,2)
, mkt_val DECIMAL(17,2)
, pmt_code STRING
, firm_lv DECIMAL(17,2)
, rr STRING
, open_bal_mth DECIMAL(17,2)
, open_bal_yr DECIMAL(17,2)
, open_bk_mth DECIMAL(17,2)
, open_bk_yr DECIMAL(17,2)
, trades_ok STRING
, last_act_dt DATE
, evaluate STRING
, oi_ctrl STRING
, reg_code STRING
, `buypwr-rtm` DECIMAL(17,2)
, `equity-rts` DECIMAL(17,2)
, dir_part STRING
, residence STRING
, port_type STRING
, ctrl_codes STRING
, dcs_cuid STRING
, dcs_unit_code STRING
, np41 STRING
, inst_code STRING
, trader_code STRING
, asset_chg DECIMAL(17,2)
, ticket_base INT
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
, rrif_open_dt DATE
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
, ni54e DATE
, esir_num STRING
, dtc_part_num STRING
, addt_party_id STRING
, addt_party_acct STRING
, addt_party_instr1 STRING
, addt_party_instr2 STRING
, incept_dt DATE
, rrsp_non_res_dt DATE
, crest_id STRING
, crest_comments STRING
, leveraged STRING
, dsl_cuid STRING
, robj_code STRING
, last_roi_dt DATE
, plan_end_dt DATE
, contri_end_dt DATE
, initial_tran_dt DATE
, kyc_upd_dt DATE
, monthly_stmt STRING
, crm2_incept_dt DATE
, lead_acct STRING
, cp_related STRING
, counterparty INT
, crm2_eligible STRING
, broker_code STRING
, process_timestamp timestamp
)
PARTITIONED BY (process_date DATE)
STORED AS ORC;

CREATE TABLE inv_aum_typed_qa.dpl_mfclcl (class STRING
, descr STRING
, cl_type STRING
, last_dt DATE
, last_time STRING
, last_usr STRING
, table_type INT
, rpt_typ STRING
, seg_proc STRING
, reg_type STRING
, recip_type STRING
, broker_code STRING
, process_timestamp timestamp
)
PARTITIONED BY (process_date DATE)
STORED AS ORC;

CREATE TABLE inv_aum_typed_qa.dpl_mfrr (rr STRING
, last_name STRING
, first_name STRING
, addr1 STRING
, addr2 STRING
, pcode STRING
, ph_home STRING
, ph_work STRING
, open_dt DATE
, close_dt DATE
, branch STRING
, last_dt DATE
, last_time STRING
, last_usr STRING
, trades_ok STRING
, short_name STRING
, stmt_copies INT
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
, holdback_pct DECIMAL(17,2)
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
, process_timestamp timestamp
)
PARTITIONED BY (process_date DATE)
STORED AS ORC;

CREATE TABLE inv_aum_typed_qa.dpl_mfrrus (usr STRING
, rr STRING
, last_dt DATE
, last_time STRING
, last_usr STRING
, sys_gen STRING
, broker_code STRING
, process_timestamp timestamp
)
PARTITIONED BY (process_date DATE)
STORED AS ORC;

CREATE TABLE inv_aum_typed_qa.dpl_mfsc (cusip STRING
, short_name STRING
, symbol STRING
, sec_code STRING
, port_sort STRING
, fund_type STRING
, stl_override STRING
, last_trd_prc DECIMAL(38,5)
, stl_dt DATE
, trailers STRING
, prc_factor DECIMAL(20,5)
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
, div_days INT
, priv_days INT
, int_rate STRING
, maturity_dt STRING
, accr_dt DATE
, mkt_maker STRING
, trades_opt STRING
, last_usr STRING
, last_dt DATE
, last_time STRING
, stl_days INT
, class INT
, cdn_int_calc STRING
, coupons STRING
, num_decimals INT
, exp_dt DATE
, strike_prc DECIMAL(20,5)
, undr_cusip STRING
, stat STRING
, added_dt DATE
, delisted_dt DATE
, bid_prc DECIMAL(38,5)
, ask_prc DECIMAL(38,5)
, osc_exempt STRING
, seg_pri INT
, seg_pri_or INT
, mgn_code STRING
, opt_call STRING
, trades_ok STRING
, bucket STRING
, stmt_prc DECIMAL(38,5)
, hold_prc STRING
, prc_dt DATE
, prc_chg_pdg STRING
, man_prc STRING
, cds_elig STRING
, cns_elig STRING
, sec_agents STRING
, sec_comment STRING
, certificated STRING
, tax_code STRING
, mbs_pr_amt DECIMAL(17,2)
, mbs_pool STRING
, disc_note STRING
, fixed_rate STRING
, cds_subtype STRING
, cds_debt_sub STRING
, unit_prc DECIMAL(18,3)
, last_act_dt DATE
, cds_stat STRING
, prc_source STRING
, qsc_exempt STRING
, nids_elig STRING
, dcs_ecs_elig STRING
, first_cpn_dt DATE
, dsl_stat STRING
, racode STRING
, qssp_elig STRING
, ccpc_elig STRING
, css_elig STRING
, mf_cat STRING
, red_mgn STRING
, redeemable STRING
, convertible STRING
, deliverables DECIMAL(20,5)
, fully_foreign_dt DATE
, industry STRING
, sector STRING
, sub_sector STRING
, region STRING
, aa_data_dt DATE
, aa_data_src STRING
, seg_fund STRING
, isin STRING
, monitor STRING
, cert_num STRING
, reg_type STRING
, int_cmpd_period STRING
, day_count_basis STRING
, pymt_freq_units STRING
, cds_day_count STRING
, port_class STRING
, pymt_day INT
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
, days_prc_cur INT
, pymt_count_basis STRING
, broker_code STRING
, process_timestamp timestamp
)
PARTITIONED BY (process_date DATE)
STORED AS ORC;

CREATE TABLE inv_aum_typed_qa.dpl_tpcont (acct STRING
, fill_prc_int STRING
, fill_qty STRING
, principal STRING
, mkt_code STRING
, hold_mail STRING
, cusip STRING
, buy STRING
, comm_clt_ovrd STRING
, trailers STRING
, kp_tlr_lines INT
, other_side STRING
, exch_rate DECIMAL(21,6)
, trd_dt DATE
, stl_dt DATE
, rr STRING
, trd_num INT
, inv_tsfr DECIMAL(21,6)
, comm_cust DECIMAL(17,2)
, comm_no_min STRING
, accr_int DECIMAL(17,2)
, sec_fee DECIMAL(17,2)
, ny_tax DECIMAL(17,2)
, exch_amt DECIMAL(17,2)
, processed STRING
, qty DECIMAL(20,5)
, gross DECIMAL(17,2)
, net DECIMAL(17,2)
, fund_type STRING
, comm_full STRING
, cxl STRING
, other_tax DECIMAL(17,2)
, other_tax_type STRING
, comm_split DECIMAL(19,4)
, comm_basic DECIMAL(17,2)
, comm_prefig STRING
, fill_prc_ext STRING
, client_side STRING
, comm_rr DECIMAL(17,2)
, comm_cents DECIMAL(19,4)
, comm_thru STRING
, opt_trd_type STRING
, proc_dt DATE
, printed STRING
, cxld STRING
, last_dt DATE
, last_time STRING
, last_usr STRING
, entr_dt DATE
, entr_time STRING
, entr_usr STRING
, ref_num STRING
, cxl_proc_dt DATE
, cxl_trd_num INT
, oi_num STRING
, comm_rr_net DECIMAL(17,2)
, bucket STRING
, comm_trd_ct STRING
, notes STRING
, comm_rr_fx DECIMAL(17,2)
, comm_rr_net_fx DECIMAL(17,2)
, inst_code STRING
, order_dt DATE
, order_num INT
, cc75 STRING
, cc77 STRING
, broker_code STRING
, process_timestamp timestamp
)
PARTITIONED BY (process_date DATE)
STORED AS ORC;

CREATE TABLE inv_aum_typed_qa.dpl_tpkptl (proc_dt DATE
, trd_num INT
, last_dt DATE
, tlr_num INT
, tlr_text STRING
, client_side STRING
, last_time STRING
, last_usr STRING
, broker_code STRING
, process_timestamp timestamp
)
PARTITIONED BY (process_date DATE)
STORED AS ORC;