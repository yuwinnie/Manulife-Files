--shcema name/table name/partition


set hivevar:s_process_date = '2020-05-02';

alter   table   inv_aum_raw_qa.test_dpl_actran    add partition   (process_date = ${s_process_date});
analyze table   inv_aum_raw_qa.test_dpl_actran    partition       (process_date) compute statistics;

alter   table   inv_aum_raw_qa.test_dpl_gngnco    add partition   (process_date = ${s_process_date});
analyze table   inv_aum_raw_qa.test_dpl_gngnco    partition       (process_date) compute statistics;

alter   table   inv_aum_raw_qa.test_dpl_mfclac    add partition   (process_date = ${s_process_date});
analyze table   inv_aum_raw_qa.test_dpl_mfclac    partition       (process_date) compute statistics;
 
alter   table   inv_aum_raw_qa.test_dpl_mfacct    add partition   (process_date = ${s_process_date});
analyze table   inv_aum_raw_qa.test_dpl_mfacct    partition       (process_date) compute statistics;
 
alter   table   inv_aum_raw_qa.test_dpl_mfrr      add partition   (process_date = ${s_process_date});
analyze table   inv_aum_raw_qa.test_dpl_mfrr      partition       (process_date) compute statistics;
 
alter   table   inv_aum_raw_qa.test_dpl_esmgr     add partition   (process_date = ${s_process_date});
analyze table   inv_aum_raw_qa.test_dpl_esmgr     partition       (process_date) compute statistics;

alter   table   inv_aum_raw_qa.test_dpl_acpos     add partition   (process_date = ${s_process_date});
analyze table   inv_aum_raw_qa.test_dpl_acpos     partition       (process_date) compute statistics;
 
alter   table   inv_aum_raw_qa.test_dpl_gnexch    add partition   (process_date = ${s_process_date});
analyze table   inv_aum_raw_qa.test_dpl_gnexch    partition       (process_date) compute statistics;

alter   table   inv_aum_raw_qa.test_dpl_tpkptl    add partition   (process_date = ${s_process_date});
analyze table   inv_aum_raw_qa.test_dpl_tpkptl    partition       (process_date) compute statistics;
 
alter   table   inv_aum_raw_qa.test_dpl_mfclcl    add partition   (process_date = ${s_process_date});
analyze table   inv_aum_raw_qa.test_dpl_mfclcl    partition       (process_date) compute statistics;
 
alter   table   inv_aum_raw_qa.test_dpl_mfsc      add partition   (process_date = ${s_process_date});
analyze table   inv_aum_raw_qa.test_dpl_mfsc      partition       (process_date) compute statistics;

alter   table   inv_aum_raw_qa.test_dpl_tpcont    add partition   (process_date = ${s_process_date});
analyze table   inv_aum_raw_qa.test_dpl_tpcont    partition       (process_date) compute statistics;
 
alter   table   inv_aum_raw_qa.test_dpl_mfbr      add partition   (process_date = ${s_process_date});
analyze table   inv_aum_raw_qa.test_dpl_mfbr      partition       (process_date) compute statistics;
 
alter   table   inv_aum_raw_qa.test_dpl_mfcl      add partition   (process_date = ${s_process_date});
analyze table   inv_aum_raw_qa.test_dpl_mfcl      partition       (process_date) compute statistics;
 
alter   table   inv_aum_raw_qa.test_dpl_mfrrus    add partition   (process_date = ${s_process_date});
analyze table   inv_aum_raw_qa.test_dpl_mfrrus    partition       (process_date) compute statistics;
 
alter   table   inv_aum_raw_qa.test_dpl_gncode    add partition   (process_date = ${s_process_date});
analyze table   inv_aum_raw_qa.test_dpl_gncode    partition       (process_date) compute statistics;

--**************************** Raw tables
show partitions inv_aum_raw_qa.test_dpl_actran ;
show partitions inv_aum_raw_qa.test_dpl_gngnco ;
show partitions inv_aum_raw_qa.test_dpl_mfclac ;
show partitions inv_aum_raw_qa.test_dpl_mfacct ;
show partitions inv_aum_raw_qa.test_dpl_mfrr ;
show partitions inv_aum_raw_qa.test_dpl_esmgr ;
show partitions inv_aum_raw_qa.test_dpl_acpos ;
show partitions inv_aum_raw_qa.test_dpl_gnexch ;
show partitions inv_aum_raw_qa.test_dpl_tpkptl ;
show partitions inv_aum_raw_qa.test_dpl_mfclcl ;
show partitions inv_aum_raw_qa.test_dpl_mfsc ;
show partitions inv_aum_raw_qa.test_dpl_tpcont ;
show partitions inv_aum_raw_qa.test_dpl_mfbr ;
show partitions inv_aum_raw_qa.test_dpl_mfcl ;
show partitions inv_aum_raw_qa.test_dpl_mfrrus ;
show partitions inv_aum_raw_qa.test_dpl_gncode ;


set hivevar:s_process_date = '2020-05-01';

alter   table   inv_aum_raw_qa.test_dpl_actran    drop partition   (process_date = ${s_process_date});
alter   table   inv_aum_raw_qa.test_dpl_gngnco    drop partition   (process_date = ${s_process_date});
alter   table   inv_aum_raw_qa.test_dpl_mfclac    drop partition   (process_date = ${s_process_date});
alter   table   inv_aum_raw_qa.test_dpl_mfacct    drop partition   (process_date = ${s_process_date});
alter   table   inv_aum_raw_qa.test_dpl_mfrr      drop partition   (process_date = ${s_process_date});
alter   table   inv_aum_raw_qa.test_dpl_esmgr     drop partition   (process_date = ${s_process_date});
alter   table   inv_aum_raw_qa.test_dpl_acpos     drop partition   (process_date = ${s_process_date});
alter   table   inv_aum_raw_qa.test_dpl_gnexch    drop partition   (process_date = ${s_process_date});
alter   table   inv_aum_raw_qa.test_dpl_tpkptl    drop partition   (process_date = ${s_process_date});
alter   table   inv_aum_raw_qa.test_dpl_mfclcl    drop partition   (process_date = ${s_process_date});
alter   table   inv_aum_raw_qa.test_dpl_mfsc      drop partition   (process_date = ${s_process_date});
alter   table   inv_aum_raw_qa.test_dpl_tpcont    drop partition   (process_date = ${s_process_date});
alter   table   inv_aum_raw_qa.test_dpl_mfbr      drop partition   (process_date = ${s_process_date});
alter   table   inv_aum_raw_qa.test_dpl_mfcl      drop partition   (process_date = ${s_process_date});
alter   table   inv_aum_raw_qa.test_dpl_mfrrus    drop partition   (process_date = ${s_process_date});
alter   table   inv_aum_raw_qa.test_dpl_gncode    drop partition   (process_date = ${s_process_date});

analyze table   inv_aum_raw_qa.test_dpl_actran    partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_gngnco    partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_mfclac    partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_mfacct    partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_mfrr      partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_esmgr     partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_acpos     partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_gnexch    partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_tpkptl    partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_mfclcl    partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_mfsc      partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_tpcont    partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_mfbr      partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_mfcl      partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_mfrrus    partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_gncode    partition       (process_date) compute statistics;

truncate table inv_aum_raw_qa.test_dpl_actran;
truncate table inv_aum_raw_qa.test_dpl_gngnco;
truncate table inv_aum_raw_qa.test_dpl_mfclac;
truncate table inv_aum_raw_qa.test_dpl_mfacct;
truncate table inv_aum_raw_qa.test_dpl_mfrr;
truncate table inv_aum_raw_qa.test_dpl_esmgr;
truncate table inv_aum_raw_qa.test_dpl_acpos;
truncate table inv_aum_raw_qa.test_dpl_gnexch;
truncate table inv_aum_raw_qa.test_dpl_tpkptl;
truncate table inv_aum_raw_qa.test_dpl_mfclcl;
truncate table inv_aum_raw_qa.test_dpl_mfsc;
truncate table inv_aum_raw_qa.test_dpl_tpcont;
truncate table inv_aum_raw_qa.test_dpl_mfbr;
truncate table inv_aum_raw_qa.test_dpl_mfcl;
truncate table inv_aum_raw_qa.test_dpl_mfrrus;
truncate table inv_aum_raw_qa.test_dpl_gncode;



--**************************** Typed tables
show partitions inv_aum_typed_qa.dpl_actran ;
show partitions inv_aum_typed_qa.dpl_gngnco ;
show partitions inv_aum_typed_qa.dpl_mfclac ;
show partitions inv_aum_typed_qa.dpl_mfacct ;
show partitions inv_aum_typed_qa.dpl_mfrr ;
show partitions inv_aum_typed_qa.dpl_esmgr ;
show partitions inv_aum_typed_qa.dpl_acpos ;
show partitions inv_aum_typed_qa.dpl_gnexch ;
show partitions inv_aum_typed_qa.dpl_tpkptl ;
show partitions inv_aum_typed_qa.dpl_mfclcl ;
show partitions inv_aum_typed_qa.dpl_mfsc ;
show partitions inv_aum_typed_qa.dpl_tpcont ;
show partitions inv_aum_typed_qa.dpl_mfbr ;
show partitions inv_aum_typed_qa.dpl_mfcl ;
show partitions inv_aum_typed_qa.dpl_mfrrus ;
show partitions inv_aum_typed_qa.dpl_gncode ;

set hivevar:t_process_date = '2020-05-15';

alter   table   inv_aum_typed_qa.dpl_actran    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_gngnco    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_mfclac    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_mfacct    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_mfrr      drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_esmgr     drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_acpos     drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_gnexch    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_tpkptl    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_mfclcl    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_mfsc      drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_tpcont    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_mfbr      drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_mfcl      drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_mfrrus    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_gncode    drop partition   (process_date = ${t_process_date});

analyze table   inv_aum_typed_qa.dpl_actran    partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_gngnco    partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_mfclac    partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_mfacct    partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_mfrr      partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_esmgr     partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_acpos     partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_gnexch    partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_tpkptl    partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_mfclcl    partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_mfsc      partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_tpcont    partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_mfbr      partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_mfcl      partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_mfrrus    partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_gncode    partition       (process_date) compute statistics;
