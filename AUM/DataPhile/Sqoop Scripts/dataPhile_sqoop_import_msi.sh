#1.actran
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_actran/process_date=${process_date} \
--query "select  a.*,'MSI' as brokder_code from  PUB.actran a  where proc_dt >= '${actran}' and \$CONDITIONS" \
--boundary-query "select min(proc_dt), max(proc_dt) from pub.actran where proc_dt in (select proc_dt from pub.actran where proc_dt >= '${actran}')" \
--split-by proc_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/01_actran_msi.log \
--relaxed-isolation

while ! grep -q Retrieved ${log_path}/01_actran_msi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/01_actran_msi.log
echo "[1.actran_msi ---- Done]"
echo "==============================================="

#2.gngnco
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_gngnco/process_date=${process_date} \
--query "select  a.*,'MSI' as brokder_code from  PUB.gngnco a  where \$CONDITIONS" \
--boundary-query "select min(fst_bal), max(fst_bal) from PUB.gngnco where fst_bal in (select fst_bal from PUB.gngnco)" \
--split-by fst_bal \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/02_gngnco_msi.log \
--relaxed-isolation

while ! grep -q Retrieved ${log_path}/02_gngnco_msi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/02_gngnco_msi.log
echo "[2.gngnco_msi ---- Done]"
echo "==============================================="

#3.mfclac
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_mfclac/process_date=${process_date} \
--query "select  a.*,'MSI' as brokder_code from  PUB.mfclac a  where  \$CONDITIONS" \
--boundary-query "select min(client), max(client) from PUB.mfclac where client in (select client from PUB.mfclac)" \
--split-by client \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/03_mfclac_msi.log \
--relaxed-isolation

while ! grep -q Retrieved ${log_path}/03_mfclac_msi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/03_mfclac_msi.log
echo "[3.mfclac_msi ---- Done]"
echo "==============================================="

##4.mfacct
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_mfacct/process_date=${process_date} \
--query "select  a.*,'MSI' as brokder_code from  PUB.mfacct a  where  \$CONDITIONS" \
--boundary-query "select min(last_dt), max(last_dt) from PUB.mfacct where last_dt in (select last_dt from PUB.mfacct)" \
--split-by last_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/04_mfacct_msi.log \
--relaxed-isolation

while ! grep -q Retrieved ${log_path}/04_mfacct_msi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/04_mfacct_msi.log
echo "[4.mfacct_msi ---- Done]"
echo "==============================================="

##5.mfrr
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_mfrr/process_date=${process_date} \
--query "select  a.*,'MSI' as brokder_code from  PUB.mfrr a  where  \$CONDITIONS" \
--boundary-query "select min(last_dt), max(last_dt) from PUB.mfrr where last_dt in (select last_dt from PUB.mfrr)" \
--split-by last_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/05_mfrr_msi.log \
--relaxed-isolation

while ! grep -q Retrieved ${log_path}/05_mfrr_msi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/05_mfrr_msi.log
echo "[5.mfrr_msi ---- Done]"
echo "==============================================="

##6.esmgr
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_esmgr/process_date=${process_date} \
--query "select  a.*,'MSI' as brokder_code from  PUB.esmgr a  where  \$CONDITIONS" \
--boundary-query "select min(last_dt), max(last_dt) from PUB.esmgr where last_dt in (select last_dt from PUB.esmgr)" \
--split-by last_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/06_esmgr_msi.log \
--relaxed-isolation

while ! grep -q Retrieved ${log_path}/06_esmgr_msi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/06_esmgr_msi.log
echo "[6.esmgr_msi ---- Done]"
echo "==============================================="

##7.acpos
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_acpos/process_date=${process_date} \
--query "select  a.*,'MSI' as brokder_code from  PUB.acpos a  where  \$CONDITIONS" \
--boundary-query "select min(last_dt), max(last_dt) from PUB.acpos where last_dt in (select last_dt from PUB.acpos)" \
--split-by last_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/07_acpos_msi.log \
--relaxed-isolation

while ! grep -q Retrieved ${log_path}/07_acpos_msi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/07_acpos_msi.log
echo "[7.acpos_msi ---- Done]"
echo "==============================================="

##8.gnexch
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_gnexch/process_date=${process_date} \
--query "select  a.*,'MSI' as brokder_code from  PUB.gnexch a  where last_dt >= '${gnexch}' and \$CONDITIONS" \
--boundary-query "select min(last_dt), max(last_dt) from pub.gnexch where last_dt in (select last_dt from pub.gnexch where last_dt >= '${gnexch}')" \
--split-by last_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/08_gnexch_msi.log \
--relaxed-isolation

while ! grep -q Retrieved ${log_path}/08_gnexch_msi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/08_gnexch_msi.log
echo "[8.gnexch_msi ---- Done]"
echo "==============================================="

##9.tpkptl
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_tpkptl/process_date=${process_date} \
--query "select  a.*,'MSI' as brokder_code from  PUB.tpkptl a  where proc_dt >= '${tpkptl}' and \$CONDITIONS" \
--boundary-query "select min(proc_dt), max(proc_dt) from pub.tpkptl where proc_dt in (select proc_dt from pub.tpkptl where proc_dt >= '${tpkptl}')" \
--split-by proc_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/09_tpkptl_msi.log \
--relaxed-isolation

while ! grep -q Retrieved ${log_path}/09_tpkptl_msi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/09_tpkptl_msi.log
echo "[9.tpkptl_msi ---- Done]"
echo "==============================================="

##12.tpcont
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_tpcont/process_date=${process_date} \
--query "select  a.*,'MSI' as brokder_code from  PUB.tpcont a  where proc_dt >= '${tpcont}' and \$CONDITIONS" \
--boundary-query "select min(proc_dt), max(proc_dt) from pub.tpcont where proc_dt in (select proc_dt from pub.tpcont where proc_dt >= '${tpcont}')" \
--split-by proc_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/12_tpcont_msi.log \
--relaxed-isolation

while ! grep -q Retrieved ${log_path}/12_tpcont_msi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/12_tpcont_msi.log
echo "[12.tpcont_msi ---- Done]"
echo "==============================================="

##13.mfbr
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_mfbr/process_date=${process_date} \
--query "select  a.*,'MSI' as brokder_code from  PUB.mfbr a  where  \$CONDITIONS" \
--boundary-query "select min(last_dt), max(last_dt) from PUB.mfbr where last_dt in (select last_dt from PUB.mfbr)" \
--split-by last_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/13_mfbr_msi.log \
--relaxed-isolation

while ! grep -q Retrieved ${log_path}/13_mfbr_msi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/13_mfbr_msi.log
echo "[13.mfbr_msi ---- Done]"
echo "==============================================="

##14.mfcl
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_mfcl/process_date=${process_date} \
--query "select  a.*,'MSI' as brokder_code from  PUB.mfcl a  where  \$CONDITIONS" \
--boundary-query "select min(last_dt), max(last_dt) from PUB.mfcl where last_dt in (select last_dt from PUB.mfcl)" \
--split-by last_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/14_mfcl_msi.log \
--relaxed-isolation

while ! grep -q Retrieved ${log_path}/14_mfcl_msi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/14_mfcl_msi.log
echo "[14.mfcl_msi ---- Done]"
echo "==============================================="

##15.mfrrus
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_mfrrus/process_date=${process_date} \
--query "select  a.*,'MSI' as brokder_code from  PUB.mfrrus a  where last_dt >= '${mfrrus}' and \$CONDITIONS" \
--boundary-query "select min(last_dt), max(last_dt) from PUB.mfrrus where last_dt in (select last_dt from PUB.mfrrus where last_dt >= '${mfrrus}')" \
--split-by last_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/15_mfrrus_msi.log \
--relaxed-isolation

while ! grep -q Retrieved ${log_path}/15_mfrrus_msi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/15_mfrrus_msi.log
echo "[15.mfrrus_msi ---- Done]"
echo "==============================================="

##16.gncode
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_gncode/process_date=${process_date} \
--query "select  a.*,'MSI' as brokder_code from  PUB.gncode a  where  \$CONDITIONS" \
--boundary-query "select min(last_dt), max(last_dt) from PUB.gncode where last_dt in (select last_dt from PUB.gncode)" \
--split-by last_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/16_gncode_msi.log \
--relaxed-isolation

while ! grep -q Retrieved ${log_path}/16_gncode_msi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/16_gncode_msi.log
echo "[16.gncode_msi ---- Done]"
echo "==============================================="

export actran=${actran}
export gnexch=${gnexch}
export tpkptl=${tpkptl}
export tpcont=${tpcont}
export mfrrus=${mfrrus}
export process_date=${process_date}

export root_path=${root_path}
export log_path=${log_path}

export msisi_conn=${msisi_conn}
export msii_conn=${msii_conn}
export vm_conn=${vm_conn}

./dataPhile_sqoop_import_msisi.sh