#1.actran
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msisi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_actran/process_date=${process_date} \
--query "select  a.*,'MSISI' as brokder_code from  PUB.actran a  where proc_dt >= '${actran}' and \$CONDITIONS" \
--boundary-query "select min(proc_dt), max(proc_dt) from pub.actran where proc_dt in (select proc_dt from pub.actran where proc_dt >= '${actran}')" \
--split-by proc_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/01_actran_msisi.log \
--relaxed-isolation \
--append

while ! grep -q Retrieved ${log_path}/01_actran_msisi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/01_actran_msisi.log
echo "[1.actran_msisi ---- Done]"
echo "==============================================="

#2.gngnco
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msisi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_gngnco/process_date=${process_date} \
--query "SELECT  a.*,'MSISI' as brokder_code FROM  PUB.gngnco a  where \$CONDITIONS" \
--boundary-query "SELECT MIN(fst_bal), MAX(fst_bal) FROM PUB.gngnco WHERE fst_bal IN (SELECT fst_bal FROM PUB.gngnco)" \
--split-by fst_bal \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/02_gngnco_msisi.log \
--relaxed-isolation \
--append

while ! grep -q Retrieved ${log_path}/02_gngnco_msisi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/02_gngnco_msisi.log
echo "[2.gngnco_msisi ---- Done"

#3.mfclac
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msisi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_mfclac/process_date=${process_date} \
--query "select  a.*,'MSISI' as brokder_code from  PUB.mfclac a  where  \$CONDITIONS" \
--boundary-query "select min(client), max(client) from PUB.mfclac where client in (select client from PUB.mfclac)" \
--split-by client \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/03_mfclac_msisi.log \
--relaxed-isolation \
--append

while ! grep -q Retrieved ${log_path}/03_mfclac_msisi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/03_mfclac_msisi.log
echo "[3.mfclac_msisi ---- Done]"
echo "==============================================="


##4.mfacct
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msisi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_mfacct/process_date=${process_date} \
--query "select  a.*,'MSISI' as brokder_code from  PUB.mfacct a  where  \$CONDITIONS" \
--boundary-query "select min(last_dt), max(last_dt) from PUB.mfacct where last_dt in (select last_dt from PUB.mfacct)" \
--split-by last_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/04_mfacct_msisi.log \
--relaxed-isolation \
--append

while ! grep -q Retrieved ${log_path}/04_mfacct_msisi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/04_mfacct_msisi.log
echo "[4.mfacct_msisi ---- Done]"
echo "==============================================="

##5.mfrr
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msisi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_mfrr/process_date=${process_date} \
--query "select  a.*,'MSISI' as brokder_code from  PUB.mfrr a  where  \$CONDITIONS" \
--boundary-query "select min(last_dt), max(last_dt) from PUB.mfrr where last_dt in (select last_dt from PUB.mfrr)" \
--split-by last_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/05_mfrr_msisi.log \
--relaxed-isolation \
--append

while ! grep -q Retrieved ${log_path}/05_mfrr_msisi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/05_mfrr_msisi.log
echo "[5.mfrr_msisi ---- Done]"
echo "==============================================="

##6.esmgr
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msisi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_esmgr/process_date=${process_date} \
--query "select  a.*,'MSISI' as brokder_code from  PUB.esmgr a  where  \$CONDITIONS" \
--boundary-query "select min(last_dt), max(last_dt) from PUB.esmgr where last_dt in (select last_dt from PUB.esmgr)" \
--split-by last_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/06_esmgr_msisi.log \
--relaxed-isolation \
--append

while ! grep -q Retrieved ${log_path}/06_esmgr_msisi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/06_esmgr_msisi.log
echo "[6.esmgr_msisi ---- Done]"
echo "==============================================="

##7.acpos
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msisi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_acpos/process_date=${process_date} \
--query "select  a.*,'MSISI' as brokder_code from  PUB.acpos a  where  \$CONDITIONS" \
--boundary-query "select min(last_dt), max(last_dt) from PUB.acpos where last_dt in (select last_dt from PUB.acpos)" \
--split-by last_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/07_acpos_msisi.log \
--relaxed-isolation \
--append

while ! grep -q Retrieved ${log_path}/07_acpos_msisi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/07_acpos_msisi.log
echo "[7.acpos_msisi ---- Done]"
echo "==============================================="

##8.gnexch
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msisi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_gnexch/process_date=${process_date} \
--query "select  a.*,'MSISI' as brokder_code from  PUB.gnexch a  where last_dt >= '${gnexch}' and \$CONDITIONS" \
--boundary-query "select min(last_dt), max(last_dt) from pub.gnexch where last_dt in (select last_dt from pub.gnexch where last_dt >= '${gnexch}')" \
--split-by last_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/08_gnexch_msisi.log \
--relaxed-isolation \
--append

while ! grep -q Retrieved ${log_path}/08_gnexch_msisi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/08_gnexch_msisi.log
echo "[8.gnexch_msisi ---- Done]"
echo "==============================================="

##9.tpkptl
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msisi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_tpkptl/process_date=${process_date} \
--query "select  a.*,'MSISI' as brokder_code from  PUB.tpkptl a  where proc_dt >= '${tpkptl}' and \$CONDITIONS" \
--boundary-query "select min(proc_dt), max(proc_dt) from pub.tpkptl where proc_dt in (select proc_dt from pub.tpkptl where proc_dt >= '${tpkptl}')" \
--split-by proc_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/09_tpkptl_msisi.log \
--relaxed-isolation \
--append

while ! grep -q Retrieved ${log_path}/09_tpkptl_msisi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/09_tpkptl_msisi.log
echo "[9.tpkptl_msisi ---- Done]"
echo "==============================================="

##12.tpcont
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msisi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_tpcont/process_date=${process_date} \
--query "select  a.*,'MSISI' as brokder_code from  PUB.tpcont a  where proc_dt >= '${tpcont}' and \$CONDITIONS" \
--boundary-query "select min(proc_dt), max(proc_dt) from pub.tpcont where proc_dt in (select proc_dt from pub.tpcont where proc_dt >= '${tpcont}')" \
--split-by proc_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/12_tpcont_msisi.log \
--relaxed-isolation \
--append

while ! grep -q Retrieved ${log_path}/12_tpcont_msisi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/12_tpcont_msisi.log
echo "[12.tpcont_msisi ---- Done]"
echo "==============================================="

##13.mfbr
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msisi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_mfbr/process_date=${process_date} \
--query "select  a.*,'MSISI' as brokder_code from  PUB.mfbr a  where  \$CONDITIONS" \
--boundary-query "select min(last_dt), max(last_dt) from PUB.mfbr where last_dt in (select last_dt from PUB.mfbr)" \
--split-by last_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/13_mfbr_msisi.log \
--relaxed-isolation \
--append

while ! grep -q Retrieved ${log_path}/13_mfbr_msisi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/13_mfbr_msisi.log
echo "[13.mfbr_msisi ---- Done]"
echo "==============================================="

##14.mfcl
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msisi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_mfcl/process_date=${process_date} \
--query "select  a.*,'MSISI' as brokder_code from  PUB.mfcl a  where  \$CONDITIONS" \
--boundary-query "select min(last_dt), max(last_dt) from PUB.mfcl where last_dt in (select last_dt from PUB.mfcl)" \
--split-by last_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/14_mfcl_msisi.log \
--relaxed-isolation \
--append

while ! grep -q Retrieved ${log_path}/14_mfcl_msisi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/14_mfcl_msisi.log
echo "[14.mfcl_msisi ---- Done]"
echo "==============================================="

##15.mfrrus
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msisi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_mfrrus/process_date=${process_date} \
--query "select  a.*,'MSISI' as brokder_code from  PUB.mfrrus a  where last_dt >= '${mfrrus}' and \$CONDITIONS" \
--boundary-query "select min(last_dt), max(last_dt) from PUB.mfrrus where last_dt in (select last_dt from PUB.mfrrus where last_dt >= '${mfrrus}')" \
--split-by last_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/15_mfrrus_msisi.log \
--relaxed-isolation \
--append

while ! grep -q Retrieved ${log_path}/15_mfrrus_msisi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/15_mfrrus_msisi.log
echo "[15.mfrrus_msisi ---- Done]"
echo "==============================================="

##16.gncode
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${msisi_conn} \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_gncode/process_date=${process_date} \
--query "select  a.*,'MSISI' as brokder_code from  PUB.gncode a  where  \$CONDITIONS" \
--boundary-query "select min(last_dt), max(last_dt) from PUB.gncode where last_dt in (select last_dt from PUB.gncode)" \
--split-by last_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/16_gncode_msisi.log \
--relaxed-isolation \
--append

while ! grep -q Retrieved ${log_path}/16_gncode_msisi.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/16_gncode_msisi.log
echo "[16.gncode_msisi ---- Done]"
echo "==============================================="



export actran=${actran}
export gnexch=${gnexch}
export tpkptl=${tpkptl}
export tpcont=${tpcont}
export mfrrus=${mfrrus}
export process_date=${process_date}

export root_path=${root_path}
export log_path=${log_path}

export msii_conn=${msii_conn}
export vm_conn=${vm_conn}

./dataPhile_sqoop_import_msii.sh