#10.mfclcl
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${vm_conn} \
--username progress \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_mfclcl/process_date=${process_date} \
--query "SELECT  a.*,'SM' as brokder_code FROM  PUB.mfclcl a  where \$CONDITIONS" \
--boundary-query "SELECT MIN(last_dt), MAX(last_dt) FROM PUB.mfclcl WHERE last_dt IN (SELECT last_dt FROM PUB.mfclcl)" \
--split-by last_dt \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/10_mfclcl_vm.log \
--relaxed-isolation

while ! grep -q Retrieved ${log_path}/10_mfclcl_vm.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/10_mfclcl_vm.log
echo "[10_mfclcl_vm ---- Done]"
echo "==============================================="

#11.mfsc
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect ${vm_conn} \
--username progress \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_mfsc/process_date=${process_date} \
--query "SELECT  a.*,'SM' as brokder_code FROM  PUB.mfsc a  where \$CONDITIONS" \
--boundary-query "SELECT MIN(class), MAX(class) FROM PUB.mfsc WHERE class IN (SELECT class FROM PUB.mfsc)" \
--split-by class \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/11_mfsc_vm.log \
--relaxed-isolation

while ! grep -q Retrieved ${log_path}/11_mfsc_vm.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/11_mfsc_vm.log
echo "[11_mfsc_vm ---- Done]"
echo "==============================================="
