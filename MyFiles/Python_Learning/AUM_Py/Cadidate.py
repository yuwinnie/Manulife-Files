
##Execute Hive Beeline JDBC String Command from Python
import commands
import re

query = 'select count(*) from inv_aum_raw_qa.test_dpl_gngnco;'

host=str('azcedlmstv001.v01caedl.manulife.com')
port=str('2181')
authMechanism=str('KERBEROS')
##database=str('inv_aum_raw_qa')
serviceDiscoveryMode = 'zooKeeper'
zooKeeperNamespace = 'hiveserver2-batch?tez.queue.name=inv'

result_string= 'beeline --verbose -u "jdbc:hive2://azcedlmstv001.v01caedl.manulife.com:2181,azcedlmstv002.v01caedl.manulife.com:2181,azcedlmstv003.v01caedl.manulife.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-batch?tez.queue.name=inv"'
print(result_string)
status, output = commands.getstatusoutput(result_string)

if status == 0:
    print output
else:
    print "Error encountered while executing HiveQL queries."
