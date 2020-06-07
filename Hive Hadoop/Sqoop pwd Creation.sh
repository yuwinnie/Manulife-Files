--sqoop

###Storing Protected Passwords in Sqoop
Steps1. Store the credential on HDFS.

hadoop credential create dpl.sqoop.password.alias -provider jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks

dpl.sqoop.password.alias

--password-alias dpl.sqoop.password.alias \
-