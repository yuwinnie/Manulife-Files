Group ID: yuwinni@MFCGD.COM
Domain:   MLIDDOMAIN1
employee ID:  412258

#######AUM:
Host: azcedledgev001.v01caedl.manulife.com 
Port:  22
Service Account: invaumsvcqa
Pwd: AzSxDc9! 

#######Beeline connection: 
/******QA/UAT*****/
beeline --verbose -u "jdbc:hive2://azcedlmstv001.v01caedl.manulife.com:2181,azcedlmstv002.v01caedl.manulife.com:2181,azcedlmstv003.v01caedl.manulife.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-batch?tez.queue.name=inv"

#######spark-shell
spark.sql(s""" """).show(false)

#######set hive.execution.engine=mr;
set hive.execution.engine=tez;
set hive.auto.convert.join=true;
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.exec.compress.output=true;
set hive.exec.compress.intermediate=true;
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions = 10000;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.parallel=true;
set hive.merge.mapfiles=false;
set hive.optimize.bucketmapjoin=true;
set hive.optimize.bucketmapjoin.sortedmerge=true;
set hive.optimize.index.filter=false;
set hive.optimize.ppd=true;
set hive.optimize.ppd.storage=true;
set hive.stats.autogather=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;
set hive.support.quoted.identifiers = none;
set hive.tez.auto.reducer.parallelism=true;
set hive.tez.container.size=8192;
Set hive.tez.java.opts=-Xmx6554m; 
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set mapred.output.compression.type=BLOCK;
Set tez.queue.name=inv;
set tez.task.resource.memory.mb=5000;

#######
msck repair table <tablename>

#######
oozie jobs -oozie https://azcedlmstv004.v01caedl.manulife.com:11443/oozie -jobtype coordinator -len 1000 -filter "status=RUNNING;user=invaumsvcqa"

	• Check runnings:   sh status_oozie.sh coordinators 100 RUNNING
	• Check status:     ./info_oozie.sh 0006356-200426001352143-oozie-oozi-W
	• Kill job:         ./kill_oozie.sh 0013079-191214230531735-oozie-oozi-C
	• Rerun job:        sh rerun_error_common.sh main_curation.properties 0003733-190414011353076-oozie-oozi-W 
	• Check yarn log:   ./yarn_stderr_logs.sh  job_1589034955217_14173
	• Kill application: yarn application -kill application_1584844656286_43981
    
    
#######Temp Table Creation
	• Step1: Create QA directory in AUM HDFS zone
        hdfs dfs -mkdir  /apps/inv_qa/aum/raw/qa_test/indonesia_qa/
 
	• step 2: Copy the source file on to HDFS
        hdfs dfs -put -f /data-01/inv_landing_qa/aum/common_folder/All_HK_files/HK_LatestFiles/MIT_20190903_Prices.csv /apps/inv_qa/aum/raw/qa_test/indonesia_qa/
        
	• Step 3: Create table, with link to HDFS. DDL below
        Create table inv_raw_qa.indonesia_qa ##same table structure as target
        (
        inv_acct_num String,      
        dist_chnl_cd String, 
        fnd_id String,     
        aum_dt date,     
        unit decimal(38,18)
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES 
        (
            "separatorChar" = "|", ##May need to change to "," or other delimiters as per source file
            "quoteChar"     = "\""
        )
          STORED AS TEXTFILE
          LOCATION '/apps/inv_qa/aum/raw/qa_test/indonesia_qa/'
          tblproperties ("skip.header.line.count"="1"); ##indicate first row is header only
 
	• Step 4 - Comparison below: 
    select  count(*) 
    from    inv_raw_qa.indonesia_qa a   inner join inv_typed_qa.aum_ta_asia_id_units b
        on  nvl(a.inv_acct_num, '@') = nvl(b.inv_acct_num, '@')
        and nvl(a.dist_chnl_cd, '@') = nvl(b.dist_chnl_cd, '@')
        and nvl(a.fnd_id, '@') = nvl(b.fnd_id, '@')
        --and nvl(to_date(from_unixtime(unix_timestamp(a.aum_dt,'yyyyMMdd'))), cast('9999-12-31' as date)) = nvl(cast(b.aum_dt as date), cast('9999-12-31' as date))
        --and nvl(to_date(from_unixtime(unix_timestamp(a.aum_dt,'yyyyMMdd'))), '9999-12-31' ) = nvl(b.aum_dt,'9999-12-31')
        and to_date(from_unixtime(unix_timestamp(a.aum_dt,'yyyyMMdd'))) = b.aum_dt
        and nvl(cast(a.unit as decimal(38,18)), 9.9) = nvl(b.unit, 9.9);
