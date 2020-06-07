#/data-01/MFCGD.COM/yuwinni/Automation

result_file=testresult_$(date +%Y_%m_%d_%H%M%S).txt

mkdir -p logs

(
echo "Automation Test Run Start Time " $(date +%Y_%m_%d_%H%M%S)

for file in *.hql; do
    echo ""
    echo "$(basename "$file")"
    echo ""
    echo "Test Case "$file " Execution Start Time: " $(date +%Y_%m_%d_%H%M%S)
    logfile=logs/$file.log_$(date +%Y_%m_%d_%H%M%S)
    
    beeline --verbose -u "jdbc:hive2://azcedlmstv001.v01caedl.manulife.com:2181,azcedlmstv002.v01caedl.manulife.com:2181,azcedlmstv003.v01caedl.manulife.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-batch?tez.queue.name=inv"  --hivevar s_process_date='20200501' --hivevar t_process_date='20200517'  -f $file >> $logfile 2>&1
    re=$?
    
    echo "Test Case "$file " Execution End Time:   " $(date +%Y_%m_%d_%H%M%S)
    let i=i+1
    
    if [ ${re} -ne 0 ]; then
        echo "Test case "$file " is failed."
        echo "Have grammar error, please check your hql! "
        #failed
        let k=k+1
    else
        #echo $queryresult
        cat $logfile | grep '| FAIL' >/dev/null 2>&1 
        if [ $? -eq 0 ]; then 
            echo "Test case "$file " is failed."
            let k=k+1
        else
            echo "Test case "$file " is passed."
            let j=j+1
        fi
    fi
done

echo ""
echo "Automation Test Run End Time: " $(date +%Y_%m_%d_%H%M%S)

echo ""
echo "###########################################"
echo "Automation Test Run Result Summary:"
echo "Total test cases have been executed: " $i
echo "Total test cases have been failed  : " $k
echo "Total test cases have been passed  : " $j
echo "###########################################"
) | tee $result_file 2>&1