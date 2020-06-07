#/data-01/MFCGD.COM/yuwinni/Automation
    
result_file=testresult_$(date +%Y_%m_%d_%H%M%S).txt

mkdir -p logs

(
echo "Automation Test Run Start Time " $(date +%Y_%m_%d_%H%M%S)

for file in *.spark; do
    echo ""
    echo "$(basename "$file")"
    echo ""
    echo "Test Case "$file " Execution Start Time: " $(date +%Y_%m_%d_%H%M%S)
    logfile=logs/$file.log_$(date +%Y_%m_%d_%H%M%S)
    
    target_date='2020-05-17'
    source_date='2020-05-01'
    
    spark-shell > $logfile 2>&1 << EOF 
val t_process_date = "$target_date"
val s_process_date = "$source_date"
:load '$file' 

:quit
EOF
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
        cat $logfile | grep 'Count with mismatches = 0' >/dev/null 2>&1 
        if [ $? -ne 0 ]; then 
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