FILES=$(ls -lrt /data-01/inv_landing_qa/aum/common_folder/HK_Scripts/ALL_FILES/ |awk -F" " '{print $9}')
for line in $FILES
do 
        SRC_CNT=$(wc -l $line|cut -d" " -f1)
	SGFL=$(echo $line|cut -c 5,6 )
if [ "$SGFL" == "AC" ] ; then 
	echo "$line"
	rm OUTPUT/New_$line 2>/dev/null
	cat  $line |sed '1d' |sed '$d'|awk '{print substr($0,1,20) "|" substr($0,21,6) "|" substr($0,27,1) "|" substr($0,28,3) "|" substr($0,31,20) "|" substr($0,51,6) "|" substr($0,57,1) "|" substr($0,58,1) "|" substr($0,59,10) "|" substr($0,69,1) "|" substr($0,70,1) "|" substr($0,71,16) "|" substr($0,87,2) "|" substr($0,89,3) "|" substr($0,92,2) "|" substr($0,94,1) "|" substr($0,95,1) "|" substr($0,96,3) "|" substr($0,99,3) "|" substr($0,102,1) "|" substr($0,103,10) "|" substr($0,113,8) "|" substr($0,121,8) "|" substr($0,129,8) "|" substr($0,137,8) "|" substr($0,145,1) "|" substr($0,146,60) "|" substr($0,206,8) "|" substr($0,214,8) "|" substr($0,222,60) "|" substr($0,282,3) "|" substr($0,285,8) "|" substr($0,293,8) "|" substr($0,301,8) "|" substr($0,309,8) "|" substr($0,317,8) "|" substr($0,325,1) "|" substr($0,326,1)  }' |sed 's/+//g'|sed 's/  //g'|sed 's/ |/|/g'|sort > OUTPUT/New_$line
         TGT_CNT=$(cat  OUTPUT/New_$line|wc -l ) 
         echo $SRC_CNT $TGT_CNT
elif [ "$SGFL" == "DL" ] ; then
	echo "$line"
	rm OUTPUT/New_$line 2>/dev/null
	cat  $line |sed '1d' |sed '$d'|awk '{print substr( $0,1,10)  "|" substr($0,11,60)   "|" substr($0,71,1)   "|" substr($0,72,1)   "|" substr($0,73,1)   "|" substr($0,74,8)   "|" substr($0,82,1) }' |sed 's/+//g'|sed 's/  //g'|sed 's/ |/|/g'|sort > OUTPUT/New_$line
         TGT_CNT=$(cat  OUTPUT/New_$line|wc -l ) 
         echo $SRC_CNT $TGT_CNT
#cat  $line |sed '1d' |sed '$d'|awk '{print substr( $0,1,10)  "|" substr($0,11,60)   "|" substr($0,71,1)   "|" substr($0,72,1)   "|" substr($0,73,1)   "|" substr($0,74,8)   "|" substr($0,82,1) }' |sed 's/+//g'|sed 's/ //g'|sort > OUTPUT/New_$line
elif [ "$SGFL" == "AF" ] ; then
	echo "$line"
	rm OUTPUT/New_$line 2>/dev/null
	cat  $line |sed '1d' |sed '$d'|awk '{print substr( $0,1,20)  "|" substr($0,21,6)   "|" substr($0,27,6)   "|" substr($0,33,8)   "|" substr($0,41,8)   "|" substr($0,49,3)   "|" substr($0,52,14)  "|" substr($0,66,14)  "|" substr($0,80,14)  "|" substr($0,94,14)  "|" substr($0,108,14) "|" substr($0,122,14) "|" substr($0,136,14) "|" substr($0,150,14) "|" substr($0,164,14) "|" substr($0,178,14) "|" substr($0,192,14) "|" substr($0,206,14) "|" substr($0,220,14) "|" substr($0,234,14) "|" substr($0,248,14) "|" substr($0,262,14) "|" substr($0,276,14) "|" substr($0,290,8)  "|" substr($0,298,8)  "|" substr($0,306,8)  "|" substr($0,314,1) }' |sed 's/+//g'|sed 's/  //g'|sed 's/ |/|/g'|sort > OUTPUT/New_$line
         TGT_CNT=$(cat  OUTPUT/New_$line|wc -l ) 
         echo $SRC_CNT $TGT_CNT

elif [ "$SGFL" == "FP" ] ; then
	echo "$line"
	rm OUTPUT/New_$line 2>/dev/null
	cat  $line |sed '1d' |sed '$d'|awk '{print substr($0,1,6) "|" substr($0,7,8) "|" substr($0,15,14) "|" substr($0,29,14) "|" substr($0,43,14) "|" substr($0,57,8) "|" substr($0,65,1) "|" substr($0,66,14)}' |sed 's/+//g'|sed 's/  //g'|sed 's/ |/|/g'|sort > OUTPUT/New_$line
         TGT_CNT=$(cat  OUTPUT/New_$line|wc -l ) 
         echo $SRC_CNT $TGT_CNT
elif [ "$SGFL" == "TX" ] ; then
	echo "$line"
	rm OUTPUT/New_$line 2>/dev/null
	cat  $line |sed '1d' |sed '$d'|awk '{print substr($0,1,20) "|" substr($0,21,8) "|" substr($0,29,6) "|" substr($0,35,5) "|" substr($0,40,3) "|" substr($0,43,14) "|" substr($0,57,14) "|" substr($0,71,14) "|" substr($0,85,3) "|" substr($0,88,8) "|" substr($0,96,12) "|" substr($0,108,2) "|" substr($0,110,8) "|" substr($0,118,6) "|" substr($0,124,5) "|" substr($0,129,1) "|" substr($0,130,14) "|" substr($0,144,14) "|" substr($0,158,14) "|" substr($0,172,14) "|" substr($0,186,14) "|" substr($0,200,14) "|" substr($0,214,14) "|" substr($0,228,15) "|" substr($0,243,8) "|" substr($0,251,8) "|" substr($0,259,6) "|" substr($0,265,10) "|" substr($0,275,15) "|" substr($0,290,20) "|" substr($0,310,3) "|" substr($0,313,17) "|" substr($0,330,5) "|" substr($0,335,17) "|" substr($0,352,6) "|" substr($0,358,1) "|" substr($0,359,15) }' |sed 's/+//g'|sed 's/  //g'|sed 's/ |/|/g'|sort > OUTPUT/New_$line
         TGT_CNT=$(cat  OUTPUT/New_$line|wc -l ) 
         echo $SRC_CNT $TGT_CNT

fi 
done
