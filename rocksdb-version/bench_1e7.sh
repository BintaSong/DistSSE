db_file="cs.db" #这里设置客户端使用的数据库

kKeywordGroupBase="Group-"
kKeyword10GroupBase=$kKeywordGroupBase"10^"
thread_num=4 #在这里设置线程数目

for i in `seq 0 4`;
do
	for j in `seq 0 $thread_num`; #total number is 1000
	do
		kw=$kKeyword10GroupBase"1_"$i"_"$j
		./rpc_client $db_file 0 $kw $thread_num 
	done
done

for i in `seq 0 $thread_num`;
do
	for j in `seq 0 3`; #total number is 1000
	do
		kw=$kKeyword10GroupBase"2_"$i"_"$j
		./rpc_client $db_file 0 $kw $thread_num  
	done
done

for i in `seq 0 $thread_num`;
do
	for j in `seq 0 3`; #total number is 1000
	do
		kw=$kKeyword10GroupBase"3_"$i"_"$j
		./rpc_client $db_file 0 $kw $thread_num 
	done
done

for i in `seq 0 $thread_num`;
do
	for j in `seq 0 3`; #total number is 1000
	do
		kw=$kKeyword10GroupBase"4_"$i"_"$j
		./rpc_client $db_file 0 $kw $thread_num  
	done
done

for i in `seq 0 $thread_num`;
do
		for j in `seq 0 3`;
		do
			kw=$kKeyword10GroupBase"5_"$i"_"$j
			./rpc_client $db_file 0 $kw $thread_num  
		done
done


	# echo $kw_list

