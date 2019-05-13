flag=1
count=0
while { [ $flag -ne 0 ] || [ $count -eq 5 ]; }
do
	/media/partiks/Study/586-DS/DS\ Android\ Projects/SimpleDynamo/simpledynamo-grading.linux ./app/build/outputs/apk/debug/app-debug.apk | tee /dev/tty > shell_grader_output.txt
	cat shell_grader_output.txt >> new_shell_grader_output.txt
	cat shell_grader_output.txt | grep -q -e "not found" -e "never sent" -e "failed" -e "still has data" -e "No query result for key" -e "Incorrect value" -e "no pass" -e "Timed out" -e "Values different" -e "items are stored"
	#echo "second line executed"
	#echo $grader_output 
	#echo "third line executed"
	#echo $grader_output 
	#echo "Total score: 0" | tee /dev/tty | egrep -q '(Total score: 0)'
	flag=$?
	echo "-------------------------------------------------------------------------------------------------"
	count=$((count+1))
	echo "Successful Count = " $count
	echo "Successful Count = " $count >> new_shell_grader_output.txt
	echo "-------------------------------------------------------------------------------------------------" >> new_shell_grader_output.txt
	echo "  " >> new_shell_grader_output.txt
done