flag=1
count=0
while [ $flag -ne 0 ]
do
	/media/partiks/Study/586-DS/DS\ Android\ Projects/SimpleDynamo/simpledynamo-grading.linux -p 3 ./app/build/outputs/apk/debug/app-debug.apk | tee /dev/tty >> new_shell_grader_output.txt
	tail -n 10 new_shell_grader_output.txt | grep -q -e "no pass" -e "not found" -e "never sent"
	#echo "second line executed"
	#echo $grader_output 
	#echo "third line executed"
	#echo $grader_output 
	#echo "Total score: 0" | tee /dev/tty | egrep -q '(Total score: 0)'
	flag=$?
	echo "-------------------------------------------------------------------------------------------------"
	count=$((count+1))
	echo "Successful Count = " $count
	echo "Successful Count = " $count >> shell_grader_output.txt
done