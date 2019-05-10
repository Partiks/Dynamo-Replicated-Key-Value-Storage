command="./simpledynamo-grading.linux -p 2 -c app/build/outputs/apk/debug/app-debug.apk"
log="dynamo_grader_output.log"
match="no pass"

$command > "$log" 2>&1 &
pid=$!

while true
do
    if fgrep --quiet "$match" "$log"
    then
        kill $pid
        exit 0
    fi
done
