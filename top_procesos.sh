rm -f /tmp/top_procesos.txt
hdfs dfs -rm /tmp/erocho/top_procesos.txt
top_procesos=$(top -b -n 1 | head -n $(($(tput lines) - 7)) | tail -n +8 | head -n 10)
echo "$top_procesos" > /tmp/top_procesos.txt
chmod 777 /tmp/top_procesos.txt
hdfs dfs -put -f /tmp/top_procesos.txt /tmp/erocho/top_procesos.txt
hdfs dfs -chmod 777 /tmp/erocho/top_procesos.txt
