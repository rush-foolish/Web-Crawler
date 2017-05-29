#!/usr/bin/bash -eux
#####################################################
#	File Name: process_kill.sh
#	Author: Rachel Liu
#	Created Time: 2017-05-22
#	Description: 
#	
#	Modified History: 
#	
#	Copyright (C)2017 All Rights Reserved
#####################################################
process=$1
# find the pid which is matched to the condition
pid=`ps -ef |grep $process | grep -v grep | awk -F " " '{print $2}'`

for prc in $pid
do
	# for each process, check if the pid exists currently, and if it is current process which is running
	if [ $prc -ne $$ ]
	then 
		echo "the process_id: $prc"
		kill -9 $prc
	fi
done
