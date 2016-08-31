#!/bin/bash
set -x

loop=1
while(($loop>0))
do
	echo "loop$loop"
	go test > out.txt
	if (( $?==0 ))
	then
		loop=`expr $loop + 1`
	else
		loop=0
	fi
done
