#!/bin/bash

sigquit()
{
   echo "signal QUIT received"
}

sigint()
{
   echo "signal INT received, script ending"
   exit 0
}

sigkill()
{
   echo "signal KILL received, script ending"
   exit 0
}

trap 'sigquit' QUIT
trap 'sigint'  INT
trap 'sigkill' KILL      # ignore the specified signals

echo "test script started. My PID is $$"
while true ; do
  sleep 30
done
