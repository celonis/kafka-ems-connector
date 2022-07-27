#!/bin/bash -
#===============================================================================
#
#          FILE: testmessagesender.sh
#
#         USAGE: ./testmessagesender.sh
#
#   DESCRIPTION: 
#
#       OPTIONS: ---
#  REQUIREMENTS: ---
#          BUGS: ---
#         NOTES: ---
#        AUTHOR: YOUR NAME (), 
#  ORGANIZATION: 
#       CREATED: 07/26/22 18:44:02
#      REVISION:  ---
#===============================================================================

set -o nounset                                  # Treat unset variables as an error

count=0
while true
  do
    kcat -P -b localhost:9092 -t testdatatopic -D§ -K^ -l jsonexample.json
    ((count=$count+1))
    sleep 1;
done
