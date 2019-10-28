#!/bin/bash

# source common functions
source common.sh

for i in ${SERVICE_PRECONDITION[@]}
do
    wait_for_it ${i}
done

exec $@
