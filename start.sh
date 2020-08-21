#!/bin/bash

PROJECT=$1
if [[ "${PROJECT}" = "" ]] || ! [[ "${PROJECT}" = "all" || "${PROJECT}" = "admin" || "${PROJECT}" = "receive" || "${PROJECT}" = "center" || "${PROJECT}" = "worker" ]];then
    echo "The script needs to enter a parameter, [all/admin/receive/center/worker]"
    exit
fi


export GOPATH=$(dirname $(dirname "$(cd `dirname $0`; pwd)"))
CONFIG=$GOPATH/src/sorakaq/config/config.toml

if [[ "${PROJECT}" = "all" ]];then
    su root -c "cd ${GOPATH}/src/sorakaq/ && ./../../bin/center -conf ${CONFIG}"  &
    sleep 1
    su root -c "cd ${GOPATH}/src/sorakaq/ && ./../../bin/admin -conf ${CONFIG}"  &
    sleep 1
    su root -c "cd ${GOPATH}/src/sorakaq/ && ./../../bin/receive -conf ${CONFIG}"  &
    sleep 1
    su root -c "cd ${GOPATH}/src/sorakaq/ && ./../../bin/worker -conf ${CONFIG}"  &
else
    su root -c "cd ${GOPATH}/src/sorakaq/ && ./../../bin/${PROJECT} -conf ${CONFIG}"  &
fi




