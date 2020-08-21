#!/bin/bash
set -e
export GOPATH=$(dirname $(dirname "$(cd `dirname $0`; pwd)"))
go install sorakaq/center
go install sorakaq/worker
go install sorakaq/admin
go install sorakaq/receive

