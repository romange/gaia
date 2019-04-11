#!/bin/bash

usage() {
    echo "Usage: $0 -l <dirname> --name <testname> -t <timeout>"
}


while [[ $# -gt 1 ]]
do
key="$1"
TIMEOUT=10
LOG_DIR=`pwd`

case $key in
    -l)
    LOG_DIR="$2"
    shift # past argument
    ;;
    --name)
    TEST_NAME="$2"
    shift # past argument
    ;;
    -t)
    TIMEOUT="$2"
    shift # past argument
    ;;
    -h|--help)
     usage; shift; exit 0
    ;;
    *) usage; exit 1
    ;;
esac
shift # past argument or value
done

if [[ -z $TEST_NAME ]]; then
    usage; exit 1;
fi

set -o pipefail

gdbbt() {
    tmp=$(tempfile)
    echo thread apply all bt >$tmp
    sudo gdb -batch -nx -q -x $tmp -p $1
    kill -9 $1
    rm -f $tmp
}

run_timeout() {
  	timeout=$1
	(./"${TEST_NAME}" --log_dir="${LOG_DIR}" --gtest_output=xml:"${LOG_DIR}"/"${TEST_NAME}".xml \
     --gtest_color=yes | tee ${TEST_NAME}.test_result.log) & pid=$!

	while kill -0 $pid &>/dev/null && [ "${timeout}" -ne 0 ]; do
		sleep 1
		timeout=$((timeout - 1))
	done

	if kill -0 $pid &>/dev/null; then
		gdbbt $pid
		exit 124
	fi

    wait $pid
    exit $?
}

run_timeout "${TIMEOUT}"
