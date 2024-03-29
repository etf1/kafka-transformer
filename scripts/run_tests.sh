#!/usr/bin/env bash

set -euo pipefail

function wait_for_kafka(){
    local server="$1"

    for i in {1..20}
    do
        echo "Waiting for kafka cluster $1 to be ready ..."
        # kafkacat has 5s timeout
        kafkacat -b "${server}" -L > /dev/null 2>&1 && break
    done
}

wait_for_kafka $KAFKA_BOOTSTRAP_SERVER

make tests
