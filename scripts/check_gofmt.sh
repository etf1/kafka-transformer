#!/usr/bin/env bash

set -euo pipefail

readonly GO_FILES=$(find . -iname '*.go' -type f | grep -v vendor)
readonly GOFMT_FILES=$(gofmt -l ${GO_FILES})

if [[ -n "${GOFMT_FILES}" ]]; then
     echo "Wrong formatting for:"
     echo "${GOFMT_FILES}"
     exit 1
fi

exit 0
