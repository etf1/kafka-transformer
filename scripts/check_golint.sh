#!/usr/bin/env bash

set -euo pipefail

golint -set_exit_status ./...
