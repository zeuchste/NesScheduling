#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Runs the systest join suite (or any systest arguments passed after --) once per configuration of the
# stream-join design space: storage (S1-S3) x processing (P1-P4) on the hash join, plus the nested-loop
# baseline (A3). One line of output per configuration: PASS/FAIL and wall-clock seconds.
#
# Usage:
#   ./scripts/run-join-design-space.sh <path-to-systest-binary> [extra systest args...]
# Example:
#   ./scripts/run-join-design-space.sh build/nes-systests/systest/systest --groups Join --data build/nes-systests/testdata

set -u

if [ $# -lt 1 ] || [ ! -x "$1" ]; then
    echo "usage: $0 <path-to-systest-binary> [extra systest args...]" >&2
    exit 1
fi
SYSTEST="$1"
shift
EXTRA_ARGS=("$@")
WORKDIR_BASE="${WORKDIR_BASE:-/tmp/join-design-space}"
THREADS="${THREADS:-4}"
FAILED=0

# run_config <name> <join_strategy> [worker args after --]...
run_config() {
    local name="$1" strategy="$2"
    shift 2
    local start end rc
    start=$(date +%s)
    "$SYSTEST" -n 6 --workingDir="${WORKDIR_BASE}/${name}" "${EXTRA_ARGS[@]}" \
        --optimizer join_strategy="$strategy" \
        -- --worker.query_engine.number_of_worker_threads="$THREADS" "$@" >"${WORKDIR_BASE}/${name}.log" 2>&1
    rc=$?
    end=$(date +%s)
    if [ $rc -eq 0 ]; then
        printf '%-45s PASS  %4ds\n' "$name" $((end - start))
    else
        printf '%-45s FAIL  %4ds  (log: %s)\n' "$name" $((end - start)) "${WORKDIR_BASE}/${name}.log"
        FAILED=1
    fi
}

mkdir -p "$WORKDIR_BASE"

## A3 baseline: nested-loop join
run_config "NESTED_LOOP_JOIN" NESTED_LOOP_JOIN

## A2: sort-merge join (trigger-time sort by key hash, merge probe)
run_config "SORT_MERGE_JOIN" SORT_MERGE_JOIN

## A4: index join (shared, incrementally maintained ordered index)
run_config "INDEX_JOIN" INDEX_JOIN

## A1: hash join, storage x processing matrix (lazy trigger)
for storage in PER_KEY_PAGED SHARED_CHAINS FIXED_ARRAY; do
    for processing in SINGLE_TASK TASK_PER_PAIR SHARED_TABLE BROADCAST; do
        run_config "HASH_${storage}_${processing}" HASH_JOIN \
            --worker.default_query_execution.join_storage="$storage" \
            --worker.default_query_execution.join_processing="$processing"
    done
done

exit $FAILED
