#!/usr/bin/env bash
set -x
set -eo pipefail

# if a Scylla container is running, print instructions to kill it and exit
RUNNING_CONTAINER=$(docker ps --filter 'name=scylla' --format '{{.ID}}')
if [[ -n $RUNNING_CONTAINER ]]; then
  echo >&2 "there is a scylla container already running, kill it with"
  echo >&2 " docker kill ${RUNNING_CONTAINER}"
  exit 1
fi

SCYLLA_NAME="scylla-$(date '+%s')"

# Launch Scylla using Docker
docker run --rm -it \
  -p 9042:9042 \
  --name "${SCYLLA_NAME}" \
  --hostname "${SCYLLA_NAME}" \
  -d scylladb/scylla \
  --memory 4G \
  --smp 1


>&2 echo "Scylla is ready to go!"
