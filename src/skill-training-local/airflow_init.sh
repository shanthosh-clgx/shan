#!/bin/bash
# airflow_init.sh

mkdir -p /sources/logs /sources/plugins
chown -R "${AIRFLOW_UID}:0" /sources/{logs,plugins}
exec /entrypoint airflow version