#!/usr/bin/env bash

set -euo pipefail

readonly PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
readonly CONTAINER_NAME="arena-mysql"
readonly MYSQL_USER="a"
readonly MYSQL_PASSWORD="111111"
readonly MYSQL_DATABASE="arena"

usage()
{
    printf '%s\n' \
        "Usage:" \
        "  $0 shell" \
        "  $0 import <sql-file>" \
        "  $0 exec <sql>"
}

run_mysql()
{
    sudo docker exec -i \
        -e "MYSQL_PWD=${MYSQL_PASSWORD}" \
        "${CONTAINER_NAME}" \
        mysql \
        --default-character-set=utf8mb4 \
        -u "${MYSQL_USER}" \
        "${MYSQL_DATABASE}" \
        "$@"
}

case "${1:-}" in
    shell)
        exec sudo docker exec -it \
            -e "MYSQL_PWD=${MYSQL_PASSWORD}" \
            "${CONTAINER_NAME}" \
            mysql \
            --default-character-set=utf8mb4 \
            -u "${MYSQL_USER}" \
            "${MYSQL_DATABASE}"
        ;;
    import)
        if [[ $# -ne 2 ]]; then
            usage
            exit 2
        fi

        sql_file="$2"
        if [[ "${sql_file}" != /* ]]; then
            sql_file="${PROJECT_ROOT}/${sql_file}"
        fi
        if [[ ! -f "${sql_file}" ]]; then
            printf 'SQL file not found: %s\n' "${sql_file}" >&2
            exit 2
        fi

        run_mysql < "${sql_file}"
        ;;
    exec)
        if [[ $# -lt 2 ]]; then
            usage
            exit 2
        fi
        shift
        run_mysql -e "$*"
        ;;
    *)
        usage
        exit 2
        ;;
esac
