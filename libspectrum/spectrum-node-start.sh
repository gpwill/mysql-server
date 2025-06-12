export SPECTRUM_NODE_ID=${1}

MYSQL_PORT=${2}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MYSQL_SPECTRUM_ROOT=${SCRIPT_DIR}/../build
MYSQL_SPECTRUM_NODE_DATA_ROOT=${MYSQL_SPECTRUM_ROOT}/mysql-spectrum-data/${SPECTRUM_NODE_ID}
MYSQL_SPECTRUM_NODE_SOCK=/tmp/mysql-spectrum-${SPECTRUM_NODE_ID}.sock

${MYSQL_SPECTRUM_ROOT}/bin/mysqld --console --log-error-verbosity=3 --port=${MYSQL_PORT} --socket=${MYSQL_SPECTRUM_NODE_SOCK} --datadir=${MYSQL_SPECTRUM_NODE_DATA_ROOT}

