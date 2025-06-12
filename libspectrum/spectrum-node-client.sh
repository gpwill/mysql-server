SPECTRUM_NODE_ID=${1}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MYSQL_SPECTRUM_ROOT=${SCRIPT_DIR}/../build
MYSQL_SPECTRUM_NODE_SOCK=/tmp/mysql-spectrum-${SPECTRUM_NODE_ID}.sock

${MYSQL_SPECTRUM_ROOT}/bin/mysql --socket=${MYSQL_SPECTRUM_NODE_SOCK} --user=root
