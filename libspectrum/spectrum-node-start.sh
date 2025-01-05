export SPECTRUM_NODE_ID=${1}

MYSQL_PORT=${2}

MYSQL_SPECTRUM_ROOT=/Users/gupeng/workplace/mysql-server/build/mysql-spectrum
MYSQL_SPECTRUM_NODE_DATA_ROOT=/Users/gupeng/mysql-spectrum-data/${SPECTRUM_NODE_ID}
MYSQL_SPECTRUM_NODE_SOCK=/tmp/mysql-spectrum-${SPECTRUM_NODE_ID}.sock

${MYSQL_SPECTRUM_ROOT}/bin/mysqld --console --log-error-verbosity=3 --port=${MYSQL_PORT} --socket=${MYSQL_SPECTRUM_NODE_SOCK} --datadir=${MYSQL_SPECTRUM_NODE_DATA_ROOT}

