SPECTRUM_NODE_ID=${1}

MYSQL_SPECTRUM_ROOT=/Users/gupeng/workplace/mysql-server/build/mysql-spectrum
MYSQL_SPECTRUM_NODE_SOCK=/tmp/mysql-spectrum-${SPECTRUM_NODE_ID}.sock

${MYSQL_SPECTRUM_ROOT}/bin/mysql --socket=${MYSQL_SPECTRUM_NODE_SOCK} --user=root
