MYSQL_SPECTRUM_ROOT=/Users/gupeng/workplace/mysql-server/build/mysql-spectrum
MYSQL_SPECTRUM_COMPUTE_DATA_ROOT=/Users/gupeng/mysql-spectrum-data/compute
MYSQL_SPECTRUM_COMPUTE_PORT=3306

export SPECTRUM_COMPUTE_NODE=TRUE
${MYSQL_SPECTRUM_ROOT}/bin/mysqld --console --log-error-verbosity=3 --socket=/tmp/mysql-spectrum-compute.sock --port=$MYSQL_SPECTRUM_COMPUTE_PORT --datadir=${MYSQL_SPECTRUM_COMPUTE_DATA_ROOT}

