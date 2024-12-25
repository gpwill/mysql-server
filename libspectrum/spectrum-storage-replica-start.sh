MYSQL_SPECTRUM_ROOT=/Users/gupeng/workplace/mysql-server/build/mysql-spectrum
MYSQL_SPECTRUM_STORAGE_DATA_ROOT=/Users/gupeng/mysql-spectrum-data/storage-replica
MYSQL_SPECTRUM_STORAGE_PORT=3407

export SPECTRUM_STORAGE_NODE=TRUE
export SPECTRUM_STORAGE_NODE_PORT=64001
${MYSQL_SPECTRUM_ROOT}/bin/mysqld --console --log-error-verbosity=3 --socket=/tmp/mysql-spectrum-storage-replica.sock --port=$MYSQL_SPECTRUM_STORAGE_PORT --datadir=${MYSQL_SPECTRUM_STORAGE_DATA_ROOT}

