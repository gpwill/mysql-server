MYSQL_SPECTRUM_ROOT=/Users/gupeng/workplace/mysql-server/build/mysql-spectrum
MYSQL_SPECTRUM_STORAGE_DATA_ROOT=/Users/gupeng/mysql-spectrum-data/storage
MYSQL_SPECTRUM_STORAGE_PORT=3406

export SPECTRUM_STORAGE_NODE=TRUE
${MYSQL_SPECTRUM_ROOT}/bin/mysqld --console --log-error-verbosity=3 --socket=/tmp/mysql-spectrum-storage.sock --port=$MYSQL_SPECTRUM_STORAGE_PORT --datadir=${MYSQL_SPECTRUM_STORAGE_DATA_ROOT}

