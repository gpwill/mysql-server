MYSQL_SPECTRUM_ROOT=/Users/gupeng/workplace/mysql-server/build/mysql-spectrum
MYSQL_SPECTRUM_COMPUTE_DATA_ROOT=/Users/gupeng/mysql-spectrum-data/compute

mkdir -p ${MYSQL_SPECTRUM_COMPUTE_DATA_ROOT}
${MYSQL_SPECTRUM_ROOT}/bin/mysqld --initialize-insecure --datadir=${MYSQL_SPECTRUM_COMPUTE_DATA_ROOT}

