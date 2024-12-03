MYSQL_SPECTRUM_ROOT=/Users/gupeng/workplace/mysql-server/build/mysql-spectrum
MYSQL_SPECTRUM_COMPUTE_PORT=3306

${MYSQL_SPECTRUM_ROOT}/bin/mysql --port=${MYSQL_SPECTRUM_COMPUTE_PORT} --socket=/tmp/mysql-spectrum-compute.sock --user=root
