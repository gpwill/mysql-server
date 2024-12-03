MYSQL_SPECTRUM_ROOT=/Users/gupeng/workplace/mysql-server/build/mysql-spectrum
MYSQL_SPECTRUM_STORAGE_PORT=3406

${MYSQL_SPECTRUM_ROOT}/bin/mysql --port=${MYSQL_SPECTRUM_STORAGE_PORT} --socket=/tmp/mysql-spectrum-storage.sock --user=root
