# Build
cmake .. -DCMAKE_INSTALL_PREFIX=/home/codespace/mysql-spectrum -DWITH_DEBUG=1 -DDOWNLOAD_BOOST=1 -DWITH_BOOST=/home/codespace/boost
make
make install

# Initialize storage server
./spectrum-storage-init

# Initialize compute server
./spectrum-compute-init

# Start storage server
./spectrum-storage-start

# Start compute server
./spectrum-compute-start

# Run test sql to create table through compute server
./spectrum-compute-client

mysql>
use test;

CREATE TABLE employees (
    id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL, 
    hire_date DATE,
    salary DECIMAL(10, 2)
);

INSERT INTO employees (id, first_name, last_name, hire_date, salary)
VALUES (1, 'John', 'Doe', '2024-12-01', 50000.00);

