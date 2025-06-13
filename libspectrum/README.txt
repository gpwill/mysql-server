# Build
mkdir build
cd build
cmake .. -DWITH_DEBUG=1 -DDOWNLOAD_BOOST=1 -DWITH_BOOST=./boost
make

# Initialize
./spectrum-node-init storage-0
./spectrum-node-init compute-0
./spectrum-node-init compute-1

# Start
./spectrum-node-start storage-0 98998
./spectrum-node-start compute-0 3306
./spectrum-node-start compute-1 3307

# Test
./spectrum-node-client compute-0
>> create database test;

cd mysql-test
./mysql-test-run.pl spectrum
