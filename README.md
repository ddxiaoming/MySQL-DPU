# BUILD WITH DEBUG
````
cmake -S . -B cmake-build-debug -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=/home/lemon/mysql/debug -DMYSQL_DATADIR=/home/lemon/mysql/debug/data -DMYSQL_UNIX_ADDR=/home/lemon/mysql/debug/data/mysql.sock -DSYSCONFDIR=/home/lemon/mysql/debug/data -DWITH_DEBUG=OFF -DWITH_BOOST=/home/lemon/code/mysql-dpu/boost -DMYSQL_MAINTAINER_MODE=OFF
````