# 0. 依赖
在 `Ubuntu20.04` 下：
```shell
sudo apt install libssl-dev bison pkg-config
```

# 1. 构建项目

## Debug模式

### 配置生成Makefile

```shell
# 进入项目根目录
mkdir cmake-build-debug
cmake -S . \
-B cmake-build-debug \
-G "Unix Makefiles" \
-DCMAKE_BUILD_TYPE=Debug \
-DCMAKE_INSTALL_PREFIX=/home/lemon/mysql \
-DMYSQL_DATADIR=/home/lemon/mysql/data \
-DMYSQL_UNIX_ADDR=/home/lemon/mysql/data/mysql.sock \
-DSYSCONFDIR=/home/lemon/mysql/data \
-DWITH_DEBUG=ON \
-DWITH_BOOST=/home/lemon/code/mysql-dpu/boost \
-DMYSQL_MAINTAINER_MODE=OFF
```

| CMake指令                                           | 解释                                                 |
| --------------------------------------------------- | ---------------------------------------------------- |
| -G "Unix Makefiles"                                 | 使用Makefile作为CMake的后端生成器                    |
| -DCMAKE_BUILD_TYPE=Debug                            | 使用Debug模式构建项目                                |
| -DCMAKE_INSTALL_PREFIX=/home/lemon/mysql/           | 指定MySQL的安装目录                                  |
| -DMYSQL_DATADIR=/home/lemon/mysql/data              | 指定MySQL存放数据的数据目录                          |
| -DMYSQL_UNIX_ADDR=/home/lemon/mysql/data/mysql.sock | 指定使用UNIX域套接字进行本地连接时，所使用的sock文件 |
| -DSYSCONFDIR=/home/lemon/mysql/data                 | 指定配置文件my.conf所在的目录                        |
| -DWITH_DEBUG=ON                                     | 设置MySQL开启Debug模式                               |
| -DWITH_BOOST=/home/lemon/code/mysql-dpu/boost       | Boost库所在目录                                      |
| -DMYSQL_MAINTAINER_MODE=OFF                         | 一定要关掉MAINTAINER模式                             |

### 编译生成可执行文件

```shell
cmake --build cmake-build-debug --target all -- -j 16
```

`-- -j 16` 指定编译项目时使用的线程数量。

## Release模式

# 2. 启动与调试

## 2.1 初始化工作

在调试 MySQL 之前需要初始化数据目录，设置 root 账户的权限和密码。

### 初始化数据目录前创建 MySQL 用户组和 MySQL 用户

```shell
sudo groupadd mysql
```

```shell
sudo useradd -r -g mysql -s /bin/false mysql
```

### 初始化mysqld的数据目录

进入项目根部录下，执行下列操作

```shell
cd ./cmake-build-debug/sql
./mysqld --basedir=/home/lemon/mysql --datadir=/home/lemon/mysql/data --lower_case_table_names=0 --user=mysql --innodb_buffer_pool_size=1G --innodb-flush-method=O_DIRECT --innodb_flush_log_at_trx_commit=1 --innodb_log_file_size=10G --initialize-insecure
```

解决启动 `Error` ：

![image-20220915112038779](/home/lemon/.config/Typora/typora-user-images/image-20220915112038779.png)

`MySQL` 打印错误日志时依赖 `errmsg.sys` 文件，在编译 `MySQL` 后，会在 `cmake-build-debug/sql/share` 文件夹下生成各种语言版本的 `errmsg.sys` 文件，将该文件拷贝至 `/home/lemon/mysql/share` 文件夹即可。

```shell
mkdir /home/lemon/mysql/share/
cp /home/lemon/code/mysql-dpu/cmake-build-debug/sql/share/english/errmsg.sys /home/lemon/mysql/share/
```

### 给数据目录添加读写执行权限

```shell
chmod -R 777 /home/lemon/mysql/data
```

### 启动Server端mysqld

```shell
./mysqld --basedir=/home/lemon/mysql --datadir=/home/lemon/mysql/data --lower_case_table_names=0 --user=mysql --innodb_buffer_pool_size=1073741824 --innodb-flush-method=O_DIRECT --innodb_flush_log_at_trx_commit=1
```

### 启动Client端mysql修改root用户密码

进入项目根部录下，执行下列操作：

```shell
cd ./cmake-build-debug/client
./mysql -uroot -h127.0.0.1 -P3306 -p
```

不用输入密码，直接回车，启动 MySQL 客户端登录 MySQL 之后，在客户端修改 `root` 用户密码，然后关闭Server端和Client端：

```sql
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY "root";
FLUSH privileges;
shutdown;
exit;
```

## 2.2 启动调试

### 
