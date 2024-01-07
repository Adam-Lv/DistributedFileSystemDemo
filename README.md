# Distributed File System Demo

## Introduction
这是一个分布式文件系统的demo，实现了基本的文件上传、下载、删除的功能，实现了元数据的管理，实现了文件的分块存储。

## Quick Start
### 1. 环境准备
- 需要安装docker和docker-compose，保持宿主机的8080端口没有被占用。
- 若在宿主机启动client，需要安装python3和requests库。
### 2. 启动

1. 在项目根目录下执行`docker-compose up -d`，启动服务。
2. 在`name_server`容器中执行`python3 /project/name_server.py`，启动NameServer。
3. 在`data_server_1`容器中执行`python3 /project/data_server.py`，启动DataServer。
4. 在`data_server_2`容器中执行`python3 /project/data_server.py`，启动DataServer。
5. 在`data_server_3`容器中执行`python3 /project/data_server.py`，启动DataServer。
6. 在`data_server_4`容器中执行`python3 /project/data_server.py`，启动DataServer。
7. 在宿主机中执行`python3 client.py`，启动Client。


### 3. 功能测试
1. ls <dir|null> : 列出当前目录下的文件和目录，若指定了dir，则列出dir目录下的文件和目录。
2. cd \<dir\> : 进入dir目录。
3. mkdir \<dir\> : 在指定路径下创建dir目录。
4. touch \<file\> : 在指定路径下创建file文件（空文件，不可下载）。
5. upload \<local-file\> \<dir\> : 上传local-file文件到dfs的dir目录下。
6. read \<file\> : 读取dfs中的file文件。文件会默认下载到./test_files/download目录下。
7. rm <file|dir>: 删除dfs中的file文件或dir目录。
8. exit : 退出Client。
