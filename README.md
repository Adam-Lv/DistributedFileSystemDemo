# Distributed File System

## 程序架构

1. data_server.py
  - class DataServer
    - read
      - name_server读文件时，会调用data_server的read方法，
    - handle_request
    - flask app：接收来自name_server的请求、接收来自其他data_server的请求

2. name_server.py
  
  

3. metadata_server.py
  - class MetadataNode
  - class FolderNode(MetadataNode)
    - metadata属性会在被创建节点的时候自动创建，初始化需要的参数是**父节点**和在**dfs中的绝对路径**
    - add_child方法：传入一个创建好了的MetadataNode对象
  - class FileNode(MetadataNode)
    - 初始化需要的参数是**父节点**和在**dfs中的绝对路径**，还有metadata属性
    - metadata属性部分需要传递参数
      - file-size 
      - chunk-num: 文件分了多少块
      - main-chunk-list: 主chunk列表，每个元素是一个dataserver的ip地址
      - replications: [[]]
