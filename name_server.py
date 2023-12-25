from metadata_server import MetadataServer,FileNode
import requests
from concurrent.futures import ThreadPoolExecutor
import os
from flask import Flask, request, jsonify

class NameServer:
    def __init__(self, metadata_server):
        self.pwd = '/'
        self.metadata_server: MetadataServer = metadata_server
        self.root = metadata_server.root

    def _mkdir(self, path):
        """
        创建目录
        """
        
        return self.metadata_server.mkdir(path, self.pwd)

    def _ls(self, path):
        """
        列出目录下的文件
        """
        return self.metadata_server.ls(path, self.pwd)

    def _rm(self, path):
        """
        删除文件
        """
        return self.metadata_server.rm(path, self.pwd)


    def _touch(self, path):
        """
        创建文件
        """
        return self.metadata_server.touch(path, self.pwd)

    def _cd(self, path):
        """
        切换目录
        """
        new_pwd = self.metadata_server.cd(path)
        if new_pwd is not None:
            self.pwd = new_pwd
        return self.pwd

    def _read_metadata(self, path):
        """
        读取传入path的metadata，可以是文件或者目录
        """
        if not self.metadata_server.exist(path):
            raise FileNotFoundError(f'{path} does not exist.')
        return self.metadata_server.pwd.metadata



    def _upload(self, file, path):
        """
        从客户端接收上传的文件流，根据文件大小确定对应的分块所在dataserver的位置，
        并将文件分块保存到dataserver中，同时更新metadata_file_tree。
        """
        # 1. 二进制读一个文件，把file分块，装到一个list里面
        # 2. 为这个文件创建一个metadata，文件大小、文件块数、main-chunk-list、replications
        # 3. 确定每个块对应的dataserver
        # 4. 将每个块发送给对应的dataserver，只需要发送一次
        #         requests.post(
        #             f'http://{host}:9080/upload_chunk',
        #             files={'file': (file_name, data)},
        #             data={'source': host, 'chunk_num': chunk_num}

        #         )
        self.metadata_server.touch(file,path)
        file = open(file,'b')
        data = file.read()
        file_size = len(data)
        chunk_size = 2 * 1024 * 1024  # 2MB per chunk
        chunk_num = (file_size + chunk_size - 1) // chunk_size  # 计算文件块数

        # 准备文件块和元数据
        chunks = [data[i * chunk_size:(i + 1) * chunk_size] for i in range(chunk_num)]
        main_chunk_list = []
        replications = []
        


    def _read(self, file_path):
        """
        读取文件：根据文件路径读取metadata，根据metadata中的文件大小确定需要读取的分块，
        从dataserver中读取分块并拼接成完整的文件，返回文件流。
        """
        # 1. 读取metadata，文件块数、main-chunk-list、replications
        # 2. 确定每个块对应的dataserver
        # 3. 从dataserver中读取每个块，拼接成完整的文件，返回文件流
        #         requests.post(
        #             f'http://{host}:9080/read_chunk',
        #             data={'file_name': filename}
        #         )
        #    requests会返回一些信息{'status': 'success', 'data': data}
        # 4. 接收dataserver返回的文件流，拼接成完整的文件，存在本地

# if __name__ == '__main__':
app = Flask(__name__)
ds = NameServer()
# 客户端通过访问http://localhost:9080/upload_chunk来上传文件块
app.add_url_rule('/upload', 'upload', ds._upload, methods=['POST'])
# 客户端通过访问http://localhost:9080/read_chunk来读取文件块
app.add_url_rule('/read', 'read', ds._read, methods=['POST'])
app.add_url_rule('/mkdir', 'mkdir', ds._mkdir, methods=['POST'])
app.add_url_rule('/ls', 'ls', ds._ls, methods=['GET'])
app.add_url_rule('/rm', 'rm', ds._rm, methods=['POST'])
app.add_url_rule('/touch', 'touch', ds._touch, methods=['POST'])
app.add_url_rule('/cd', 'cd', ds._cd, methods=['POST'])
app.add_url_rule('/read_metadata', 'read_metadata', ds._read_metadata, methods=['GET'])
if __name__ == '__main__':
    app.run(port=9080)
