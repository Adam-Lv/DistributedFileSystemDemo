from concurrent.futures import ThreadPoolExecutor
import requests
import yaml

from metadata_server import MetadataServer
from flask import Flask, request, jsonify


class NameServer:
    def __init__(self):
        self.replication_num = 3
        self.working_directory = '/'
        self.metadata_server = MetadataServer()
        self.root = self.metadata_server.root
        self.data_servers = []
        with open('docker-compose.yml', 'r') as f:
            data = yaml.load(f.read(), Loader=yaml.FullLoader)
        for service in data['services']:
            if service.startswith('data_server'):
                self.data_servers.append(service)

    @staticmethod
    def send_chunk(host, data, file_name, chunk_num):
        """
        发送文件块给其他DataServer或NameServer
        """
        requests.post(
            f'http://{host}:9080/upload_chunk',
            files={'file': (file_name, data)},
            data={'source': host, 'chunk_num': chunk_num}
        )

    def mkdir(self):
        """
        创建目录
        """
        path = request.form.get('path')
        res = self.metadata_server.mkdir(path, self.working_directory)
        if res == 'success':
            return jsonify({'status': 'success'})
        else:
            return jsonify({'status': 'fail', 'message': res})

    def ls(self):
        """
        列出目录下的文件
        """
        path = request.form.get('path')
        res = self.metadata_server.ls(path, self.working_directory)
        if not isinstance(res, str):
            return jsonify({'status': 'success', 'data': res})
        else:
            return jsonify({'status': 'fail', 'message': res})

    def rm(self):
        """
        删除文件
        """
        path = request.form.get('path')

        res = self.metadata_server.rm(path, self.working_directory)
        # TODO: 在这里向dataserver发送删除文件的请求
        # response = requests.get('http://')
        if res == 'success':
            return jsonify({'status': 'success'})
        else:
            return jsonify({'status': 'fail', 'message': res})

    def touch(self):
        """
        创建文件
        """
        path = request.form.get('path')
        res = self.metadata_server.touch(path, self.working_directory)
        if res == 'success':
            return jsonify({'status': 'success'})
        else:
            return jsonify({'status': 'fail', 'message': res})

    def cd(self):
        """
        切换目录
        """
        path = request.form.get('path')
        cd_res = self.metadata_server.cd(path, self.working_directory)
        if cd_res == 'success':
            self.working_directory = path
            return jsonify({'status': 'success'})
        else:
            return jsonify({'status': 'fail', 'message': cd_res})

    def read_metadata(self):
        """
        读取传入path的metadata，可以是文件或者目录
        """
        path = request.args.get('path')
        if not self.metadata_server.exist(path):
            return jsonify({'status': 'fail', 'message': 'path does not exist'})
        return jsonify({'status': 'success', 'data': dict(self.metadata_server.pwd.metadata)})

    def upload(self):
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
        path = request.form.get('path')
        file = request.files['file']
        chunk_size = 2 * 1024 * 1024  # 2MB per chunk
        chunks = []
        chunk_num = 0
        main_chunk_list = []
        while True:
            chunk = file.stream.read(chunk_size)
            if not chunk:
                break
            chunk_num += 1
            main_chunk_list.append(chunk_num % 4)
            chunks.append(chunk)
        file_size = file.tell()
        metadata = {
            'file_size': file_size,
            'chunk_num': chunk_num,
            'main_chunk_list': main_chunk_list,
            'replications': [main_chunk_list.copy() for _ in range(self.replication_num)]
        }
        touch_res = self.metadata_server.touch(path, self.working_directory, metadata)

        # 创建一个线程，将chunk发送给对应的dataserver
        with ThreadPoolExecutor(max_workers=4) as executor:
            for chunk, chunk_num in zip(chunks, range(chunk_num)):
                executor.submit(self.send_chunk, self.data_servers[main_chunk_list[chunk_num]], chunk, path, chunk_num)

    def read(self, file_path):
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
app.add_url_rule('/upload', 'upload', ds.upload, methods=['POST'])
# 客户端通过访问http://localhost:9080/read_chunk来读取文件块
app.add_url_rule('/read', 'read', ds.read, methods=['POST'])
app.add_url_rule('/mkdir', 'mkdir', ds.mkdir, methods=['POST'])
app.add_url_rule('/ls', 'ls', ds.ls, methods=['POST'])
app.add_url_rule('/rm', 'rm', ds.rm, methods=['POST'])
app.add_url_rule('/touch', 'touch', ds.touch, methods=['POST'])
app.add_url_rule('/cd', 'cd', ds.cd, methods=['POST'])
app.add_url_rule('/read_metadata', 'read_metadata', ds.read_metadata, methods=['GET'])
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9080)
