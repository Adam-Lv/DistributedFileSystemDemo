import os
import socket
import yaml
from flask import Flask, request, jsonify
import requests
from concurrent.futures import ThreadPoolExecutor
from hashlib import md5


class DataServer:
    data_disk_path = '/data_disk/'
    # metadata_path = '/meta_data/'

    def __init__(self):
        if not os.path.exists(self.data_disk_path):
            os.mkdir(self.data_disk_path)
        # if not os.path.exists(self.metadata_path):
        #     os.mkdir(self.metadata_path)

        all_data_servers = self.get_hostnames()  # 所有的data_server的hostname
        self.hostname = socket.gethostname()  # 当前data_server的hostname
        # 除了自己以外的其他dataserver。分发文件块的时候需要和他们进行通信
        self.peer_data_servers = [data_server for data_server in all_data_servers if data_server != self.hostname]

    @staticmethod
    def _cal_checksum(data):
        """
        计算文件块的checksum
        """
        m = md5()
        m.update(data)
        return m.hexdigest()

    @staticmethod
    def get_hostnames():
        """
        根据docker-compose获取所有的data_server的hostname
        """
        all_data_servers = []
        with open('docker-compose.yml', 'r') as f:
            data = yaml.load(f.read(), Loader=yaml.FullLoader)
        for service in data['services']:
            if service.startswith('data_server'):
                all_data_servers.append(service)
        return all_data_servers

    @staticmethod
    def _write(data, file_name, chunk_num):
        """
        将文件流写入本地，文件名命名为{原文件名}.{块号}.{checksum},返回文件的绝对路径
        """
        if len(file_name.split('.')) == 1:
            # 这里是指文件来自于NameServer，所以文件名不包含checksum
            checksum = DataServer._cal_checksum(data)
            file_name = os.path.join(DataServer.data_disk_path, 'main', f'{file_name}.{chunk_num}.{checksum}')
        else:
            file_name, checksum = file_name.split('.')[0], file_name.split[2]
            file_name = os.path.join(DataServer.data_disk_path, 'replication', f'{file_name}.{chunk_num}.{checksum}')
        ### 这里的wb保证了当文件不存在时，会创建文件，如果文件存在，则会覆盖文件
        with open(file_name, 'wb') as f:
            f.write(data)
        return file_name

    @staticmethod
    def read_chunk():
        """
        读取本地文件，返回读取的二进制文件内容
        先在本地检查读进来的文件的checksum和传入的checksum是否一致,再发给nameserver
        """
        ### 得到name server传来的文件名，这里原始的client传来的文件名应该是不包含块数和checksum的
        file_name = request.form.get('file_name')
        checksum = file_name.split('.')[2]
        try:
            with open(file_name, 'rb') as f:
                data = f.read()
        except FileNotFoundError:
            return jsonify({'status': 'fail', 'data': None, 'message': 'file not found'})
        cal_checksum = DataServer._cal_checksum(data)
        if checksum == cal_checksum:
            # checksum一致，说明文件没有损坏
            return jsonify({'status': 'success', 'data': data})
        else:
            return jsonify({'status': 'fail', 'data': None, 'message': 'file broken'})

    def upload_chunk(self):
        """
        接收来自其他DataServer的文件流，或者接收来自NameServer的文件流，然后将文件保存到本地。
        如果是来自NameServer的文件流，还需要将文件分发给其他DataServer
        同时给NameServer返回本地文件的路径，以便NameServer更新metadata
        """
        source = request.form.get('source')
        file_name, data = request.files['file']
        chunk_num = request.form.get('chunk_num')
        local_path = DataServer._write(data, file_name, chunk_num)
        # 如果是name_server发的，就需要分发给其他data_server
        if source == 'name_server':
            # 用线程池发送多个文件块，并发操作提高速度
            with ThreadPoolExecutor(max_workers=4) as executor:
                for data_server in self.peer_data_servers:
                    executor.submit(self.send_chunk, self.hostname, data_server, data, file_name, chunk_num)
        return jsonify({'status': 'success', 'local_path': local_path})

    @staticmethod
    def send_chunk(source, host, data, file_name, chunk_num):
        """
        发送文件块给其他DataServer或NameServer
        """
        requests.post(
            f'http://{host}:9080/upload_chunk',
            files={'file': (file_name, data)},
            data={'source': source, 'chunk_num': chunk_num}
        )


if __name__ == '__main__':
    app = Flask(__name__)
    ds = DataServer()
    # 客户端通过访问http://localhost:9080/upload_chunk来上传文件块
    app.add_url_rule('/upload_chunk', 'upload_chunk', ds.upload_chunk, methods=['POST'])
    # 客户端通过访问http://localhost:9080/read_chunk来读取文件块
    app.add_url_rule('/read_chunk', 'read_chunk', ds.read_chunk, methods=['POST'])
    app.run(host='0.0.0.0', port=9080)
