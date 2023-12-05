import os
import json
import socket
import yaml
from hashlib import md5


class DataServer:
    def __init__(self):
        self.data_disk_path = '/data_disk/'
        self.meta_data_path = '/meta_data/'
        if not os.path.exists(self.data_disk_path):
            os.mkdir(self.data_disk_path)
        if not os.path.exists(self.meta_data_path):
            os.mkdir(self.meta_data_path)

        all_data_servers = self.get_hostnames()
        self.socket = socket.socket()
        self.socket.bind(('0.0.0.0', '2322'))
        self.hostname = socket.gethostname()
        self.peer_data_servers = [data_server for data_server in all_data_servers if data_server != self.hostname]

    def run(self):
        """
        启动socket，接收来自client的请求，根据请求类型调用相应的函数
        """
        while True:
            self.socket.listen(5)
            # 这里接收的是要发送文件的元数据
            conn, addr = self.socket.accept()

    def handle_request(self, conn, addr):
        """
        处理来自client的请求
        """
        while True:
            data = conn.recv(1024)
            if not data:
                break
        data = json.loads(data.decode())
        source = data['source']
        file_name = data['file_name']
        chunk_num = data['chunk_num']
        if source == 'name_server':
            self.socket.send('ok'.encode('utf-8'))
            # 新建套接字，用于接收文件流
            file_stream_socket = socket.socket()
            file_stream_socket.bind((addr[0], 2323))
            file_stream_socket.listen(1)
            file_stream_conn, file_stream_addr = file_stream_socket.accept()
            # 写入文件到本地
            while True:
                data = file_stream_conn.recv(1024)
                if not data:
                    break
            self._write(data, file_name, chunk_num)
            # 将文件元数据、文件流发送给其他dataserver
            for peer_data_server in self.peer_data_servers:
                peer_data_server_socket = socket.socket()
                peer_data_server_socket.connect((peer_data_server, 2322))
                peer_data_server_socket.send(data)
                peer_data_server_socket.close()

    @staticmethod
    def _cal_checksum(data):
        """
        计算文件的checksum
        """
        m = md5()
        m.update(data)
        return m.hexdigest()

    @staticmethod
    def get_hostnames():
        all_data_servers = []
        with open('docker-compose.yml', 'r') as f:
            data = yaml.load(f.read(), Loader=yaml.FullLoader)
        for service in data['services']:
            if service.startswith('data_server'):
                all_data_servers.append(service)
        return all_data_servers

    def _write(self, data, file_name, chunk_num):
        """
        将文件流写入本地，文件名命名为{原文件名}.{块号}.{checksum}
        """
        checksum = self._cal_checksum(data)
        file_name = file_name + '.' + str(chunk_num) + '.' + checksum
        with open(self.data_disk_path + file_name, 'wb') as f:
            f.write(data)

    def _read(self, checksum):
        """
        读取本地文件，返回文件流
        先在本地检查读进来的文件的checksum和传入的checksum是否一致，再发给client
        """
