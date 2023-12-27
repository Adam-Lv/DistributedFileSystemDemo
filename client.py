import requests
import os
class Client:
    def __init__(self, name_server_url):
        self.name_server = 'name_server'

    def _upload(self, file, path):
        response = requests.post(
            f'http://{self.name_server}:9080/upload',
            files={'file': file},
            data={'path': path}
        )
        print(response.json())

    def _read(self, file_path):
        response = requests.get(
            f'http://{self.name_server}:9080/read',
            data={'path': file_path}
        )
        if response.status_code == 200:
            print(response.json())
        else:
            print(f"Error: {response.json()['message']}")

    def _mkdir(self, path):
        response = requests.post(
            f'http://{self.name_server}:9080/mkdir',
            data={'path': path}
        )
        print(response.json())

    def _ls(self, path):
        response = requests.get(
            f'http://{self.name_server}:9080/ls',
            data={'path': path}
        )
        print(response.json())

    def _rm(self, path):
        response = requests.post(
            f'http://{self.name_server}:9080/rm',
            data={'path': path}
        )
        print(response.json())

    def _touch(self, path):
        response = requests.post(
            f'http://{self.name_server}:9080/touch',
            data={'path': path}
        )
        print(response.json())

    def _cd(self, path):
        response = requests.post(
            f'http://{self.name_server}:9080/cd',
            data={'path': path}
        )
        print(response.json())

    def _read_metadata(self, path):
        response = requests.get(
            f'http://{self.name_server}:9080/read_metadata',
            data={'path': path}
        )
        print(response.json())

    def run(self):
        """
        while True:
            1. 从用户输入中解析出命令和参数
            2. 根据命令调用相应的函数
        """
        while True:
            user_input = input("Enter command: ")
            parts = user_input.split()
            command = parts[0].lower()
            args = parts[1:]
            if command == 'upload':
                self._upload(*args)
            elif command == 'read':
                self._read(*args)
            elif command == 'mkdir':
                self._mkdir(*args)
            elif command == 'ls':
                self._ls(*args)
            elif command == 'rm':
                self._rm(*args)
            elif command == 'touch':
                self._touch(*args)
            elif command == 'cd':
                self._cd(*args)
            elif command == 'read_metadata':
                self._read_metadata(*args)
            elif command == 'exit':
                break
            else:
                print("Invalid command. Supported commands: upload, read, mkdir, ls, rm, touch, cd, read_metadata, exit")
client = Client(name_server_url='name_server')
client.run()