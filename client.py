import json
import requests
import os


class Client:
    def __init__(self, name_server_url):
        self.name_server = name_server_url

    def _upload(self, file, path):
        try:
            with open(file, 'rb') as f:
                file_r = f.read()
        except FileNotFoundError:
            print(f"Error: {file} does not exist.")
            return
        file_name = os.path.basename(file)
        path = f'{path}/{file_name}'
        response = requests.post(
            f'http://{self.name_server}:9080/upload',
            files={'file': file_r},
            data={'path': path}
        )
        if response.json()['status'] != 'success':
            print(f"Error: {response.json()['message']}")

    def _read(self, file_path):
        response = requests.post(
            f'http://{self.name_server}:9080/read',
            data={'path': file_path}
        )
        try:
            if response.json()['status'] != 'success':
                print(f"Error: {response.json()['message']}")
                return
        except json.decoder.JSONDecodeError as e:
            pass
        file = response.content
        file_name = file_path.split('/')[-1]
        with open(f'test_files/download/{file_name}', 'wb') as f:
            f.write(file)
        print(f"Successfully downloaded {file_name} to test_files/download/")

    def _mkdir(self, path):
        response = requests.post(
            f'http://{self.name_server}:9080/mkdir',
            data={'path': path}
        )
        if response.json()['status'] != 'success':
            print(f"Error: {response.json()['message']}")

    def _ls(self, path='.'):
        response = requests.post(
            f'http://{self.name_server}:9080/ls',
            data={'path': path}
        )
        if response.json()['status'] != 'success':
            print(f"Error: {response.json()['message']}")
            return
        list_files = response.json()['data']
        res = " ".join(list_files)
        print(res)

    def _rm(self, path):
        response = requests.post(
            f'http://{self.name_server}:9080/rm',
            data={'path': path}
        )
        if response.json()['status'] != 'success':
            print(f"Error: {response.json()['message']}")

    def _touch(self, path):
        response = requests.post(
            f'http://{self.name_server}:9080/touch',
            data={'path': path}
        )
        if response.json()['status'] != 'success':
            print(f"Error: {response.json()['message']}")

    def _cd(self, path):
        response = requests.post(
            f'http://{self.name_server}:9080/cd',
            data={'path': path}
        )
        if response.json()['status'] != 'success':
            print(f"Error: {response.json()['message']}")

    def _pwd(self):
        response = requests.get(f'http://{self.name_server}:9080/pwd')
        if response.json()['status'] != 'success':
            print(f"Error: {response.json()['message']}")
            return
        print(response.json()['working_directory'])

    def _read_metadata(self, path):
        response = requests.post(
            f'http://{self.name_server}:9080/read_metadata',
            data={'path': path}
        )
        if response.json()['status'] != 'success':
            print(f"Error: {response.json()['message']}")
            return
        print(response.json()['data'])

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
            elif command == 'pwd':
                self._pwd()
            elif command == 'read_metadata':
                self._read_metadata(*args)
            elif command == 'exit':
                break
            else:
                print("Invalid command. Supported commands: upload, read, mkdir, ls, "
                      "rm, touch, cd, pwd, read_metadata, exit")


if __name__ == '__main__':
    client = Client(name_server_url='localhost')
    client.run()
