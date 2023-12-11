class Client:
    def __init__(self):
        ...

    def _upload(self, file, path):
        ...

    def _read(self, file_path):
        ...

    def _mkdir(self, path):
        ...

    def _ls(self, path):
        ...

    def _rm(self, path):
        ...

    def _touch(self, path):
        ...

    def _cd(self, path):
        ...

    def _read_metadata(self, path):
        ...

    def run(self):
        """
        while True:
            1. 从用户输入中解析出命令和参数
            2. 根据命令调用相应的函数
        """
