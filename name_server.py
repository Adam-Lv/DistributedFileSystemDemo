from metadata_server import MetadataServer

class NameServer:
    def __init__(self, metadata_server):
        """
        从metadata中读取之前保存的metadata_file_tree，如果没有则新建
        """
        self.pwd = '/'
        self.metadata_server: MetadataServer = metadata_server

    def run(self):
        """
        启动socket，接收来自client的请求，根据请求类型调用相应的函数
        """

    def _mkdir(self, path):
        """
        创建目录
        """
        return self.metadata_server.mkdir(path, self.pwd)


    def _ls(self, path):
        """
        列出目录下的文件
        """

    def _rm(self, path):
        """
        删除文件
        """

    def _touch(self, path):
        """
        创建文件
        """

    def _cd(self, path):
        """
        切换目录
        """

    def _read_metadata(self, path):
        """
        读取传入path的metadata，可以是文件或者目录
        """

    def _upload(self, file, path):
        """
        从客户端接收上传的文件流，根据文件大小确定对应的分块所在dataserver的位置，
        并将文件分块保存到dataserver中，同时更新metadata_file_tree。
        """

    def _read(self, file_path):
        """
        读取文件：根据文件路径读取metadata，根据metadata中的文件大小确定需要读取的分块，
        从dataserver中读取分块并拼接成完整的文件，返回文件流。
        """
