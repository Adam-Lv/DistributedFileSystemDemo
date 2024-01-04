import pickle
import time
import os
from collections import OrderedDict
from hashlib import md5
import sys


class MetadataServer:
    metadata_path = '/meta_data'

    def __init__(self):
        # 整个目录树的根节点
        if not os.path.exists(self.metadata_path):
            os.mkdir(self.metadata_path)
        if len(os.listdir(self.metadata_path)) == 0:
            self.root = FolderNode(None, '/')
            self.save_pickle()
        else:
            with open(f'{self.metadata_path}/root.pkl', 'rb') as f:
                self.root = pickle.loads(f.read())
        self.pwd = self.root

    def save_pickle(self):
        with open(f'{self.metadata_path}/root.pkl', 'wb') as f:
            f.write(pickle.dumps(self.root))

    def exist(self, path):
        """
        检查输入的文件或文件夹是否存在，如果存在则会将pwd指针指向该文件或文件夹
        :param path: dfs文件系统中的绝对路径
        :return:
        """
        if path == '/':
            self.pwd = self.root
            return True
        path = path.split('/')
        self.pwd = self.root
        for i, folder in enumerate(path[1:]):
            try:
                if folder == '.':
                    continue
                elif folder == '..':
                    self.pwd = self.pwd.parent
                    continue
                try:
                    self.pwd = self.pwd.children[folder]
                except KeyError:
                    # children中没有子目录
                    return False
            except AttributeError:
                # FileNode没有children属性，所以此时已经来到了file节点
                if self.pwd.name != folder:
                    return False
                if i != len(path) - 2:
                    # 这说明还没到最后一个节点，但是已经来到了file节点，所以说明路径有误
                    return False
        return True

    @staticmethod
    def get_abs_path(path, working_directory):
        # 如果path是绝对路径，则直接规范化路径
        if os.path.isabs(path):
            return os.path.normpath(path)
        # 如果path是相对路径，首先将工作目录和path合并，然后再规范化
        joined_path = os.path.join(working_directory, path)
        return os.path.normpath(os.path.abspath(joined_path))

    def ls(self, path, working_directory):
        if path is None:
            abs_path = working_directory
        else:
            abs_path = self.get_abs_path(path, working_directory)
        pwd = self.pwd
        cd_res = self.cd(abs_path, '/')
        if cd_res != 'success':
            return cd_res
        folder = self.pwd
        self.pwd = pwd
        res = list(folder.metadata['files'])
        if abs_path == '/':
            res = ['.'] + res
        else:
            res = ['.', '..'] + res
        return res

    def mkdir(self, path, working_directory):
        abs_path = self.get_abs_path(path, working_directory)
        parent = "/".join(abs_path.split('/')[:-1])
        print(abs_path, parent)
        cd_res = self.cd(parent, '/')
        if cd_res != 'success':
            return cd_res
        new_node = FolderNode(parent, abs_path)
        self.pwd.add_child(new_node)
        self.save_pickle()
        return cd_res

    def touch(self, path, working_directory, metadata=None):
        abs_path = self.get_abs_path(path, working_directory)
        if self.exist(abs_path):
            return f'{path} already exists.'
        parent = "/".join(path.split('/')[:-1])
        self.cd(parent, working_directory)
        if metadata is None:
            new_file = FileNode(parent, abs_path, file_size=0, chunk_num='0', main_chunk_list=[],
                                replications=0, chunk_name=None)
        else:
            new_file = FileNode(parent, abs_path, **metadata)
        self.pwd.add_child(new_file)
        self.save_pickle()
        return 'success'

    def cd(self, path, working_directory):
        abs_path = self.get_abs_path(path, working_directory)
        pwd = self.pwd
        if not self.exist(abs_path):
            self.pwd = pwd
            return f'{path} does not exist.'
        if isinstance(self.pwd, FileNode):
            message = f'{self.pwd.metadata["name"]} is not a directory.'
            self.pwd = pwd
            return message
        return 'success'

    def rm(self, path, working_directory):
        ### 实现删除文件或者文件夹功能
        abs_path = self.get_abs_path(path, working_directory)
        if not self.exist(abs_path):
            return f'{path} does not exist.'
        parent = self.pwd.parent
        parent.remove_child(self.pwd)
        self.save_pickle()
        return 'success'

    def _recursive_delete_folder(self, folder):
        """
        递归删除文件夹下的所有文件和文件夹
        :param folder: 文件夹节点
        """
        if folder == self.root:
            # for local_file in os.listdir(MetadataServer.metadata_path):
            #     os.remove(local_file)
            folder.children = []
            # folder.children_pkls = []
            folder.metadata['files'] = OrderedDict()
            self.pwd = self.root
            return
        # 删除当前文件夹的metadata文件
        # os.remove(folder.local_file)
        for child in folder.children:
            # if isinstance(child, FileNode):
            #     删除该文件的metadata文件
            #     os.remove(child.local_file)
            if isinstance(child, folder):
                self._recursive_delete_folder(child)
        # 递归完全退出时，更新父节点的信息
        if folder == self.pwd:
            # folder.parent.children_pkls.remove(folder.local_file)
            folder.parent.children.remove(folder)
            folder.parent.metadata['files'].pop(folder.name)
        self.pwd = folder.parent
        refcount = sys.getrefcount(folder)
        if refcount != 2:
            raise OSError(f'{folder} is not deleted.')
        return

    def read_metadata(self, path, working_directory):
        abs_path = self.get_abs_path(path, working_directory)
        if not self.exist(abs_path):
            return f'{path} does not exist.'
        return self.pwd.metadata


class MetadataNode:
    def __init__(self, parent, path, **kwargs):
        """
        :param parent: 父文件夹
        :param path: 绝对路径
        """
        self.parent: MetadataNode = parent
        self.path = path
        if path[-1] == '/':
            self.name = path.split('/')[-2]
        else:
            self.name = path.split('/')[-1]
        self.metadata = OrderedDict()

    def update(self, **kwargs):
        """
        更新metadata内容
        """
        for k, v in kwargs.items():
            if k not in self.metadata:
                raise KeyError(f'{k} is not in metadata.')
            self.metadata[k] = v
        self.metadata['update-time'] = time.time()

    def __eq__(self, other):
        return self.path == other.path


class FolderNode(MetadataNode):
    def __init__(self, parent, path):
        super().__init__(parent, path)
        self.children = OrderedDict()

        create_time = time.time()
        self.metadata['create_time'] = create_time
        self.metadata['update_time'] = create_time
        self.metadata['name'] = self.name
        self.metadata['files'] = []

    def add_child(self, child):
        self.metadata['files'].append(child.name)
        self.metadata['update_time'] = time.time()
        self.children[child.name] = child

    def remove_child(self, child):
        self.metadata['files'].pop(child.name)
        self.metadata['update_time'] = time.time()
        del self.children[child.name]


class FileNode(MetadataNode):
    def __init__(self, parent, path, **metadata):
        super().__init__(parent, path, **metadata)

        self.metadata['create_time'] = time.time()
        self.metadata['update_time'] = time.time()
        self.metadata['name'] = path.split('/')[-1]
        self.metadata['file_size'] = metadata['file_size']
        self.metadata['chunk_num'] = metadata['chunk_num']
        self.metadata['main_chunk_list'] = metadata['main_chunk_list']
        self.metadata['replications'] = metadata['replications']
        self.metadata['chunk_name'] = metadata['chunk_name']
