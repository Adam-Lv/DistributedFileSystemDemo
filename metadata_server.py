import pickle
from abc import abstractmethod
import time
import os
from collections import OrderedDict
from hashlib import md5


class MetadataServer:
    metadata_path = '/meta_data/'

    def __init__(self):
        # 整个目录树的根节点
        if not os.path.exists(self.metadata_path):
            os.mkdir(self.metadata_path)
        if len(os.listdir(self.metadata_path)) == 0:
            root = FolderNode(None, '/')
            with open(f'{self.metadata_path}/root.pkl', 'wb') as f:
                f.write(pickle.dumps(root))
        else:
            with open(f'{self.metadata_path}/root.pkl', 'rb') as f:
                root = pickle.loads(f.read())
        self.root = root
        # 当前指针指向的节点
        self.pwd = root

    def reconstruct_tree(self, root):
        """
        从根节点的pkl文件开始重新构建整个目录树
        :param root: 根节点的pkl文件
        :return: 根节点
        """
        with open(root, 'rb') as f:
            root = pickle.loads(f.read())
        self.root = root
        self.pwd = root
        my_queue = []
        for child_pkl in root.children_pkls:
            my_queue.append((child_pkl, root))
        while my_queue:
            child_pkl, parent = my_queue.pop(0)
            self.pwd = parent
            with open(child_pkl, 'rb') as f:
                child = pickle.loads(f.read())
            child.parent = parent
            self.pwd.children.append(child)
            if isinstance(child, FolderNode):
                for child_pkl in child.children_pkls:
                    my_queue.append((child_pkl, root))
        return root

    def exist(self, path):
        """
        检查输入的文件或文件夹是否存在，如果存在则会将pwd指针指向该文件或文件夹
        :param path:
        :return:
        """
        path = path.split('/')
        self.pwd = self.root
        enter_flag = False
        for i, folder in enumerate(path[1:]):
            try:
                for child in self.pwd.children:
                    if child.metadata['name'] == folder:
                        self.pwd = child
                        enter_flag = True
                        break
                if enter_flag:
                    # 找到了子目录
                    enter_flag = False
                else:
                    # children中没有子目录
                    return False
            except AttributeError:
                # FileNode没有children属性，所以此时已经来到了file节点
                if self.pwd.metadata['name'] != folder:
                    return False
                if i != len(path) - 2:
                    # 这说明还没到最后一个节点，但是已经来到了file节点，所以说明路径有误
                    return False
        return True

    def ls(self, path, working_directory):
        ...

    def mkdir(self, path, working_directory):
        # path = path.split('/')
        # parent = self.pwd + '/'.join(path[:-1])
        # if not self.exist(parent):
        #     return False
        # parent = self.cd(parent)
        # new_node = FolderNode(parent, path)
        # parent.add_child(new_node)
        ...

    def touch(self, path, working_directory):
        ...

    def cd(self, path):
        if not self.exist(path):
            raise FileNotFoundError(f'{path} does not exist.')
        if isinstance(self.pwd, FileNode):
            raise NotADirectoryError(f'{self.pwd.metadata["name"]} is not a directory.')
        return self.pwd


class MetadataNode:
    def __init__(self, parent, path, **kwargs):
        """
        :param parent: 父文件夹
        :param path: 绝对路径
        """
        self.parent: MetadataNode = parent
        self.path: str = path
        self.name: str = path.split('/')[-1]
        self._metadata = OrderedDict()
        self.local_file: str = ''

    @property
    @abstractmethod
    def metadata(self):
        ...

    def update(self, **kwargs):
        """
        更新metadata内容
        """
        for k, v in kwargs.items():
            if k not in self._metadata:
                raise KeyError(f'{k} is not in metadata.')
            self._metadata[k] = v
        self._metadata['update-time'] = time.time()
        with open(self.local_file, 'wb') as f:
            f.write(pickle.dumps(self))


class FolderNode(MetadataNode):
    def __init__(self, parent, path):
        super().__init__(parent, path)
        self.children: list[MetadataNode] = []

        create_time = time.time()
        self._metadata['create-time'] = create_time
        self._metadata['update-time'] = create_time
        self._metadata['name'] = path.split('/')[-1]
        self._metadata['files'] = OrderedDict()
        self._metadata['files']['.'] = self

        self.children.append(self)
        if self.parent is not None:
            self._metadata['files']['..'] = self.parent
            self.children.append(parent)

        # 只有子节点的pkl文件路径才需要存，父节点、本节点的不在这里存
        self.children_pkls: list[str] = []

    def add_child(self, child):
        self._metadata['files'][child.name] = child
        self.children.append(child)
        self.children_pkls.append(child.local_file)
        with open(self.local_file, 'wb') as f:
            f.write(pickle.dumps(self))

    @property
    def metadata(self):
        return self._metadata


class FileNode(MetadataNode):
    def __init__(self, parent, path, **metadata):
        super().__init__(parent, path, **metadata)

        self._metadata['create-time'] = time.time()
        self._metadata['update-time'] = time.time()
        self._metadata['name'] = path.split('/')[-1]
        self._metadata['file-size'] = metadata['file_size']
        self._metadata['chunk-num'] = metadata['chunk_num']
        self._metadata['main-chunk-list'] = metadata['main_chunk_list']
        self._metadata['replications'] = metadata['replications']

        m = md5()
        m.update(self._metadata.__str__().encode('utf-8'))
        self.local_file = f"{MetadataServer.metadata_path}/{m.hexdigest()}.pkl"
        with open(self.local_file, 'wb') as f:
            f.write(pickle.dumps(self))

    @property
    def metadata(self):
        return self._metadata
