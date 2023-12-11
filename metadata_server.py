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
        path = path.split('/')
        parent = self.pwd + '/'.join(path[:-1])
        if not self.exist(parent):
            return False
        parent = self.cd(parent)
        new_node = FolderNode(parent, path)
        parent.add_child(new_node)

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
        self.parent = parent
        self.path = path
        self.name = path.split('/')[-1]
        self.local_file = None

    @property
    @abstractmethod
    def metadata(self): ...


class FolderNode(MetadataNode):
    def __init__(self, parent, path):
        super().__init__(parent, path)
        self.children = []
        create_time = time.time()
        self._metadata = OrderedDict()
        self._metadata['create-time'] = create_time
        self._metadata['name'] = path.split('/')[-1]
        self._metadata['files'] = OrderedDict()
        self._metadata['files']['.'] = self
        self.children.append(self)
        if self.parent is not None:
            self._metadata['files']['..'] = self.parent
            self.children.append(parent)

    def add_child(self, child):
        self._metadata['files'][child.name] = child
        self.children.append(child)

    @property
    def metadata(self):
        return self._metadata


class FileNode(MetadataNode):
    def __init__(self, parent, path, **metadata):
        super().__init__(parent, path, **metadata)

        self._metadata = OrderedDict()
        self._metadata['create-time'] = time.time()
        self._metadata['name'] = path.split('/')[-1]
        self._metadata['file-size'] = metadata['file-size']
        self._metadata['chunk-num'] = metadata['chunk-num']
        self._metadata['main-chunk-list'] = metadata['main-chunk-list']
        self._metadata['replications'] = metadata['replications']
        m = md5()
        m.update(self._metadata.__str__().encode('utf-8'))
        self.local_file = f"{metadata['metadata-path']}{m.hexdigest()}.pkl"

    @property
    def metadata(self):
        return self._metadata
