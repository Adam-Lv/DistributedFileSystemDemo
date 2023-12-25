### 实现删除文件或者文件夹功能
def rm(self, path, working_directory):
    if path[0] != '/':
        path = working_directory + '/' + path
    if not self.exist(path):
        raise FileNotFoundError(f'{path} does not exist.')
    if isinstance(self.pwd, FileNode):
        ### 删除该文件
        self.pwd.parent.children_pkls.remove(self.pwd.local_file)
        os.remove(self.pwd.local_file)
        self.pwd.parent.children.remove(self.pwd)
        self.pwd.parent.metadata['files'].pop(self.pwd.metadata['name'])
        self.pwd.parent.update()
    ### 如果是文件夹，需要递归删除文件夹下的所有文件和文件夹
    elif isinstance(self.pwd, FolderNode):
        # 递归删除所有子节点
        self._recursive_delete_folder(self.pwd)

    #self.reconstruct_tree(f'{self.metadata_path}/root.pkl')
    return True

def _recursive_delete_folder(self, FolderNode):
    """
    递归删除文件夹下的所有文件和文件夹
    :param folder: 文件夹节点
    :return:
    """
    # 复制一份子节点列表，因为在迭代过程中会修改这个列表
    for child in list(FolderNode.children):
        if isinstance(child, FileNode):
            ### 删除该文件
            FolderNode.children_pkls.remove(child.local_file)
            os.remove(child.local_file)
            FolderNode.children.remove(child)
            FolderNode.metadata['files'].pop(child.metadata['name'],None)
        elif isinstance(child, FolderNode):
            self._recursive_delete_folder(child)
    # 如果文件夹不是根节点，则需要更新父节点的信息
    if FolderNode.parent:
        FolderNode.parent.children_pkls.remove(FolderNode.local_file)
        FolderNode.parent.children.remove(FolderNode)
        FolderNode.parent.metadata['files'].pop(FolderNode.metadata['name'], None)
        FolderNode.parent.update()
    os.remove(FolderNode.local_file)