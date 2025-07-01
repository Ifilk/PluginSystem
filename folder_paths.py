import os

from configuration import singleton


@singleton("global")
class PathManager:
    def __init__(self, base_path: str, path_config: dict):
        self.base_path = os.path.abspath(base_path)
        self.folder_paths = {
            name: os.path.join(self.base_path, rel_path)
            for name, rel_path in path_config.items()
        }

    def get_path(self, name: str) -> str:
        return self.folder_paths[name]

    def ensure_folders_exist(self):
        for path in self.folder_paths.values():
            os.makedirs(path, exist_ok=True)

    def dir(self, name, dirname=None, make=True):
        if name not in self.folder_paths:
            path = os.path.join(self.base_path, name if dirname is None else dirname)
            self.folder_paths[name] = path
            if make:
                os.makedirs(path, exist_ok=True)
        return self.get_path(name)