import os
import shutil

class CacheFolder:
    def __init__(self,project_name:str,version:int,minor_version:int=None,patch_version:int=None,data_dir='./data'):
        self.project_name = project_name
        self.version = version or 0
        self.minor_version = minor_version or 0
        self.patch_version = patch_version or 0
        self.data_dir = data_dir
        os.makedirs(self.root_folder, exist_ok=True)
    @property
    def root_folder(self):
        version_str = '.'.join(map(str, [self.version, self.minor_version, self.patch_version]))
        return os.path.join(self.data_dir, self.project_name+'_v'+version_str)
    def __call__(self,relative_path):
        resolved_path = os.path.join(self.root_folder, relative_path)
        os.makedirs(os.path.dirname(resolved_path), exist_ok=True)
        return resolved_path
    def delete_all(self,warning=True):
        try:
            if warning:
                user_consent = input(f"Are you sure you want to delete all data in {self.root_folder}? Type 'DELETE ALL DATA' to confirm: ")
                if user_consent != 'DELETE ALL DATA':
                    print("Deletion cancelled.")
                    return
            if os.path.exists(self.root_folder):
                for root, dirs, files in os.walk(self.root_folder, topdown=False):
                    for name in files:
                        os.remove(os.path.join(root, name))
                    for name in dirs:
                        os.rmdir(os.path.join(root, name))
                os.rmdir(self.root_folder)
                print(f"All data in {self.root_folder} has been deleted.")
        except KeyboardInterrupt:
            print("Deletion interrupted by user.")
    def compress(self,archive_path:str=None, override_warning=True):
        if archive_path is None:
            archive_path = self.root_folder + '.zip'
        if not archive_path.endswith('.zip'):
            raise ValueError("Archive path must end with .zip")
        if os.path.exists(archive_path) and override_warning:
            user_consent = input(f"Archive {archive_path} already exists. Do you want to override it? Type 'yes' to confirm: ")
            if user_consent.lower() != 'yes':
                print("Compression cancelled.")
                return
        shutil.make_archive(archive_path.replace('.zip', ''), 'zip', self.root_folder)
        print(f"Backup Data compressed to {archive_path}")

__all__ = [
    'CacheFolder'
]

    