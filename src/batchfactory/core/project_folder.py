import shutil
from pathlib import Path
from contextvars import ContextVar
from typing import Dict, Any

_current_project: ContextVar["ProjectFolder"] = ContextVar("current_project", default=None)

class ProjectFolder:
    "Manage a versioned project folder under a common data directory."
    def __init__(self,project_name:str,version=0,minor_version=0,patch_version=0,data_dir:str|Path='./data'):
        if not isinstance(project_name, str):
            raise ValueError("Project name must be a string, not a Path object.")
        self.project_name = project_name
        self.version, self.minor_version, self.patch_version = version, minor_version, patch_version
        self.data_dir = Path(data_dir)
        self.root_folder.mkdir(parents=True, exist_ok=True)
        self.default_brokers:Dict[type, Any] = {}
        self.op_name_count:Dict[str, int] = {}
    @property
    def root_folder(self)->Path:
        version_str = '.'.join(map(str, [self.version, self.minor_version, self.patch_version]))
        return self.data_dir / f"{self.project_name}_v{version_str}"
    def resolve_path(self,relative_path:str|Path, mkdir=True)->Path:
        resolved_path = self.root_folder / Path(relative_path)
        if not resolved_path.is_relative_to(self.root_folder):
            raise ValueError(f"Resolved path {resolved_path} is outside the project folder {self.root_folder}.")
        if mkdir:
            resolved_path.parent.mkdir(parents=True, exist_ok=True)
        return resolved_path
    def delete(self,relative_path:str|Path):
        if not get_user_consent(f"Are you sure you want to delete {relative_path} in {self.root_folder}?", 'DELETE'):
            print("Deletion cancelled.")
            return
        resolved_path = self.resolve_path(relative_path, mkdir=False)
        if resolved_path.exists():
            if resolved_path.is_dir():
                shutil.rmtree(resolved_path)
            else:
                resolved_path.unlink()
            print(f"{resolved_path} has been deleted.")
        else:
            print(f"{resolved_path} does not exist.")
    def delete_all(self,warning=True):
        if warning and not get_user_consent(f"Are you sure you want to delete all data in {self.root_folder}?", 'DELETE ALL DATA'):
            print("Deletion cancelled.")
            return
        if self.root_folder.exists():
            shutil.rmtree(self.root_folder)
            print(f"All data in {self.root_folder} has been deleted.")
        else:
            print(f"{self.root_folder} does not exist. Nothing to delete.")
    def compress(self,archive_path:str|Path=None, override_warning=True):
        if archive_path is None:
            archive_path = self.root_folder.with_suffix('.zip')
        else:
            archive_path = Path(archive_path)
        if not archive_path.suffix == '.zip':
            raise ValueError("Archive path must end with .zip")
        if archive_path.exists() and override_warning:
            if not get_user_consent(f"Archive {archive_path} already exists. Do you want to override it?", 'yes'):
                print("Compression cancelled.")
                return
        shutil.make_archive(archive_path.with_suffix(''), 'zip', self.root_folder)
        print(f"Backup Data compressed to {archive_path}")
    def __getitem__(self,relative_path):
        return self.resolve_path(relative_path)
    def __delitem__(self,relative_path):
        self.delete(relative_path)
    def __repr__(self):
        return f"<ProjectFolder {self.project_name}, v{self.version}.{self.minor_version}.{self.patch_version}> at {self.root_folder}>"
    def __enter__(self):
        global _current_project
        self._context_token = _current_project.set(self)
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        global _current_project
        _current_project.reset(self._context_token)
        return False
    @staticmethod
    def get_current()->"ProjectFolder":
        if _current_project.get() is None:
            raise RuntimeError("No current project set. Use 'with ProjectFolder(...):' to set the current project.")
        return _current_project.get()
    def get_default_broker(self, broker_type:type):
        if broker_type not in self.default_brokers:
            self.default_brokers[broker_type] = broker_type(self["broker_cache"] / f"{broker_type.__name__}.jsonl")
        return self.default_brokers[broker_type]
    def set_default_broker(self, broker:Any):
        if type(broker) in self.default_brokers:
            raise ValueError(f"Broker of type {type(broker)} is already set as default.")
        self.default_brokers[type(broker)] = broker
    def generate_op_path(self, op:str|Any):
        if isinstance(op, str):
            op_name = op
        elif isinstance(op, type):
            op_name = op.__name__
        else:
            op_name = type(op).__name__
        count = self.op_name_count.get(op_name, 0)
        self.op_name_count[op_name] = count + 1
        return self.resolve_path(f"op_cache/{op_name}_{count}.jsonl")


def get_user_consent(prompt,consent)->bool:
    prompt = prompt + f" Type '{consent}' to confirm: "
    user_consent = input(prompt)
    return user_consent == consent



__all__ = [
    'ProjectFolder'
]

    