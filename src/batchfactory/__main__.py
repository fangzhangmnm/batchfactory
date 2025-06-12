
from .op import _generate_all_ops_md_str
import argparse, sys, os
from pathlib import Path

def find_project_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "src").is_dir() and (
            (parent / "README.md").exists()
        ):
            return parent
    return None


def generate_doc(project_root: Path):
    readme_path = project_root / "README.md"
    readme_template_path = project_root / "docs" / "README_template.md"
    all_ops_placeholder = "<!-- ALL_OPS_PLACEHOLDER -->"
    if not readme_template_path.exists():
        print(f"Template file not found: {readme_template_path}")
        return
    with open(readme_template_path, "r", encoding="utf-8") as template_file:
        readme_str = template_file.read()
    all_ops_str = _generate_all_ops_md_str()
    readme_str = readme_str.replace(all_ops_placeholder, all_ops_str)
    with open(readme_path, "w", encoding="utf-8") as readme_file:
        readme_file.write(readme_str)
    print(f"Updated README.md at: {readme_path}")




if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--update_doc", action="store_true", help="Update the ops documentation  (dev only)")

    args = parser.parse_args()
    if args.update_doc:
        project_root = find_project_root()
        if not project_root:
            print("Could not find project root.")
            sys.exit(1)
        generate_doc(project_root)