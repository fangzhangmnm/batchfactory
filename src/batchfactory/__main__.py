
from .op._registery import generate_ops_md_str
import argparse, sys, os
from pathlib import Path

def find_project_root() -> Path:
    here = Path(__file__).resolve()
    parent = here.parent.parent.parent
    if (parent / "README.md").exists() and (parent / "src").is_dir():
        return parent
    return None

def read_file(file_path: Path) -> str:
    if not file_path.exists():
        print(f"File not found: {file_path}")
        return ""
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()
    
def get_code_demo(file_path: Path) -> str:
    text = read_file(file_path)
    text = text.split("# START_EXAMPLE_EXPORT", 1)[-1]
    text = text.split("# END_EXAMPLE_EXPORT", 1)[0].strip()
    text = "```python\n" + text + "\n```"
    return text


def generate_docs(project_root: Path):
    readme_str = read_file(project_root / "docs" / "README_template.md")
    readme_str = readme_str.replace("<!-- HIGHLIGHTED_OPS_PLACEHOLDER -->", 
                                    generate_ops_md_str())

    readme_str = readme_str.replace("<!-- QUICK_START_EXAMPLE_PLACEHOLDER -->", 
                                    get_code_demo(project_root / "examples" / "1_quickstart.py"))
    readme_str = readme_str.replace("<!-- TEXT_SEGMENTATION_EXAMPLE_PLACEHOLDER -->", 
                                    get_code_demo(project_root / "examples" / "3_text_segmentation.py"))
    readme_str = readme_str.replace("<!-- ROLEPLAY_EXAMPLE_PLACEHOLDER -->",
                                    get_code_demo(project_root / "examples" / "2_roleplay.py"))
    readme_str = readme_str.replace("<!-- EMBEDDING_EXAMPLE_PLACEHOLDER -->",
                                    get_code_demo(project_root / "examples" / "5_embeddings.py"))

    readme_path = project_root / "README.md"
    with open(readme_path, "w", encoding="utf-8") as readme_file:
        readme_file.write(readme_str)
    print(f"Updated README.md at: {readme_path}")




if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--update_docs", action="store_true", help="Update the ops documentation  (dev only)")

    args = parser.parse_args()
    if args.update_docs:
        project_root = find_project_root()
        if not project_root:
            print("--update_docs must be called in a dev environment, please check github repository for instructions.")
            sys.exit(1)
        generate_docs(project_root)