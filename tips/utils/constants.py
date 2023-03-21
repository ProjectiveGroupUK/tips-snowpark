from pathlib import Path

PACKAGE_DIRECTORY = Path(__file__).parent.parent.resolve()
ROOT_DIRECTORY = PACKAGE_DIRECTORY.parent
JINJA_TEMPLATES_DIRECTORY = PACKAGE_DIRECTORY / "jinja_templates"
