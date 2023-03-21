from jinja2 import Environment, FileSystemLoader, PackageLoader
from pathlib import Path
import os
from typing import Dict
import re
import pathlib


class SQLTemplate:

    _templatePath = os.path.join(
        os.path.dirname(os.path.dirname(os.path.relpath(__file__))), "templates"
    )

    _templatePath = os.path.join("templates")
    # raise ValueError(f'templatePath = {_templatePath}')

    def getTemplate(self, sqlAction: str, parameters: Dict) -> str:
        templateName = f"{sqlAction.lower().strip()}.j2"
        templateEnv = Environment(
            loader=PackageLoader(package_name="tips.framework",package_path="templates"), trim_blocks=True
            # loader=FileSystemLoader(self._templatePath), trim_blocks=True
        )
        cmd = (
            templateEnv.get_template(templateName)
            .render(parameters=parameters)
            .strip()
            .replace("\n", " ")
        )
        return re.sub("  +", " ", cmd)
