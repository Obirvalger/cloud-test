from dataclasses import dataclass
from pathlib import Path

import re


@dataclass(frozen=True)
class Image:
    name: str
    path: Path
    arch: str
    branch: str
    kind: str


def image_from_path_re(path: Path, regex: re.Pattern) -> Image:
    m = re.match(regex, path.name)
    if not m:
        raise Exception(f'Bad image {path=} for {regex=}')

    image = Image(
        m.group('name'),
        path,
        m.group('arch'),
        m.group('branch'),
        m.group('kind'),
    )

    return image
