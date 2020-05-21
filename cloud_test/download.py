from pathlib import Path
from typing import List

import asyncio
import hashlib
import logging
import multiprocessing
import os
import re
import urllib.parse

from cloud_test.utils import tmpdir


logger = logging.getLogger(__name__)


async def download_rsync(url: str, download_dir: Path):
    rsync = await asyncio.create_subprocess_exec(
        'rsync',
        '--quiet',
        '--archive',
        url + '/',
        download_dir,
    )
    await rsync.wait()


async def download_wget(url: str, download_dir: Path):
    wget = await asyncio.create_subprocess_exec(
        'wget',
        '--quiet',
        f'--directory-prefix={download_dir}',
        '--no-directories',
        '--no-host-directories',
        '--recursive',
        '--no-parent',
        '--reject=index.html*',
        url,
    )
    await wget.wait()


async def download_link(url: str, download_dir: Path):
    src = Path(urllib.parse.urlsplit(url).path)
    for f in src.iterdir():
        dst_file = download_dir / f.name
        os.link(src / f, dst_file)


async def download_files(url: str, download_dir: Path):
    scheme = urllib.parse.urlsplit(url).scheme
    if scheme == 'http' or scheme == 'https':
        await download_wget(url, download_dir)
    elif scheme == 'rsync':
        await download_rsync(url, download_dir)
    elif scheme == '' or scheme == 'file':
        await download_link(url, download_dir)


async def check_gpg(path: Path):
    gpg = await asyncio.create_subprocess_exec(
        'gpg2',
        '--verify',
        path,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    rc = await gpg.wait()
    if rc != 0:
        raise Exception(f'Bad signature in {path}')


def sha(sha_str: str, basedir: Path):
    old_digest, name = sha_str.split()
    full_path = basedir / name
    new_digest = hashlib.sha256(full_path.read_bytes()).hexdigest()
    if old_digest != new_digest:
        raise Exception(f'Invalid checksums for {name}')
    else:
        print(f'{name}: OK')


def check_sha(sum_path: Path):
    basedir = sum_path.parent
    lines = sum_path.read_text().splitlines()
    args = [(line, basedir) for line in lines]
    with multiprocessing.Pool() as pool:
        pool.starmap(sha, args)


async def check_files(directory: Path):
    await check_gpg(directory / 'SHA256SUM.asc')
    check_sha(directory / 'SHA256SUM')


def move_images(src_dir: Path, dst_dir: Path, image_re):
    for image in src_dir.iterdir():
        if re.match(image_re, image.name):
            os.renames(image, dst_dir / image.name)
        else:
            continue


async def process_files(
    url: str,
    /,  # noqa E225
    images_dir: Path,
    image_re: re.Pattern,
    basedir: Path,
    *,
    no_check: bool = False,
):
    with tmpdir(prefix='url_dir-', dir=basedir) as td:
        await download_files(url, td)
        if not no_check:
            await check_files(td)
        move_images(td, images_dir, image_re)


async def download_images(
    urls: List[str],
    images_dir: Path,
    image_re: re.Pattern,
    *,
    no_check: bool = False,
):
    cache_dir = images_dir.parent

    with tmpdir(prefix='work_dir-', dir=cache_dir) as td:
        kwargs = {
            'images_dir': images_dir,
            'image_re': image_re,
            'basedir': td,
            'no_check': no_check,
        }
        tasks = []
        for url in urls:
            tasks.append(asyncio.create_task(process_files(url, **kwargs)))

        await asyncio.gather(*tasks)
