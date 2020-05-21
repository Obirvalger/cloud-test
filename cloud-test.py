#!/usr/bin/python3

from pathlib import Path

import argparse
import asyncio
import logging
import os
import re
import shutil

from cloud_test import download_images, test_images, image_from_path_re


def parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '-u',
        '--urls',
        nargs='+',
        help='urls with images',
    )
    parser.add_argument(
        '--no-check',
        action='store_true',
        help='does not check downloaded files',
    )
    args = parser.parse_args()

    return args


async def main():
    args = parse_args()
    urls = args.urls
    images_dir = Path(os.path.expanduser(os.getenv('XDG_CACHE_HOME',
                                                   '~/.cache'))
                      + '/cloud-test/images')
    image_re = re.compile(r'alt-(?P<branch>[Ssp]\w+)-(?P<name>[-\w]+)-'
                          r'(?P<arch>\w+)\.(?P<kind>[a-z][.\w]*)')

    shutil.rmtree(images_dir)
    os.makedirs(images_dir)
    await download_images(urls, images_dir, image_re, no_check=args.no_check)
    images = [image_from_path_re(path, image_re)
              for path in images_dir.iterdir()]
    await test_images(images, images_dir)

logging.getLogger("asyncio").setLevel(logging.DEBUG)
# logging.getLogger("asyncio").setLevel(logging.WARNING)
asyncio.run(main(), debug=True)
# asyncio.run(main())
