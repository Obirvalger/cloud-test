from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict

import asyncio
import logging
import os
import time

from cloud_test.utils import dodone
from cloud_test.image import Image
from cloud_test.config import config

import cloud_test.asyncvm as asyncvm


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TestFail:
    rc: int
    image: str


class TestImage:
    ti_sems: Dict[str, asyncio.Semaphore] = {}

    def __init__(self, ntests: int, image: Image, vm: asyncvm.AsyncVM):
        if self.__class__.ti_sems.get(vm.name) is None:
            if ntests <= 0:
                cpu_count = os.cpu_count()
                if cpu_count:
                    ntests = cpu_count
                else:
                    ntests = 4
            self.__class__.ti_sems[vm.name] = asyncio.Semaphore(ntests)

        self._sem = self.__class__.ti_sems[vm.name]
        self.image = image
        self.vm = vm

    async def _start(self):
        await self._sem.acquire()

    async def _stop(self):
        self._sem.release()

    def commands(self):
        pass

    async def test(self):
        await self._start()
        s = f'{self.image.name}-{self.image.branch}-{self.image.arch}'
        await self.vm.exec('echo', f'test {s}')
        # await self.vm.exec('sleep', '1')
        rc = await self.vm.exec(
            'docker',
            'run',
            '--rm',
            'alt',
            'echo',
            f'docker {s}',
        )
        await self._stop()
        if rc != 0:
            return TestFail(rc, self.image.path.name)


def images_by_arch_and_branch(images, arch, branch):
    return [image
            for image in images
            if image.arch == arch and image.branch == branch]


async def create_images_img(images_dir: Path):
    images_img = config.data_dir / 'images.img'
    images_img.unlink()

    du = await asyncio.create_subprocess_exec(
        'du',
        '-h',
        images_dir,
        stdout=asyncio.subprocess.PIPE,
    )

    out = (await du.communicate())[0]
    size = out.decode().split()[0]

    mkfs = await asyncio.create_subprocess_exec(
        'mkfs.ext4',
        '-d', images_dir,
        '-L', 'images_for_tests',
        images_img,
        size,
    )

    rc = await mkfs.wait()
    if rc != 0:
        raise Exception('Can not create images.img')

    return images_img


def vm_startup_actions() -> List[asyncvm.Action]:
    actions: List[asyncvm.Action] = []
    actions.append(asyncvm.Exec(
        'mkdir',
        '-p',
        config.images_mount_point,
    ))
    actions.append(asyncvm.Exec(
        'mount',
        '--options', 'ro',
        '--label', 'images_for_tests',
        config.images_mount_point,
    ))
    return actions


async def test_images(images: List[Image], images_dir: Path):
    branches = ['sisyphus']
    # branches = ['p9', 'sisyphus']
    arches = ['x86_64']
    # arches = ['x86_64', 'i586']
    nvms = 3
    ntests = 2

    # images_img = await create_images_img(images_dir)
    images_img = config.data_dir / 'images.img'

    start_t = time.monotonic()
    do_start, done_start = dodone()
    do_stop, done_stop = dodone()
    for branch in branches:
        for arch in arches:
            tester = config.tester_image(branch, arch)
            vm = asyncvm.AsyncVM(
                image=tester,
                mem='1024m',
                nvms=nvms,
                ssh_key=config.ssh_key,
                drives=[images_img],
            )
            do_start(vm.start(vm_startup_actions()))
            tasks = []
            for image in images_by_arch_and_branch(images, arch, branch):
                task = asyncio.create_task(TestImage(ntests, image, vm).test())
                tasks.append(task)
            do_stop(vm.stop(tasks))

    await done_start()
    vm_results = await done_stop()

    stop_t = time.monotonic()
    delta = stop_t - start_t
    print(f'Time: {delta}')
    errors = 0
    for test_results in vm_results:
        for result in test_results:
            if result is not None:
                errors += 1
                print(f'Failed tests for {result.image}')

    if errors:
        raise Exception(f'{errors} tests failed')
