from pathlib import Path
from typing import List, Optional

import asyncio
import logging
import os
import re
import socket

from cloud_test.config import config


logger = logging.getLogger(__name__)


class Action:
    async def apply(self, vm, **kwargs):
        raise NotImplementedError


class Exec(Action):
    def __init__(self, program, *prog_args, **exec_kwargs):
        self.program = program
        self.prog_args = prog_args
        self.exec_kwargs = exec_kwargs

    async def apply(self, vm, *, hidden=False, **kwargs):
        if hidden:
            method = '_exec'
        else:
            method = 'exec'

        fun = getattr(vm, method)
        result = await fun(self.program, *self.prog_args, **self.exec_kwargs)
        return result


class AsyncVM:
    vm_sem: Optional[asyncio.Semaphore] = None

    def __init__(self, image, mem, *, nvms=0, ssh_key=None, drives=None):
        if self.__class__.vm_sem is None:
            if nvms <= 0:
                nvms = os.cpu_count()
            self.__class__.vm_sem = asyncio.Semaphore(nvms)

        self.image = Path(image)

        if isinstance(mem, int):
            self.mem = str(mem)
        else:
            self.mem = mem

        if ssh_key is not None:
            self.ssh_key = Path(ssh_key)
        else:
            self.ssh_key = ssh_key
        if drives is None:
            drives = []
        self.drives = drives
        self.name = re.sub(r'\..*', '', image.name)
        self.socket = config.run_dir / f'{id(self)}-{self.name}.socket'
        self.running_event = asyncio.Event()
        self.events = []

    @property
    def _sem(self) -> asyncio.Semaphore:
        sem = self.__class__.vm_sem
        if sem is None:
            raise Exception(f'Not initialize semaphore in vm {self.name}')
        return sem

    def free_port(self):
        with socket.create_server(('localhost', 0)) as s:
            return s.getsockname()[1]

    async def wait_ssh(self):
        while not self.socket.exists():
            await asyncio.sleep(1)
        reader, self._writer = await asyncio.open_unix_connection(self.socket)
        line = await reader.readline()
        while not re.search(b'Started.*OpenSSH server daemon', line):
            line = await reader.readline()

    async def start(self, actions: List[Action] = None):
        if actions is None:
            actions = []
        await self._sem.acquire()
        self.port = self.free_port()
        kvm_args = [
            'kvm',
            '-m', self.mem,
            '-net', 'nic',
            '-net', f'user,hostfwd=tcp::{self.port}-:22',
            '-drive', f'file={self.image.as_posix()},if=virtio,format=qcow2',
            '-serial', f'unix:{self.socket},server',
            '-display', 'none',
        ]
        for drive in self.drives:
            if drive.as_posix().find('=') == -1:
                drive = f'file={drive},if=virtio,format=raw,' \
                         'force-share=on,read-only=on'
            kvm_args.extend(['-drive', drive])
        # print(" ".join(kvm_args))
        self.kvm = await asyncio.create_subprocess_exec(
            *kvm_args,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await self.wait_ssh()
        for action in actions:
            await action.apply(self, hidden=True)
        self.running_event.set()
        logger.info(f'Start vm {self.name}')
        return self

    async def __aenter__(self):
        await self.start()
        return self

    async def stop(self, tasks=None):
        if tasks is None:
            tasks = []
        await self.running_event.wait()
        results = await asyncio.gather(*tasks)
        for event in self.events:
            await event.wait()
        self._sem.release()
        self.running_event.clear()
        self.kvm.terminate()
        await self.kvm.wait()
        self._writer.close()
        await self._writer.wait_closed()
        logger.info(f'Stop vm {self.name}')
        return results

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()

    def _ssh(self):
        ssh = [
            'ssh',
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'UserKnownHostsFile=/dev/null',
            '-o', 'LogLevel=ERROR',
            'root@localhost',
            '-p', f'{self.port}',
        ]

        if self.ssh_key is not None:
            ssh.extend(['-i', self.ssh_key.as_posix()])

        return ssh

    async def _exec(self, program, *prog_args, **exec_kwargs):
        cmd = self._ssh() + [program] + list(prog_args)
        proc = await asyncio.create_subprocess_exec(*cmd, **exec_kwargs)
        rc = await proc.wait()
        return rc

    async def exec(self, program, *prog_args, **exec_kwargs):
        event = asyncio.Event()
        self.events.append(event)
        logger.debug(f'Begin `{program} {" ".join(prog_args)}` on {self.name}')
        await self.running_event.wait()
        rc = await self._exec(program, *prog_args, **exec_kwargs)
        logger.debug(f'End `{program} {" ".join(prog_args)}` on {self.name}')
        event.set()
        return rc
