from pathlib import Path

import asyncio
import contextlib
import os
import shutil
import tempfile


def dodone():
    tasks = []

    def do(coro):
        tasks.append(asyncio.create_task(coro))

    async def done():
        nonlocal tasks
        results = await asyncio.gather(*tasks)
        tasks = []
        return results

    return (do, done)


@contextlib.contextmanager
def pushd(new_dir):
    os.makedirs(new_dir, exist_ok=True)
    previous_dir = os.getcwd()
    os.chdir(new_dir)
    yield
    os.chdir(previous_dir)


@contextlib.contextmanager
def tmpdir(**kwargs):
    td = tempfile.mkdtemp(**kwargs)
    yield Path(td)
    shutil.rmtree(td)


def _asynced(fun):
    async def f(sem, args):
        async with sem:
            await fun(*args)
    return f


async def async_starapply(fun, args_list, *, ntasks=0):
    if ntasks <= 0:
        ntasks = len(args_list)
    sem = asyncio.Semaphore(ntasks)
    tasks = []
    fun = _asynced(fun)

    for args in args_list:
        task = asyncio.create_task(fun(sem, args))
        tasks.append(task)

    await asyncio.gather(*tasks)


async def async_apply(fun, arg_list, *, ntasks=0):
    args_list = [(arg) for arg in arg_list]
    await async_starapply(fun, args_list, ntasks=ntasks)
