from pathlib import Path

import logging
import os

import yaml


class Config():
    def __init__(self):
        self.name = 'cloud-test'
        config_path = Path(os.path.expanduser(os.getenv('XDG_CONFIG_HOME',
                                                        '~/.config'))
                           + '/cloud-test/config.yaml')

        try:
            with open(config_path) as f:
                cfg = yaml.safe_load(f)
        except OSError as e:
            msg = f'Could not read config file `{e.filename}`: {e.strerror}'
            raise Exception(msg)

        self.ssh_key = cfg.get('ssh_key')

        self.run_dir = Path(os.getenv('XDG_RUNTIME_DIR')) / self.name
        self.run_dir.mkdir(exist_ok=True)
        self.data_dir = (Path(os.path.expanduser(os.getenv('XDG_DATA_HOME',
                                                           '~/.local/share')))
                         / self.name)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.images_mount_point = '/media/images'

        logging.basicConfig(
            filename=f'{self.data_dir}/{self.name}.log',
            style='{',
            datefmt='%Y-%m-%d %H:%M:%S',
            format='{name} {levelname} {asctime} - {message}',
            level=logging.DEBUG,
        )

    def tester_image(self, branch, arch):
        branch = branch.lower()
        return self.data_dir / f'images/alt-{branch}-tester-{arch}.qcow2'


config = Config()
