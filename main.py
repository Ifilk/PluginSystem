import argparse
import gzip
import logging
import os
import shlex
import shutil
from logging.handlers import TimedRotatingFileHandler
from typing import List

from configuration import ConfigManager
from folder_paths import PathManager
from plugin_manager import ListenerManager, VERSION

logger = logging.getLogger('main_logger')

arg = argparse.ArgumentParser()
arg.add_argument('--v', '--verbose', action='store_true')
arg.add_argument('-e', '-emit', type=str, action='append', default=[])
arg.add_argument('-p', '-plugin_dir', type=str, default='plugins')
arg.add_argument('-c', '-config_dir', type=str, default='configs')
arg.add_argument('-log', type=str, default='main.log')
arg.add_argument('--default_config_format', type=str, default='toml')
arg.add_argument('--enable_async', action='store_true')
arg.add_argument('-max_concurrent', type=int)
arg.add_argument('--dry_run', action='store_true')
arg.add_argument('--version', action='store_true')

args = arg.parse_args()


def compress_log(source, dest):
    with open(source, 'rb') as f_in, gzip.open(dest, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(source)

def init_logger(
    logger_names: List[str],
    log_dir: str,
    log_file: str,
    level: int = logging.INFO,
    backup_count: int = 7,
    when: str = "midnight"
):
    os.makedirs(log_dir, exist_ok=True)

    formatter = logging.Formatter(
        fmt='[%(asctime)s.%(msecs)03d] %(filename)s -> %(funcName)s line:%(lineno)d [%(levelname)s] : %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 普通文件 handler（用于立即写入）
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # 自动轮转并压缩 handler
    zip_handler = TimedRotatingFileHandler(
        filename=log_file, when=when, backupCount=backup_count, encoding='utf-8'
    )
    zip_handler.setLevel(logging.DEBUG)
    zip_handler.setFormatter(formatter)
    zip_handler.namer = lambda _name: _name + ".gz"
    zip_handler.rotator = compress_log

    # 添加 handler 到所有 logger
    for name in logger_names:
        logger = logging.getLogger(name)
        logger.setLevel(level)
        if not logger.handlers:
            logger.addHandler(file_handler)
            logger.addHandler(zip_handler)

if __name__ in {"__main__", "__mp_main__"}:
    if args.version:
        print(f"Plugin Manager version {VERSION}")
        exit(0)
    log_level = logging.DEBUG if args.v else logging.INFO
    init_logger(
        logger_names=["main_logger", "plugin_manager_listeners_exec"],
        log_file=args.log,
        log_dir='logs',
        level=log_level
    )

    PathManager(os.path.dirname(os.path.realpath(__file__)), {
        'plugins': args.p,
        'configs': args.c
    })
    PathManager().ensure_folders_exist()
    folder_path = PathManager().dir('plugins')

    listener_manager = ListenerManager(_async=args.enable_async,
                                       max_concurrent=args.max_concurrent)

    cm = ConfigManager(PathManager().dir('configs'))
    cm.auto_config(listener_manager, args.default_config_format)
    listener_manager.load_plugins(folder_path)
    listener_manager.load()
    ctx = listener_manager.ctx

    if args.dry_run:
        result = ''
        for trigger in listener_manager.trigger_list:
            result += trigger
            result += '\n'
        print(result[:-1])
        exit(0)

    for e in args.e:
        try:
            _signal_cmd = shlex.split(e)
            trigger_name = _signal_cmd[0]
            args_list = [a if a != '#' else None for a in _signal_cmd[1:]]
            func = ctx.get_trigger(trigger_name)
            if func:
                logger.info(f"Executing trigger '{trigger_name}' with args: {args_list}")
                func(*args_list)
            else:
                logger.warning(f"Trigger '{trigger_name}' not found.")
        except Exception as ex:
            logger.exception(f"Error executing trigger '{e}': {ex}")
