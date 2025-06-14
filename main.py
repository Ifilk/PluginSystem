import argparse
import logging
import os
import shlex

from plugin_manager import ListenerManager

logger = logging.getLogger('main_logger')

LISTENER = ListenerManager()

arg = argparse.ArgumentParser()
arg.add_argument('--v', '--verbose', action='store_true')
arg.add_argument('-e', '-emit', type=str, action='append', default=[])
arg.add_argument('-p', '-plugin_dir', type=str, default='plugins')
arg.add_argument('-log', type=str, default='main.log')

args = arg.parse_args()

if __name__ in {"__main__", "__mp_main__"}:
    _logger_listeners_exec = logging.getLogger('plugin_manager_listeners_exec')
    if not args.v:
        logger.setLevel(logging.INFO)
        _logger_listeners_exec.setLevel(logging.INFO)

    file_handler = logging.FileHandler(filename=args.log, mode='a', encoding='utf8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        fmt='[%(asctime)s.%(msecs)03d] %(filename)s -> %(funcName)s line:%(lineno)d [%(levelname)s] : %(message)s',
        datefmt='%Y-%m-%d  %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    _logger_listeners_exec.addHandler(file_handler)
    file_handler.close()

    if not logger.handlers:
        logger.addHandler(file_handler)

    folder_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), args.p)

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    LISTENER.load_plugins(folder_path)
    ctx = LISTENER.load()

    for e in args.e:
        _signal_cmd = shlex.split(e)
        func = ctx.get_trigger(_signal_cmd[0])
        if func:
            (func(*([a if a != '#' else None for a in _signal_cmd[1:]])
            if len(_signal_cmd) > 0 else []))
