import difflib
import logging
import shlex
import threading
import sys
from collections import deque

from plugin_manager import PluginListenerRegister
from . import patch_logger_with_color

lr = PluginListenerRegister()

_logger = logging.getLogger('main_logger')


class SimpleHistory:
    def __init__(self, maxlen=100):
        self.history = deque(maxlen=maxlen)
        self.history_pos = -1  # -1 表示不在历史记录浏览模式
        self.current_input = ""

    def add(self, cmd):
        if cmd and (not self.history or self.history[0] != cmd):
            self.history.appendleft(cmd)

    def get_previous(self, current):
        if self.history_pos == -1:
            self.current_input = current  # 保存当前输入
        if self.history_pos < len(self.history) - 1:
            self.history_pos += 1
            return self.history[self.history_pos]
        return current

    def get_next(self, current):
        if self.history_pos > 0:
            self.history_pos -= 1
            return self.history[self.history_pos]
        elif self.history_pos == 0:
            self.history_pos = -1
            return self.current_input
        return current


history = SimpleHistory()
print_lock = threading.Lock()
PROMPT = ">>> "

@lr('echo', single=True)
async def stout_output(_, *msg: str):
    with print_lock:
        sys.stdout.write("\r")
        sys.stdout.write(" " * 80)
        sys.stdout.write("\r")
        print(' '.join(msg), flush=True)
        sys.stdout.write(PROMPT)
        sys.stdout.flush()

logger = logging.getLogger('main_logger')
@lr('_info', single=True)
def logger_info(_, msg: str):
    logger.info(msg)

@lr('_on_input', single=True)
def _on_input(_, raw_input: str):
    return raw_input

@lr('_on_handle', single=True)
async def _on_handle(ctx, signal: str, parameters):
    trigger = ctx.get_trigger(signal)
    if trigger is None:
        return
    if parameters is None:
        response = trigger()
    elif isinstance(parameters, str):
        response = trigger(parameters)
    else:
        response = trigger(*parameters)
    if response is not None:
        for e in response.values():
            if e is not None:
                ctx.echo(e)

@lr('cmd', single=True)
def reactive_cmd(ctx, color='True'):
    if bool(color):
        patch_logger_with_color('main_logger',
                                '[%(asctime)s.%(msecs)03d] %(filename)s -> %(funcName)s line:%(lineno)d [%(levelname)s] : %(message)s'
                                )
        patch_logger_with_color('plugin_manager_listeners_exec',
                                '[%(asctime)s.%(msecs)03d] Trigger("%(trigger_name)s") -> %(listener_id)s [%(levelname)s] : %(message)s'
                                )
    while True:
        try:
            raw = input(PROMPT)
            if raw == 'exit':
                exit(0)
            raw = ctx._on_input(raw)[_on_input]
            ws = shlex.split(raw)
            if len(ws) > 0:
                ctx._on_handle(ws[0], (ws[1:]) if len(ws) > 1 else None)
                history.add(raw)
        except KeyboardInterrupt:
            print("\nUse 'exit' to quit")
        except EOFError:
            exit(0)

@lr('plugin_list', single=True)
def plugin_list(ctx):
    index = 0
    result = ''
    result += (f"{'No.':<5} {'Plugin Name':<20} {'Version':<10}")
    result += '\n'
    result += ("-" * 40)
    result += '\n'
    for plugin_name, plugin_version in ctx.ctx.plugin_list:
        index += 1
        result += (f"{index:<5} {plugin_name:<20} {plugin_version:<10}")
        result += '\n'
    return result[:-1]


@lr('list', single=True)
def trigger_list(ctx):
    result = ''
    for trigger in ctx.ctx.trigger_list:
        if not trigger.startswith('_'):
            result += trigger
            result += '\n'
    return result[:-1]


@lr('?', single=True)
def trigger_list(ctx, trigger_name):
    result = ''
    try:
        doc = ctx.get_trigger(trigger_name).__doc__
        result += doc if doc else ''
        result += '\n'
    except KeyError:
        available_triggers = ctx._inner_trigger_dict.keys()
        matches = difflib.get_close_matches(trigger_name, available_triggers, n=1, cutoff=0.6)

        if matches:
            suggestion = matches[0]
            result += f"Unknown signal: {trigger_name}, do you mean '{suggestion}'?"
            result += '\n'
        else:
            result += f"Unknown signal: {trigger_name}"
            result += '\n'
    return result[:-1]

@lr('unload', single=True)
def unload(ctx, plugin_name: str):
    result = ''
    if ctx.listener_manager.unload_plugin(plugin_name) > 0:
        ctx.listener_manager.reload()
        result += (f'Unloaded {plugin_name}')
        result += '\n'
    return result[:-1]