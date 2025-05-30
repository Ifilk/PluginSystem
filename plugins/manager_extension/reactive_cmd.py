import contextlib
import difflib
import logging
import shlex
from collections import deque

from plugin_manager import ListenerManager, PluginListenerRegister

lr = PluginListenerRegister()

try:
    import msvcrt
    # Windows 实现
    def get_input(prompt):
        line = []
        print(prompt + ' ', end="", flush=True)
        while True:
            ch = msvcrt.getwch()
            if ch == '\r':  # Enter
                print()
                return ''.join(line)
            elif ch == '\x08':  # Backspace
                if line:
                    line.pop()
                    print('\b \b', end='', flush=True)
            elif ch == '\xe0':  # 特殊键
                ch = msvcrt.getwch()
                if ch == 'H':  # 上箭头
                    new_line = history.get_previous(''.join(line))
                    print('\r' + prompt + new_line + ' ' * (len(line) - len(new_line)), end='', flush=True)
                    line = list(new_line)
                elif ch == 'P':  # 下箭头
                    new_line = history.get_next(''.join(line))
                    print('\r' + prompt + new_line + ' ' * (len(line) - len(new_line)), end='', flush=True)
                    line = list(new_line)
            elif ch.isprintable():
                line.append(ch)
                print(ch, end='', flush=True)
except ImportError:
    import sys, tty, termios

    # Linux/Mac 实现
    def get_input(prompt):
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        print(prompt + ' ', end="", flush=True)
        try:
            tty.setraw(fd)
            line = []
            while True:
                ch = sys.stdin.read(1)
                if ch == '\r' or ch == '\n':  # Enter
                    print()
                    return ''.join(line)
                elif ch == '\x7f':  # Backspace
                    if line:
                        line.pop()
                        print('\b \b', end='', flush=True)
                elif ch == '\x1b':  # ESC
                    ch = sys.stdin.read(1)
                    if ch == '[':
                        ch = sys.stdin.read(1)
                        if ch == 'A':  # 上箭头
                            new_line = history.get_previous(''.join(line))
                            print('\r' + prompt + new_line + ' ' * (len(line) - len(new_line)), end='', flush=True)
                            line = list(new_line)
                        elif ch == 'B':  # 下箭头
                            new_line = history.get_next(''.join(line))
                            print('\r' + prompt + new_line + ' ' * (len(line) - len(new_line)), end='', flush=True)
                            line = list(new_line)
                elif ch.isprintable():
                    line.append(ch)
                    print(ch, end='', flush=True)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
            return None

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
_LISTENER = None

@lr('exit', single=True)
def _exit():
    exit(0)

@lr('echo', single=True)
def stout_output(_, msg: str):
    print(msg)

@lr('client_only_init', single=True)
def client_only_init(_, lm: ListenerManager):
    global _LISTENER
    _LISTENER = lm

@lr('reactive_cmd', single=True)
def reactive_cmd(ctx):
    while True:
        try:
            raw = get_input(">>> ")

            with contextlib.suppress(Exception):
                ws = shlex.split(_LISTENER.rewrite_cmd(raw))

            func = ctx.get_trigger(ws[0])
            if func:
                (func(*([a if a != 'this' else _LISTENER for a in ws[1:]])
                if len(ws) > 0 else []))

            history.add(raw)

        except KeyboardInterrupt:
            print("\nUse 'exit' to quit")
        except EOFError:
            exit(0)

@lr('plugins', single=True, help='Print all of plugins')
def plugin_list(ctx):
    index = 0
    ctx.echo(f"{'No.':<5} {'Plugin Name':<20} {'Version':<10}")
    ctx.echo("-" * 40)
    for plugin_name, plugin_version in _LISTENER.plugin_list:
        index += 1
        ctx.echo(f"{index:<5} {plugin_name:<20} {plugin_version:<10}")


@lr('list', single=True, help='Print all of triggers')
def trigger_list(ctx):
    for trigger in _LISTENER.trigger_list:
        ctx.echo(f"{trigger}")


@lr('?', single=True, help='Get help of a trigger\n? <trigger_name: str>')
def trigger_list(ctx, trigger_name):
    try:
        ctx.echo(_LISTENER.trigger_help[trigger_name])
    except KeyError:
        available_triggers = _LISTENER.trigger_help.keys()
        matches = difflib.get_close_matches(trigger_name, available_triggers, n=1, cutoff=0.6)

        if matches:
            suggestion = matches[0]
            ctx.echo(f"Unknown signal: {trigger_name}, do you mean '{suggestion}'?")
        else:
            ctx.echo(f"Unknown signal: {trigger_name}")

@lr('unload', single=True, help='unload <plugin_name: str>')
def unload(ctx, plugin_name: str):
    if _LISTENER.unload_plugin(plugin_name) > 0:
        _LISTENER.reload()
        ctx.echo(f'Unloaded {plugin_name}')