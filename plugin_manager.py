import asyncio
import importlib.util
import logging
import os
import re
import sys
import threading
import time
import traceback
import difflib
from dataclasses import dataclass
from enum import Enum
from queue import Queue
from inspect import signature
import colorlog
import nest_asyncio
from typing_extensions import Callable, Any, Dict, List, Union

VERSION = '1.1.0'

_empty_function = lambda *args, **kwargs: None

log_colors_config = {
    'DEBUG': 'white',  # cyan white
    'INFO': 'green',
    'WARNING': 'yellow',
    'ERROR': 'red',
    'CRITICAL': 'bold_red',
}

logger = logging.getLogger('main_logger')
console_handler = logging.StreamHandler()

logger.setLevel(logging.DEBUG)
console_handler.setLevel(logging.DEBUG)

console_formatter = colorlog.ColoredFormatter(
    fmt='%(log_color)s[%(asctime)s.%(msecs)03d] %(filename)s -> %(funcName)s line:%(lineno)d [%(levelname)s] : %(message)s',
    datefmt='%Y-%m-%d  %H:%M:%S',
    log_colors=log_colors_config
)
console_handler.setFormatter(console_formatter)

if not logger.handlers:
    logger.addHandler(console_handler)

console_handler.close()

handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s[%(asctime)s.%(msecs)03d] Trigger("%(trigger_name)s") '
    '-> %(listener_id)s [%(levelname)s] : %(message)s',
    datefmt='%Y-%m-%d  %H:%M:%S',
))

_logger_listeners_exec = logging.getLogger('plugin_manager_listeners_exec')
_logger_listeners_exec.addHandler(handler)
_logger_listeners_exec.setLevel(logging.DEBUG)

_logger = logging.getLogger('main_logger')

@dataclass
class Listener:
    listener_id: str
    order: int
    func: Callable[..., Any]
    single: bool

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


class PluginListenerRegister:

    def __init__(self):
        self.listener_dict: Dict[str, List[Listener]] = {}
        self.trigger_help: Dict[str, str] = {}
        self.plugin_name = ''
        self.plugin_version = ''

    def __add__(self, other):
        return self.merge(other)

    def merge(self, other):
        self.listener_dict.update(other.listener_dict)
        self.trigger_help.update(other.trigger_help)
        if self.plugin_name != other.plugin_name or self.plugin_version != other.plugin_version:
            raise ValueError("Inconsistent 'plugin_name' and 'plugin_version'")
        return self

    def generate_id(self, plugin_name, plugin_version):
        self.plugin_name = plugin_name
        self.plugin_version = plugin_version

    def register_listener(self, trigger_name: str, listener_name: str, func, order, single, help):
        # self.register_trigger(trigger_name)
        self.trigger_help[trigger_name] = help
        self.listener_dict.setdefault(trigger_name, []).append(Listener(
            listener_id=f'-{trigger_name}-{listener_name}',
            order=order,
            func=func,
            single=single,
        ))

    def register_trigger(self, trigger_name: str):
        if trigger_name not in self.listener_dict.keys():
            self.listener_dict[trigger_name] = []

    def apply_listener(self, trigger_name: str, listener_name: str, order=0, single=False):
        def listener(func):
            _logger.debug(f'listener {trigger_name}-{listener_name} register')
            self.register_listener(trigger_name, listener_name, func, order, single)

        return listener

    def for_trigger(self, trigger_name, single=False, help=''):
        def apply_listener(listener_name=None, order=0, single=single):
            # 判断是 @apply_listener 还是 @apply_listener(...) 调用
            def decorator(func: Callable[..., Union[ListenerManagerResponseOperationCode, ListenerManagerResponse]]):
                # 如果 listener_name 为 None，使用函数名
                actual_listener_name = listener_name if listener_name is not None else func.__name__
                _logger.debug(f'listener {trigger_name}-{actual_listener_name} registered')
                self.register_listener(trigger_name, actual_listener_name, func, order, single, help)
                return func

            # 如果第一个参数是可调用的，说明是 @apply_listener 形式
            if callable(listener_name):
                func = listener_name
                # 使用默认参数
                actual_listener_name = func.__name__
                _logger.debug(f'listener {trigger_name}-{actual_listener_name} registered')
                self.register_listener(trigger_name, actual_listener_name, func, 0, single, help)
                return func
            else:
                # 是 @apply_listener(...) 形式，返回装饰器
                return decorator

        return apply_listener

    def __call__(self, trigger_name, single=False, help=''):
        return self.for_trigger(trigger_name, single, help)


class AsyncFunctionPool:
    def __init__(self, interval=.1):
        self.loop = asyncio.get_event_loop()
        self.task_queue = Queue()
        self.interval = interval
        self.running = True
        self.timer_thread = threading.Thread(target=self._run_scheduled_tasks)
        self.timer_thread.setDaemon(True)
        self.timer_thread.start()
        self.cache = Queue()

    def __len__(self):
        return self.cache.qsize()

    def push_in_cache(self, coro):
        self.cache.put(coro)

    def flash(self):
        while not self.cache.empty():
            self.task_queue.put(self.cache.get())

    def add_task(self, coro):
        self.task_queue.put(coro)

    def _execute_task(self):
        try:
            if not self.task_queue.empty():
                coro = self.task_queue.get()
                self.loop.run_until_complete(coro)
        except Exception:
            _logger_listeners_exec.error(
                traceback.format_exc(), extra={'trigger_name': 'unknown',
                                               'listener_id': 'unknown'})

    def _run_scheduled_tasks(self):
        while self.running:
            self._execute_task()
            time.sleep(self.interval)

    def shutdown(self):
        self.running = False
        self.timer_thread.join()


class ListenerManagerResponseOperationCode(Enum):
    INTERRUPT = 0
    SKIP = 1

    EXIT_FOR_FATAL_ERROR = 2


INTERRUPT = ListenerManagerResponseOperationCode.INTERRUPT
EXIT_FOR_FATAL_ERROR = ListenerManagerResponseOperationCode.EXIT_FOR_FATAL_ERROR


class FatalException(Exception):
    pass

class SignalEmitException(Exception):
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg

    def __str__(self):
        return self.msg


class ListenerManagerResponse:
    operation: ListenerManagerResponseOperationCode
    param: object


class ListenerManager:
    def __init__(self, async_listener=True, verbose=True):
        self.ctx = None
        self.trigger_dict = {}
        self.trigger_list = set([])
        self.listener_dict: Dict[str, List[Listener]] = {}
        self.trigger_help = {}
        self.plugin_applied_register = []
        self.plugin_list = []
        self.async_listener = async_listener
        self.verbose = verbose
        if async_listener:
            _logger.info('Async ListenerManager is enabled')
            self.async_pool = AsyncFunctionPool()
        else:
            self.async_pool = None

    def apply_plugin_listener_register(self, plugin_listener_register: PluginListenerRegister):
        plugin = f'{plugin_listener_register.plugin_name}-{plugin_listener_register.plugin_version}'
        if plugin in self.listener_dict.keys():
            raise Exception(f'Plugin {plugin} already registered')

        self.plugin_applied_register.append(plugin)
        for k in plugin_listener_register.listener_dict.keys():
            fs = plugin_listener_register.listener_dict[k]
            for l in fs:
                l.listener_id = plugin + l.listener_id
                _logger.debug(f'listener "{l.listener_id}" applied')

        self.merge_dicts(plugin_listener_register.listener_dict)
        self.trigger_list.update(plugin_listener_register.listener_dict.keys())
        self.trigger_help.update(plugin_listener_register.trigger_help)

    def merge_dicts(self, dict2):
        merged_dict = self.listener_dict
        for key, value in dict2.items():
            if key in merged_dict:
                merged_dict[key].extend(value)
            else:
                merged_dict[key] = value


    def unload_plugin(self, plugin_name, plugin_version='*'):
        plugin_prefix = f'{plugin_name}-'

        if plugin_version != '*':
            plugin = f'{plugin_name}-{plugin_version}-'
            plugin_prefix = plugin

        removed_plugins = [
            p for p in self.plugin_applied_register
            if p.startswith(plugin_prefix) and
               (plugin_version == '*' or p.startswith(plugin))
        ]
        self.plugin_applied_register = [
            p for p in self.plugin_applied_register
            if p not in removed_plugins
        ]

        removed_count = 0
        for event_name in list(self.listener_dict.keys()):
            listeners = self.listener_dict[event_name]

            remaining_listeners = [
                lst for lst in listeners
                if not (lst.listener_id.startswith(plugin_prefix) and
                        (plugin_version == '*' or lst.listener_id.startswith(plugin)))
            ]

            for lst in listeners:
                if (lst.listener_id.startswith(plugin_prefix) and
                        (plugin_version == '*' or lst.listener_id.startswith(plugin))):
                    _logger.debug(f'listener "{lst.listener_id}" unloaded')
                    removed_count += 1

            if len(remaining_listeners) == 0:
                del self.listener_dict[event_name]
                if event_name in self.trigger_list:
                    self.trigger_list.remove(event_name)
                _logger.debug(f'Event "{event_name}" removed as it has no more listeners')
            else:
                self.listener_dict[event_name] = remaining_listeners

        _logger.info(f'Unloaded {len(removed_plugins)} plugin(s) and {removed_count} listener(s)')
        return removed_count > 0 or len(removed_plugins) > 0

    def load(self) -> 'SignalContext':
        nest_asyncio.apply()
        return self.reload()

    def reload(self) -> 'SignalContext':
        self.trigger_dict = {trigger: self.box_func_list(sorted(self.listener_dict[trigger], key=lambda x: -x.order),
                                                          trigger)
                              for trigger in self.trigger_list}

        common_keys = self.trigger_list & set(self.__dict__.keys())
        if len(common_keys) > 0:
            _logger.error(f'Illegal signals: {list(common_keys)}')
            raise KeyError(f'Illegal signals: {list(common_keys)}')
        self.ctx = SignalContext(self)
        return self.ctx

    def modify_listener(self, listener_id: str, listener_func, order=None):
        trigger = listener_id.split('-')[2]
        for listener in self.listener_dict[trigger]:
            if listener.listener_id == listener_id:
                listener.order = order or listener.order
                listener.func = listener_func
        self.trigger_dict[trigger] = self.box_func_list(
            sorted(self.listener_dict[trigger], key=lambda x: -x.order),
            trigger)
        self.ctx = SignalContext(self)

    def get_all_listeners(self):
        listeners = []
        for k in self.listener_dict.keys():
            fs = self.listener_dict[k]
            for i in range(len(fs)):
                listeners.append(fs[i].listener_id)
        return listeners

    def find_listener(self, listener_id: str, multiple=False):
        trigger_name = listener_id.split('-')[2]
        regex_pattern = f"^{listener_id.replace('*', '.*')}$"

        result = []
        if trigger_name == '*':
            for _, ls_list in self.listener_dict.items():
                for ls in ls_list:
                    if re.match(regex_pattern, ls.listener_id):
                        result.append(ls)
                        if not multiple:
                            return ls
        else:
            for ls in self.listener_dict.get(trigger_name, []):
                if re.match(regex_pattern, ls.listener_id):
                    result.append(ls)
                    if not multiple:
                        return ls

        return result if multiple else None

    def rewrite_cmd(self, c) -> str:
        return c

    def __getattr__(self, item):
        try:
            return self.__dict__[item]
        except KeyError:
            try:
                return self.trigger_dict[item]
            except KeyError:
                available_triggers = self.trigger_help.keys()
                matches = difflib.get_close_matches(item, available_triggers, n=1, cutoff=0.6)

                if matches:
                    suggestion = matches[0]
                    _logger.error(f"Unknown signal: {item}, do you mean '{suggestion}'?")
                else:
                    _logger.error(f"Unknown signal: {item}")
                return None

    def load_plugins(self, plugin_fold_path):
        sys.path.append(plugin_fold_path)
        import_times = []
        l = os.path.realpath(plugin_fold_path)
        possible_modules = os.listdir(l)
        if "__pycache__" in possible_modules:
            possible_modules.remove("__pycache__")

        for possible_module in possible_modules:
            module_path = os.path.join(plugin_fold_path, possible_module)
            if os.path.isfile(module_path) and os.path.splitext(module_path)[1] != ".py":
                continue
            if module_path.endswith(".disabled"):
                continue
            time_before = time.perf_counter()
            success = self.load_plugin(possible_module, plugin_fold_path)
            import_times.append((time.perf_counter() - time_before, module_path, success))

        if len(import_times) > 0:
            for n in sorted(import_times):
                if n[2]:
                    import_message = ""
                else:
                    import_message = " (IMPORT FAILED)"
                _logger.info("{:6.3f} seconds{}: {}".format(n[0], import_message, n[1]))

    def load_plugin(self, possible_module, workspace_path):
        module_name = os.path.splitext(possible_module)[0]
        module_path = os.path.join(workspace_path, possible_module)
        try:
            _logger.debug("Trying to load plugins {}".format(module_path))
            if os.path.isfile(module_path):
                module_spec = importlib.util.spec_from_file_location(module_name, module_path)
            else:
                module_spec = importlib.util.spec_from_file_location(module_name,
                                                                     os.path.join(module_path, "__init__.py"))

            module = importlib.util.module_from_spec(module_spec)
            module_spec.loader.exec_module(module)

            plugin_name = getattr(module, 'PLUGIN_NAME', module.__package__) or module.__name__
            plugin_version = getattr(module, 'PLUGIN_VERSION', 'null')
            if (plugin_name, plugin_version) in self.plugin_list:
                raise Exception(f'Plugin {plugin_name}-{plugin_version} already registered')
            self.plugin_list.append((plugin_name, plugin_version))

            if hasattr(module, 'LISTENER_REGISTER'):
                module.LISTENER_REGISTER.generate_id(plugin_name, plugin_version)
                self.apply_plugin_listener_register(module.LISTENER_REGISTER)
            else:
                if module.__package__.startswith('lib'):
                    return True
                possible = [v for v in module.__dict__.values() if isinstance(v, PluginListenerRegister)]
                for plugin in possible:
                    plugin.generate_id(plugin_name, plugin_version)
                    self.apply_plugin_listener_register(plugin)
            return True
        except Exception as e:
            _logger.warning(traceback.format_exc())
            _logger.warning(f"Cannot import {module_path} module for custom nodes: {e}")
            return False

    def box_func_list(self, ls_list: List[Listener], trigger_name):
        loop = asyncio.get_event_loop()
        verbose = self.verbose
        async_pool = self.async_pool
        async_listener = self.async_listener

        def listeners_exec(*args, **kwargs):
            exception = []
            skip_list = []
            result_dict = {}
            async_task_list = []
            for ls in ls_list:
                try:
                    if ls.listener_id not in skip_list:
                        result = safe_execute_listener(ls, self.ctx, args, kwargs)
                        if ls.single or result == ListenerManagerResponseOperationCode.INTERRUPT:
                            _logger_listeners_exec.debug('Interrupt polling', extra={'trigger_name': trigger_name,
                                                                                     'listener_id': ls.listener_id})
                            break
                        elif result is None:
                            continue
                        elif result == ListenerManagerResponseOperationCode.EXIT_FOR_FATAL_ERROR:
                            raise FatalException('Manual Exit')
                        elif isinstance(result, ListenerManagerResponse):
                            if result.operation == ListenerManagerResponseOperationCode.INTERRUPT:
                                _logger_listeners_exec.debug('Interrupt polling',
                                                             extra={'trigger_name': trigger_name,
                                                                    'listener_id': ls.listener_id})
                                result_dict[ls.listener_id] = result.param
                                break
                            elif result.operation == ListenerManagerResponseOperationCode.EXIT_FOR_FATAL_ERROR:
                                raise FatalException(str(result.param))
                            elif result.operation == ListenerManagerResponseOperationCode.SKIP:
                                if isinstance(result.param, str):
                                    skip_list.append(result.param)
                                elif isinstance(result.param, list):
                                    skip_list.extend(result.param)
                        elif asyncio.iscoroutine(result):
                            if async_listener:
                                async_pool.push_in_cache(result)
                            else:
                                async_task_list.append(result)
                        else:
                            result_dict[ls.listener_id] = result
                except FatalException as e:
                    _logger_listeners_exec.error(traceback.format_exc(), extra={'trigger_name': trigger_name,
                                                                                'listener_id': ls.listener_id})
                    raise e
                except SignalEmitException as e:
                    _logger_listeners_exec.error(e, extra={'trigger_name': trigger_name,
                                                           'listener_id': ls.listener_id})
                except Exception as e:
                    exception.append((ls.listener_id, traceback.format_exc() if verbose else e))
            if async_listener:
                async_pool.flash()
            else:
                for async_task in async_task_list:
                    loop.run_until_complete(async_task)

            for ex in exception:
                _logger_listeners_exec.error(ex[1], extra={'trigger_name': trigger_name,
                                                           'listener_id': ex[0]})
            return result_dict

        return listeners_exec


class SignalContext:
    __slot__ = ['__inner_trigger_dict']
    def __init__(self, lm: ListenerManager):
        self.__inner_trigger_dict = lm.trigger_dict

    def get_trigger(self, signal_name):
        try:
            return self.__inner_trigger_dict[signal_name]
        except KeyError:
            available_triggers = self.__inner_trigger_dict.keys()
            matches = difflib.get_close_matches(signal_name, available_triggers, n=1, cutoff=0.6)

            if matches:
                suggestion = matches[0]
                _logger.error(f"Unknown signal: {signal_name}, do you mean '{suggestion}'?")
            else:
                _logger.error(f"Unknown signal: {signal_name}")
            return None

    def __getattr__(self, item):
        return self.get_trigger(item)


def format_error_output(listener: Listener):
    func = listener.func
    func_name = func.__name__
    sig = signature(func)

    params = []
    for name, param in sig.parameters.items():
        param_str = name
        if param.default != param.empty:
            param_str += f"={param.default}"
        if param.kind == param.VAR_POSITIONAL:
            param_str = "*" + name
        elif param.kind == param.VAR_KEYWORD:
            param_str = "**" + name
        params.append(param_str)
    signature_str = f"{func_name}({', '.join(params)})"

    error_msg = f"Signal '{func_name}' emitted abortively:\n"
    error_msg += f"{signature_str}\n"

    return error_msg


def safe_execute_listener(listener, context, args, kwargs):
    try:
        return listener.func(context, *args, **kwargs)
    except TypeError as e:
        # 检查是否是参数数量不匹配的错误
        if "takes 0 positional arguments but" in str(e):
            try:
                return listener.func()
            except Exception:
                raise SignalEmitException(format_error_output(listener))
        else:
            raise SignalEmitException(format_error_output(listener))
    except Exception as e:
        raise e