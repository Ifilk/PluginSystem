import asyncio
import importlib.util
import logging
import os
import queue
import re
import sys
import threading
import time
import traceback
import difflib
from contextlib import suppress
from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum
from queue import Queue
from inspect import signature
import nest_asyncio
from typing import Callable, Any, Dict, List, Union

VERSION = '1.1.2'

_empty_function = lambda *args, **kwargs: None

LOG_FORMAT = '[%(asctime)s.%(msecs)03d] %(filename)s -> %(funcName)s line:%(lineno)d [%(levelname)s] : %(message)s'
TRIGGER_LOG_FORMAT = '[%(asctime)s.%(msecs)03d] Trigger("%(trigger_name)s") -> %(listener_id)s [%(levelname)s] : %(message)s'

DATE_FORMAT = '%Y-%m-%d  %H:%M:%S'

# 主 logger
logger = logging.getLogger('main_logger')
logger.setLevel(logging.DEBUG)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))
    logger.addHandler(console_handler)

# listeners 执行日志专用 logger
_logger_listeners_exec = logging.getLogger('plugin_manager_listeners_exec')
_logger_listeners_exec.setLevel(logging.DEBUG)

if not _logger_listeners_exec.handlers:
    listener_handler = logging.StreamHandler()
    listener_handler.setLevel(logging.DEBUG)
    listener_handler.setFormatter(logging.Formatter(TRIGGER_LOG_FORMAT, datefmt=DATE_FORMAT))
    _logger_listeners_exec.addHandler(listener_handler)

# 简写
_logger = logger

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
        self.plugin_name = ''
        self.plugin_version = ''

    def __add__(self, other):
        return self.merge(other)

    def merge(self, other):
        self.listener_dict.update(other.listener_dict)
        if self.plugin_name != other.plugin_name or self.plugin_version != other.plugin_version:
            raise ValueError("Inconsistent 'plugin_name' and 'plugin_version'")
        return self

    def generate_id(self, plugin_name, plugin_version):
        self.plugin_name = plugin_name
        self.plugin_version = plugin_version

    def register_listener(self, trigger_name: str, listener_name: str, func, order, single):
        # self.register_trigger(trigger_name)
        listener = Listener(
            listener_id=f'-{trigger_name}-{listener_name}',
            order=order,
            func=func,
            single=single,
        )
        self.listener_dict.setdefault(trigger_name, []).append(listener)
        listener.__name__ = func.__name__
        listener.__qualname__ = func.__qualname__
        doc = func.__doc__
        if not doc:
            params = []
            sig = signature(func)
            for name, param in sig.parameters.items():
                param_str = name
                if param.default != param.empty:
                    param_str += f"={param.default}"
                if param.kind == param.VAR_POSITIONAL:
                    param_str = "*" + name
                elif param.kind == param.VAR_KEYWORD:
                    param_str = "**" + name
                params.append(param_str)
            doc = f"{trigger_name}({', '.join(params)})"
        listener.__doc__ = doc
        return listener

    def register_trigger(self, trigger_name: str):
        if trigger_name not in self.listener_dict.keys():
            self.listener_dict[trigger_name] = []

    def apply_listener(self, trigger_name: str, listener_name: str, order=0, single=False):
        def listener(func):
            _logger.debug(f'listener {trigger_name}-{listener_name} register')
            return self.register_listener(trigger_name, listener_name, func, order, single)

        return listener

    def for_trigger(self, trigger_name, single=False):
        def apply_listener(listener=None, order=0, single=single):
            def decorator(_func):
                _actual_listener_name = listener if listener is not None else _func.__name__
                _logger.debug(f'listener {trigger_name}-{_actual_listener_name} registered')
                return self.register_listener(trigger_name, _actual_listener_name, _func, order, single)

            if callable(listener):
                func = listener
                actual_listener_name = func.__name__
                _logger.debug(f'listener {trigger_name}-{actual_listener_name} registered')
                return self.register_listener(trigger_name, actual_listener_name, func, 0, single)
            else:
                return decorator

        return apply_listener

    def __call__(self, trigger_name, single=False):
        return self.for_trigger(trigger_name, single)


class AsyncFunctionPool:
    def __init__(self, interval: float = 0.1):
        self.interval = interval
        self._queue = Queue()
        self._running = threading.Event()
        self._running.set()
        self._loop_thread = threading.Thread(target=self._loop_worker, daemon=True)
        self._loop_thread.start()

    def __len__(self) -> int:
        return self._queue.qsize()

    def add_task(self, coro):
        """Schedule a coroutine from *any* thread."""
        self._queue.put(coro)

    def shutdown(self):
        """Stop the pool gracefully."""
        self._running.clear()
        # wake up the event-loop so it notices the flag
        self._loop.call_soon_threadsafe(lambda: None)
        self._loop_thread.join()
        self._loop.close()

    def _loop_worker(self):
        """Runs in the background thread, owns the event-loop."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        # kick off the polling coroutine and stay alive
        self._loop.create_task(self._poll_queue())
        self._loop.run_forever()

    async def _poll_queue(self):
        """Coroutine that pulls tasks off the thread-safe queue."""
        while self._running.is_set():
            try:
                # non-blocking check
                coro = self._queue.get_nowait()
            except queue.Empty:
                await asyncio.sleep(self.interval)
                continue

            try:
                await coro
            except Exception:
                _logger.error(traceback.format_exc())


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
        self.plugin_applied_register = []
        self.plugin_list = []
        self.async_listener = async_listener
        self.verbose = verbose
        self.unloaded_trigger_list = set([])
        self.loading_lock = threading.Lock()
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

        amount = self.merge_dicts(plugin_listener_register.listener_dict)
        self.trigger_list.update(plugin_listener_register.listener_dict.keys())
        self.unloaded_trigger_list.update(plugin_listener_register.listener_dict.keys())

        return amount

    def merge_dicts(self, dict2):
        merged_dict = self.listener_dict
        amount = 0
        for key, value in dict2.items():
            if key in merged_dict:
                merged_dict[key].extend(value)
            else:
                merged_dict[key] = value
            amount += len(value)
        return amount


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

        self.trigger_dict = {trigger: self.trigger_of(sorted(self.listener_dict[trigger], key=lambda x: -x.order),
                                                      trigger)
                              for trigger in self.unloaded_trigger_list}
        self.unloaded_trigger_list.clear()
        del self.ctx
        self.ctx = SignalContext(self)
        return self.ctx

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

    def load_plugins(self, plugin_fold_path):
        sys.path.append(plugin_fold_path)
        import_times = []
        l = os.path.realpath(plugin_fold_path)
        possible_modules = os.listdir(l)
        amount = 0
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
            import_times.append((time.perf_counter() - time_before, module_path, success != 0))
            amount += success

        if len(import_times) > 0:
            for n in sorted(import_times):
                if n[2]:
                    import_message = ""
                else:
                    import_message = " (IMPORT FAILED)"
                _logger.info("{:6.3f} seconds{}: {}".format(n[0], import_message, n[1]))
        _logger.info(f'All plugins loaded')
        _logger.info(f'{len(self.listener_dict)} triggers registered')
        _logger.info(f'{amount} listeners registered')

    def load_plugin(self, possible_module, workspace_path):
        module_name = os.path.splitext(possible_module)[0]
        module_path = os.path.join(workspace_path, possible_module)
        amount = 0
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
                amount = self.apply_plugin_listener_register(module.LISTENER_REGISTER)
            else:
                if module.__package__.startswith('lib'):
                    return amount
                possible = [v for v in module.__dict__.values() if isinstance(v, PluginListenerRegister)]
                for plugin in possible:
                    plugin.generate_id(plugin_name, plugin_version)
                    amount = self.apply_plugin_listener_register(plugin)
            return amount
        except Exception as e:
            _logger.warning(traceback.format_exc())
            _logger.warning(f"Cannot import {module_path} module for custom nodes: {e}")
            return amount

    def trigger_of(self, ls_list: List[Listener], trigger_name):
        loop = asyncio.get_event_loop()
        verbose = self.verbose
        async_pool = self.async_pool
        async_listener = self.async_listener

        def listeners_exec(*args, **kwargs):
            exception = []
            skip_list = []
            result_dict = SignalResponse()
            async_task_list = []
            token = self.ctx.set_context(result_dict)
            for ls in ls_list:
                try:
                    if ls.listener_id not in skip_list:
                        result = safe_execute_listener(ls, self.ctx, args, kwargs)
                        if ls.single or result == ListenerManagerResponseOperationCode.INTERRUPT:
                            _logger_listeners_exec.debug('Interrupt polling', extra={'trigger_name': trigger_name,
                                                                                     'listener_id': ls.listener_id})
                            if asyncio.iscoroutine(result):
                                async_pool.add_task(result)
                                break
                            result_dict[ls.listener_id] = result
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
                                async_pool.add_task(result)
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
            self.ctx.reset_context(token)
            if not async_listener:
                for async_task in async_task_list:
                    loop.run_until_complete(async_task)

            for ex in exception:
                _logger_listeners_exec.error(ex[1], extra={'trigger_name': trigger_name,
                                                           'listener_id': ex[0]})
            return result_dict

        return listeners_exec


class SignalContext:
    __slot__ = ['_inner_trigger_dict', 'listener_manager']
    def __init__(self, lm: ListenerManager):
        self._inner_trigger_dict = lm.trigger_dict
        self.listener_manager = lm
        self._context = ContextVar("ctx", default={})

    def get_trigger(self, signal_name):
        try:
            return self._inner_trigger_dict[signal_name]
        except KeyError:
            available_triggers = self._inner_trigger_dict.keys()
            matches = difflib.get_close_matches(signal_name, available_triggers, n=1, cutoff=0.6)

            if matches:
                suggestion = matches[0]
                _logger.error(f"Unknown signal: {signal_name}, do you mean '{suggestion}'?")
            else:
                _logger.error(f"Unknown signal: {signal_name}")
            return None

    def __getattr__(self, item):
        return self.get_trigger(item)

    @property
    def context(self):
        return self._context.get()

    def set_context(self, _ctx):
        return self._context.set(_ctx)

    def reset_context(self, token):
        with suppress(ValueError):
            self._context.reset(token)

class SignalResponse:
    def __init__(self):
        self.__result = {}

    def __setitem__(self, key, value):
        self.__result[key] = value

    def __getitem__(self, listener: Union[str, Listener]):
        return self.__result.get(listener.listener_id if isinstance(listener, Listener) else listener)

    def __len__(self):
        return len(self.__result)

    def keys(self):
        return self.__result.keys()

    def values(self):
        return self.__result.values()


def format_error_output(listener: Listener, e):
    func_name = listener.__name__
    signature_str = listener.__doc__
    error_msg = f"Listener '{func_name}' emitted abortively:\n"
    error_msg += f"{signature_str}\n"
    error_msg += str(e)

    return error_msg


def safe_execute_listener(listener, context, args, kwargs, depth=0):
    try:
        return listener(context, *args, **kwargs) if context else listener()
    except TypeError as e:
        msg = str(e)
        if depth > 1:
            raise e
        depth += 1
        if re.search(r"takes \d+ positional arguments but", msg):
            if "takes 0 positional arguments but" in msg:
                return safe_execute_listener(listener, None, args, kwargs, depth)
            else:
                raise SignalEmitException(format_error_output(listener, e))
        elif "missing 1 required positional argument:" in msg:
            return safe_execute_listener(listener, context, [None], {}, depth)
        else:
            raise e
    except Exception as e:
        raise e