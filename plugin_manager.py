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
from typing import Callable, Any, Dict, List, Union, Set, Optional, Awaitable

from singleton import singleton, singleton_module_registry

VERSION = '1.1.4'

_empty_function = lambda *args, **kwargs: None

LOG_FORMAT = '[%(asctime)s.%(msecs)03d] %(filename)s -> %(funcName)s line:%(lineno)d [%(levelname)s] : %(message)s'
TRIGGER_LOG_FORMAT = '[%(asctime)s.%(msecs)03d] Trigger("%(trigger_name)s") -> %(listener_id)s [%(levelname)s] : %(message)s'

DATE_FORMAT = '%Y-%m-%d  %H:%M:%S'

# 主 logger
_logger = logging.getLogger('main_logger')
_logger.setLevel(logging.DEBUG)

if not _logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))
    _logger.addHandler(console_handler)

# listeners 执行日志专用 logger
_logger_listeners_exec = logging.getLogger('plugin_manager_listeners_exec')
_logger_listeners_exec.setLevel(logging.DEBUG)

if not _logger_listeners_exec.handlers:
    listener_handler = logging.StreamHandler()
    listener_handler.setLevel(logging.DEBUG)
    listener_handler.setFormatter(logging.Formatter(TRIGGER_LOG_FORMAT, datefmt=DATE_FORMAT))
    _logger_listeners_exec.addHandler(listener_handler)

@dataclass
class Listener:
    listener_id: str
    order: int
    func: Callable[..., Any]
    single: bool

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

@singleton(scope='module')
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
    def __init__(self, interval: float = 0.1, max_concurrent: Optional[int] = None):
        self.interval = interval
        self._queue = Queue()
        self.max_concurrent = max_concurrent
        self._active_tasks = 0
        self._running = threading.Event()
        self._running.set()
        self._shutdown_lock = threading.Lock()
        self._loop_thread = threading.Thread(target=self._loop_worker, daemon=True)
        self._loop_thread.start()
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def __len__(self) -> int:
        return self._queue.qsize()

    @property
    def active_tasks(self) -> int:
        """Return the number of currently active/running tasks."""
        return self._active_tasks

    def add_task(self, coro):
        """Schedule a coroutine from *any* thread."""
        if not self._running.is_set():
            return False
        self._queue.put(coro)
        return True

    async def shutdown(self, timeout: Optional[float] = None) -> None:
        """top the pool gracefully."""
        with self._shutdown_lock:  # Prevent multiple simultaneous shutdowns
            if not self._running.is_set():
                return

            self._running.clear()

            if self._loop is not None:
                # Wake up the event-loop so it notices the flag
                self._loop.call_soon_threadsafe(lambda: None)

            if threading.current_thread() != self._loop_thread:
                # Wait for loop thread to finish if we're not in it
                self._loop_thread.join(timeout=timeout)
                if self._loop_thread.is_alive():
                    raise TimeoutError("Shutdown timed out")

                if self._loop is not None:
                    self._loop.close()

    def _loop_worker(self):
        """Runs in the background thread, owns the event-loop."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        try:
            # Kick off the polling coroutine and stay alive
            self._loop.create_task(self._poll_queue())
            self._loop.run_forever()
        finally:
            # Clean up any remaining tasks
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()

            # Run until all tasks are cancelled
            self._loop.run_until_complete(
                asyncio.gather(*pending, return_exceptions=True)
            )

            # Reset the loop reference since we're shutting down
            self._loop = None

    async def _poll_queue(self) -> None:
        """Coroutine that pulls tasks off the thread-safe queue."""
        while self._running.is_set():
            # Check if we've hit max concurrent tasks
            if self.max_concurrent is not None and self._active_tasks >= self.max_concurrent:
                await asyncio.sleep(self.interval)
                continue

            try:
                # Non-blocking check
                coro = self._queue.get_nowait()
            except queue.Empty:
                await asyncio.sleep(self.interval)
                continue

            self._active_tasks += 1
            task = asyncio.create_task(self._run_task(coro))
            # Add callback to decrement active tasks count when done
            task.add_done_callback(lambda _: self._decrement_active_tasks())

    def _decrement_active_tasks(self) -> None:
        """Thread-safe decrement of active tasks counter."""
        self._active_tasks -= 1

    async def _run_task(self, coro: Awaitable[Any]) -> None:
        """Wrapper to run a task with proper error handling."""
        try:
            await coro
        except asyncio.CancelledError:
            # Task was cancelled during shutdown - this is expected
            pass
        except Exception:
            traceback.print_exc()  # Consider using logging module instead


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
    def __init__(self, _async=True,
                 max_concurrent=None):
        self.ctx = None
        self.trigger_dict: Dict[str, Callable[..., SignalResponse]] = {}
        self.trigger_list: Set[str] = set([])
        self.listener_dict: Dict[str, List[Listener]] = {}
        # plugin_id
        self.plugin_applied_register: List[str] = []
        # plugin_id - module_name
        self.plugin_list: Dict[str, str] = {}
        self.async_listener = _async
        self.unloaded_trigger_list = set([])
        self.loading_lock = threading.Lock()
        if _async:
            _logger.info('Async ListenerManager is enabled')
            self.async_pool = AsyncFunctionPool(max_concurrent=max_concurrent)
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

        amount = self.merge_listener_dict(plugin_listener_register.listener_dict)
        self.trigger_list.update(plugin_listener_register.listener_dict.keys())
        self.unloaded_trigger_list.update(plugin_listener_register.listener_dict.keys())

        return amount

    def merge_listener_dict(self, dict2):
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
        for signal_name in list(self.listener_dict.keys()):
            listeners = self.listener_dict[signal_name]

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
                del self.listener_dict[signal_name]
                if signal_name in self.trigger_list:
                    self.trigger_list.remove(signal_name)
                _logger.debug(f'Event "{signal_name}" removed as it has no more listeners')
            else:
                self.listener_dict[signal_name] = remaining_listeners
        _logger.info(f'Unloaded {len(removed_plugins)} plugin(s) and {removed_count} listener(s)')
        return removed_count > 0 or len(removed_plugins) > 0

    def load(self) -> Set[str]:
        try:
            _logger.debug("Applying nest_asyncio and loading triggers...")
            nest_asyncio.apply()
            with self.loading_lock:
                if not self.listener_dict:
                    _logger.warning("No listeners registered before load")
                affected_plugins = self.reload()
                if not affected_plugins:
                    _logger.debug("No plugins affected by load")
                else:
                    _logger.debug(f"Affected plugins: {affected_plugins}")
                if not self.trigger_dict:
                    _logger.warning("No triggers created after load")
                return affected_plugins
        except Exception as e:
            _logger.error(f"Error during load: {str(e)}")
            _logger.debug(traceback.format_exc())
            raise

    def reload(self) -> Set[str]:
        self.trigger_dict = {trigger: self.trigger_of(sorted(self.listener_dict[trigger], key=lambda x: -x.order),
                                                      trigger)
                              for trigger in self.unloaded_trigger_list}
        affected_plugins = set()
        for trigger in self.unloaded_trigger_list:
            for listener in self.listener_dict.get(trigger, []):
                plugin_id = listener.listener_id.split('-', 2)[0] + '-' + listener.listener_id.split('-', 2)[1]
                affected_plugins.add(plugin_id)

        self.unloaded_trigger_list.clear()
        del self.ctx
        self.ctx = SignalContext(self)
        return affected_plugins

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

        self.all_plugins = []

        for possible_module in possible_modules:
            module_path = os.path.join(plugin_fold_path, possible_module)
            if os.path.isfile(module_path) and os.path.splitext(module_path)[1] != ".py":
                continue
            if module_path.endswith(".disabled"):
                continue
            self.all_plugins.append((possible_module, plugin_fold_path, module_path))

        for possible_module, plugin_fold_path, module_path in self.all_plugins:
            time_before = time.perf_counter()
            success, _ = self.load_plugin(possible_module, plugin_fold_path)
            import_times.append((time.perf_counter() - time_before, module_path, success > 0))
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

            plugin_name = getattr(module, '__name__ ', module.__package__) or module.__name__
            plugin_version = getattr(module, '__version__ ', 'null')
            if f'{plugin_name}-{plugin_version}' in self.plugin_list:
                raise Exception(f'Plugin {plugin_name}-{plugin_version} already registered')
            self.plugin_list[f'{plugin_name}-{plugin_version}'] = module_name

            registers = []
            smr = singleton_module_registry[module.__name__].get(PluginListenerRegister._original_class)
            if hasattr(module, '__listener_register__'):
                registers.append(module.__listener_register__)
            elif smr is not None:
                registers.append(smr)
            else:
                registers = [v for v in module.__dict__.values() if isinstance(v, PluginListenerRegister._original_class)]
            for _r in registers:
                _r.generate_id(plugin_name, plugin_version)
                amount += self.apply_plugin_listener_register(_r)
            return amount, module
        except Exception as e:
            _logger.warning(traceback.format_exc())
            _logger.warning(f"Cannot import {module_path} module for custom nodes: {e}")
            return -1, None

    def trigger_of(self, ls_list: List[Listener], trigger_name):
        loop = asyncio.get_event_loop()
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
                    if ls.listener_id in skip_list:
                        continue

                    result = safe_execute_listener(ls, self.ctx, args, kwargs)

                    if ls.single:
                        _logger_listeners_exec.debug('Interrupt polling', extra={'trigger_name': trigger_name,
                                                                                 'listener_id': ls.listener_id})
                        if asyncio.iscoroutine(result):
                            handle_coroutine(result, ls, async_listener, async_pool, async_task_list)
                        else:
                            result_dict[ls.listener_id] = result
                        break

                    if result == ListenerManagerResponseOperationCode.INTERRUPT or result == ListenerManagerResponseOperationCode.EXIT_FOR_FATAL_ERROR:
                        status = handle_response_code(result, ls, result_dict, skip_list, trigger_name)
                        if status == 'break':
                            break
                    elif result is None:
                        continue
                    elif isinstance(result, ListenerManagerResponse):
                        status = handle_response_object(result, ls, result_dict, skip_list, trigger_name)
                        if status == 'break':
                            break
                    elif asyncio.iscoroutine(result):
                        handle_coroutine(result, ls, async_listener, async_pool, async_task_list)
                    else:
                        result_dict[ls.listener_id] = result

                except FatalException as e:
                    _logger_listeners_exec.error(traceback.format_exc(),
                                                 extra={'trigger_name': trigger_name, 'listener_id': ls.listener_id})
                    raise e
                except SignalEmitException as e:
                    _logger_listeners_exec.error(e, extra={'trigger_name': trigger_name, 'listener_id': ls.listener_id})
                except Exception as e:
                    exception.append((ls.listener_id, traceback.format_exc()))
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


def handle_coroutine(result, ls, async_listener, async_pool, async_task_list):
    if async_listener:
        async_pool.add_task(result)
    else:
        async_task_list.append(result)

def handle_response_code(result, ls, result_dict, skip_list, trigger_name):
    if result == ListenerManagerResponseOperationCode.INTERRUPT:
        _logger_listeners_exec.debug('Interrupt polling', extra={'trigger_name': trigger_name, 'listener_id': ls.listener_id})
        result_dict[ls.listener_id] = result
        return 'break'
    elif result == ListenerManagerResponseOperationCode.EXIT_FOR_FATAL_ERROR:
        raise FatalException('Manual Exit')
    return 'continue'

def handle_response_object(result, ls, result_dict, skip_list, trigger_name):
    if result.operation == ListenerManagerResponseOperationCode.INTERRUPT:
        _logger_listeners_exec.debug('Interrupt polling', extra={'trigger_name': trigger_name, 'listener_id': ls.listener_id})
        result_dict[ls.listener_id] = result.param
        return 'break'
    elif result.operation == ListenerManagerResponseOperationCode.EXIT_FOR_FATAL_ERROR:
        raise FatalException(str(result.param))
    elif result.operation == ListenerManagerResponseOperationCode.SKIP:
        if isinstance(result.param, str):
            skip_list.append(result.param)
        elif isinstance(result.param, list):
            skip_list.extend(result.param)
    return 'continue'


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
        if listener.__name__ not in msg or depth > 1:
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