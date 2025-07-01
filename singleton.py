import logging
import threading
from functools import wraps
from typing import Dict, Literal, Type

logger = logging.getLogger('main_logger')

singleton_global_registry: Dict[Type, object] = {}
singleton_module_registry: Dict[str, Dict[Type, object]] = {}
singleton_lock = threading.Lock()

def singleton(scope: Literal["global", "module"] = "global"):
    def decorator(cls):
        @wraps(cls)
        def global_wrapper(*args, **kwargs):
            with singleton_lock:
                if cls not in singleton_global_registry:
                    singleton_global_registry[cls] = cls(*args, **kwargs)
                return singleton_global_registry[cls]

        @wraps(cls)
        def module_wrapper(*args, **kwargs):
            module_name = get_caller_module()
            with singleton_lock:
                if module_name not in singleton_module_registry:
                    singleton_module_registry[module_name] = {}
                if cls not in singleton_module_registry[module_name]:
                    singleton_module_registry[module_name][cls] = cls(*args, **kwargs)
                return singleton_module_registry[module_name][cls]

        if scope == "global":
            global_wrapper._original_class = cls
            return global_wrapper
        elif scope == "module":
            module_wrapper._original_class = cls
            return module_wrapper
        else:
            raise ValueError(f"Unknown scope: {scope}")
    return decorator


def get_caller_module():
    import inspect
    frame = inspect.currentframe()
    while frame:
        module = inspect.getmodule(frame)
        if module and module.__name__ != __name__:
            module_name = module.__name__
            module_name = module_name.split('.')[0]
            return module_name
        frame = frame.f_back
    return "__main__"
