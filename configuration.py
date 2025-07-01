import json
import logging
import os
from dataclasses import dataclass, _MISSING_TYPE
from dataclasses import fields, is_dataclass
from enum import Enum
from typing import Type, List, Tuple, Callable, Dict, runtime_checkable, Protocol, get_origin, get_args, Union, Any

from singleton import singleton, singleton_module_registry
from plugin_manager import ListenerManager

try:
    import tomllib as toml
except ImportError:
    import toml

from utils import match_names

logger = logging.getLogger('main_logger')

@runtime_checkable
@dataclass
class Configuration(Protocol):
    """
    Base class for configurations, can be extended to define specific configuration types.
    """

    @classmethod
    def parser(cls, row_config: dict, strict: bool = False):
        """
        Default parser for dataclass-based subclasses. If not overridden,
        it will initialize the dataclass with the provided dictionary.
        It will also attempt to convert string or integer values to StrEnum or IntEnum.
        """
        _params = {}
        _attr_dict = cls.__annotations__
        for key, value in row_config.items():
            if key not in _attr_dict:
                if strict:
                    raise KeyError(f"Unexpected field: {key}")
                continue
            field_type = _attr_dict[key]
            try:
                # Handle Enum
                if isinstance(field_type, type) and issubclass(field_type, Enum):
                    if isinstance(value, str):
                        value = field_type[value]
                    else:
                        value = field_type(value)

                # Handle List[...]
                elif get_origin(field_type) in (list, List):
                    inner_type = get_args(field_type)[0]
                    if is_dataclass(inner_type) and isinstance(value, list):
                        value = [inner_type.parser(v) if hasattr(inner_type, 'parser') else inner_type(**v) for v in value]
                    else:
                        value = [inner_type(v) for v in value]

                # Handle Optional[...]
                elif get_origin(field_type) is Union and type(None) in get_args(field_type):
                    # Optional[X] is Union[X, NoneType]
                    actual_type = [arg for arg in get_args(field_type) if arg is not type(None)][0]
                    if value is not None:
                        if is_dataclass(actual_type) and isinstance(value, dict):
                            value = actual_type.parser(value) if hasattr(actual_type, 'parser') else actual_type(
                                **value)
                        else:
                            value = actual_type(value)

                # Handle Dict[...]
                elif get_origin(field_type) in (dict, Dict):
                    key_type, val_type = get_args(field_type)
                    if not isinstance(value, dict):
                        raise TypeError(f"Expected dict for field '{key}', got {type(value).__name__}")
                    new_dict = {}
                    for k, v in value.items():
                        k_converted = key_type(k)
                        if is_dataclass(val_type) and isinstance(v, dict):
                            v_converted = val_type.parser(v) if hasattr(val_type, 'parser') else val_type(**v)
                        else:
                            v_converted = val_type(v)
                        new_dict[k_converted] = v_converted
                    value = new_dict

                # Handle nested dataclass
                elif is_dataclass(field_type) and isinstance(value, dict):
                    value = field_type.parser(value) if hasattr(field_type, 'parser') else field_type(**value)

            except Exception as e:
                raise ValueError(f"Error processing field '{key}': {e}")

            _params[key] = value
        return cls(**_params)

@singleton(scope='module')
class ConfigRegistry:
    def __init__(self):
        self._registry: List[Tuple[Callable, Type[Configuration]]] = []
        self._config_list: List[Type[Configuration]] = []

    def register(self, predictor: Callable, cls: Type[Configuration]):
        self._registry.append((predictor, cls))
        self._config_list.append(cls)

    def get_all(self):
        return self._registry

    def clear(self):
        self._registry.clear()

    def configclass(self, *args, **kwargs):
        """
        UpperCamelCase, lowerCamelCase and SnakeCase can
        be accepted if strict is False
        """

        def _create_wrapper(cls: Type):
            dc = dataclass(cls)
            single_dc = singleton("module")(dc)
            alias = kwargs.get('alias', ())
            strict = kwargs.get('strict', False)

            if strict:
                predictor = lambda _n: _n if _n in alias else None
            else:
                if alias:
                    predictor = lambda _n: match_names(_n, alias)
                else:
                    predictor = lambda _n: [_n] if _n == cls.__name__ else []

            self.register(predictor, single_dc._original_class)
            return single_dc

        # case 1: @configclass
        if len(args) == 1 and callable(args[0]) and not kwargs:
            return _create_wrapper(args[0])

        # case 2: @configclass("alias", strict=True)
        kwargs['alias'] = args
        return _create_wrapper


def parser_toml(path, encoding='utf-8'):
    with open(path, 'r', encoding=encoding) as f:
        return toml.load(f)

def parser_json(path, encoding='utf-8'):
    with open(path, 'r', encoding=encoding) as f:
        return json.load(f)

# def parser_xml(path, encoding='utf-8') -> dict:
#     def etree_to_dict(elem):
#         children = list(elem)
#         if not children:
#             return elem.text.strip() if elem.text else ""
#         result = {}
#         for child in children:
#             child_dict = etree_to_dict(child)
#             if child.tag in result:
#                 if isinstance(result[child.tag], list):
#                     result[child.tag].append(child_dict)
#                 else:
#                     result[child.tag] = [result[child.tag], child_dict]
#             else:
#                 result[child.tag] = child_dict
#         return result
#
#     tree = ET.parse(path)
#     root = tree.getroot()
#     return {root.tag: etree_to_dict(root)}

class ConfigManager:
    parsers = {
        'json': (lambda name: name.endswith('.json'), parser_json),
        'toml': (lambda name: name.endswith('.toml'), parser_toml),
        # 'xml': (lambda name: name.endswith('.xml'), parser_xml)
    }

    def __init__(self, root: str, encoding='utf-8'):
        self.root = root
        self.config_instances = {}
        self.config_data = {}

        self.uninjected_config_data = {}
        self.encoding = encoding

    def _handle(self, path):
        unhandled = []
        for root, dirs, files in os.walk(path, topdown=False):
            for name in files:
                path = os.path.join(root, name)
                flag = True
                for key, (checker, parser) in self.parsers.items():
                    if checker(name):
                        self.uninjected_config_data.update(
                            parser(path, self.encoding)
                        )
                        flag = False
                if flag:
                    unhandled.append(name)

        if len(unhandled):
            logger.warning('Unsupported config files: %s', unhandled)

    def handle(self, config_dir):
        config_files = os.path.join(self.root, config_dir)
        self._handle(config_files)

    def get_config(self, cls: Type[Configuration]):
        return self.config_instances.get(cls.__name__)

    def __getitem__(self, cls: Type[Configuration]):
        return self.config_instances[cls.__name__]

    def load_configs(self, config_registry: ConfigRegistry):
        succeed = []
        unexisted = []
        fail = []
        for predictor, cls in config_registry.get_all():
            # Create instances of the class based on parsed configuration data
            cls_name = cls.__name__
            initialized = False
            found = False
            for _config_name in self.uninjected_config_data.copy():
                if len(names := predictor(_config_name)) > 0:
                    found = True
                    config_dict = self.uninjected_config_data.pop(names[0])
                    try:
                        # Create the instance and call _parser to initialize
                        config_instance = cls.parser(config_dict)
                        self.config_instances[cls_name] = config_instance
                        initialized = True
                        # Store config_instance or perform additional actions here
                        logger.info(f"Created instance of {cls_name} with config {config_dict}")
                        self.config_data[names[0]] = config_dict
                        succeed.append(cls)
                    except Exception as e:
                        self.uninjected_config_data[names[0]] = config_dict
                        logger.warning(f'Occurred exceptions when loading {names[0]}:\n{e}')
                        fail.append(cls)
            if not initialized:
                try:
                    # Try to create the instance
                    config_instance = cls.parser({})
                    self.config_instances[cls_name] = config_instance
                except TypeError:
                    logger.warning(f'{cls_name} cannot create an instance without config')
                    if found:
                        fail.append(cls)

            if not found:
                unexisted.append(cls)

        return succeed, unexisted, fail


    def auto_config(self, lm: ListenerManager, _format='toml'):
        load_plugin = lm.load_plugin

        def _load_plugin(possible_module, workspace_path):
            from folder_paths import PathManager
            num, module = load_plugin(possible_module, workspace_path)
            module_name = module.__name__
            pm = PathManager()
            pm.folder_paths[module_name] = os.path.join(self.root, module_name)
            self._handle(pm.dir(module_name))
            cr = singleton_module_registry[module_name].get(ConfigRegistry._original_class)
            if cr is None:
                singleton_module_registry[module_name] = {}
                return num, module
            configclasses = cr._config_list
            _, unexisted, _ = self.load_configs(cr)
            generate_config_templates_to_dir(unexisted,
                                             output_dir=os.path.join(PathManager().dir('configs'), module_name),
                                             _format=_format)
            singleton_module_registry[module_name] = {
                configclass: self.get_config(configclass) for configclass in configclasses
            }
            return num, module

        lm.load_plugin = _load_plugin


def get_default_or_placeholder(field_type, default):
    if default is not None:
        return default

    # Handle Enum
    if isinstance(field_type, type) and issubclass(field_type, Enum):
        return list(field_type)[0].name if list(field_type) else None

    origin = get_origin(field_type)

    # Handle Optional[T] => Union[T, None]
    if origin is Union and type(None) in get_args(field_type):
        actual_type = [t for t in get_args(field_type) if t is not type(None)][0]
        return get_default_or_placeholder(actual_type, None)

    # Handle List[T]
    if origin in (list, List):
        return []

    # Handle Dict[K, V]
    if origin in (dict, Dict):
        return {}

    # Handle common types
    if field_type == int:
        return 0
    elif field_type == float:
        return 0.0
    elif field_type == bool:
        return False
    elif field_type == str:
        return ""

    # Handle nested dataclass
    if is_dataclass(field_type):
        return field_type()  # Will fail if dataclass requires arguments

    # Optional fallback for Any or unknown types
    if field_type == Any:
        return None

    return None

def config_to_dict(config_cls: Type[Configuration]) -> dict:
    result = {}
    for f in fields(config_cls):
        if not isinstance(f.default, _MISSING_TYPE):
            value = f.default
        elif not isinstance(f.default_factory, _MISSING_TYPE):  # type: ignore
            value = f.default_factory()  # type: ignore
        else:
            value = get_default_or_placeholder(f.type, None)
        result[f.name] = value
    return result

# def dict_to_xml(data: dict, root_tag: str) -> str:
#     root = ET.Element(root_tag)
#     for key, value in data.items():
#         child = ET.SubElement(root, key)
#         child.text = str(value)
#     return ET.tostring(root, encoding="unicode")


def generate_config_templates_to_dir(config_registry,
                                     output_dir: str,
                                     _format: str = 'toml') -> Dict[str, str]:
    os.makedirs(output_dir, exist_ok=True)
    if _format == 'json':
        dumps = lambda _data: json.dumps(_data, indent=4)
        ext = 'json'
    elif _format == 'toml':
        dumps = toml.dumps
        ext = 'toml'
    # elif _format == 'xml':
    #     dumps = lambda _data: dict_to_xml(_data, root_tag=name)
    #     ext = 'xml'
    else:
        raise ValueError(f"Unsupported format: {_format}")
    results = {}

    for cls in config_registry:
        if not is_dataclass(cls):
            continue

        data = config_to_dict(cls)
        name = cls.__name__

        file_path = os.path.join(output_dir, f"{name}.{ext}")
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(dumps({name: data}))

        results[name] = dumps(data)
    return results
