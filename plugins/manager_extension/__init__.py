from .colorlog import patch_logger_with_color
from .cmd import lr as lr_main
from .server import lr as lr_server

PLUGIN_VERSION = '1.0.0'
LISTENER_REGISTER = lr_main + lr_server