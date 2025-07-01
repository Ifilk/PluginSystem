# plugin_manager.py

创建监听器管理器
```python
import sys
from plugin_manager import ListenerManager

LISTENER = ListenerManager()

# 添加插件路径
sys.path.append(folder_path)

# 加载插件
LISTENER.load_plugins(folder_path)

# 初始化上下文
LISTENER.load()

# 发出 example 信号，不携带参数
LISTENER.ctx.example()
```



# plugin

创建全局插件注册器（推荐名字listenerRegister）
```python
from plugin_manager import PluginListenerRegister
listenerRegister = PluginListenerRegister()
```

如果包含多个python文件，请使用`__init__.py`处理依赖，并且在`__all__`中声明
```python
__all__ = ['listenerRegister']
```
# 使用
```python
# 声明信号
listenerRegister.register_trigger('example')
# 获取监听器注册器
example = listenerRegister.for_trigger('example')

# 注册监听器
@example
def example_listener():
    ...
```

# 监听上下文与返回值
```python
from plugin_manager import PluginListenerRegister

lr = PluginListenerRegister()

@lr('echo')
def echo(_, raw_input: str):
    return raw_input

@lr('listener')
def listener(ctx, raw_input):
    result = ctx.on_input(raw_input)[echo]  # 或者 [echo.listener_id]
    print("echo returned:", result)
```

# 修改监听器行为
```python
@lr('echo')
def stout_output(ctx, msg: str):
    def _on_input(_ctx, _msg: str):
        print('Be modified')

    # 替换监听函数
    stout_output.func = _on_input
    print(msg)
```

# 顺序与上下文传递
```python
signal_a = lr.for_trigger('a')

@signal_a(order=1)
def a1(ctx):
    return 'a1'

@signal_a(order=0)
def a2(ctx):
    previous_result = ctx.context[a1]
    print("a1 returned:", previous_result)
    return 'a2'
```

# 配置系统
> 从 1.1.4 起，系统支持插件配置注入功能，自动识别 `configs/xxx/xxx.toml` 配置文件。
```python
from configuration import ConfigRegistry, Configuration

cr = ConfigRegistry()

@cr.configclass
class TestConfiguration(Configuration):
    test: str
    testInt: int = 10
```
注：TestConfiguration已注册模块单例，TestConfiguration()即可直接获取

# 更新日志
- 1.1.0
  - 更便捷的监听器注册
  - register_trigger新增single参数表达默认轮询打断
  - SignalEmitException用来规则化信号触发参数异常抛出
  - 新增信号emit参数错误重试（不需要参数但有传入）
- 1.1.1
  - 新增SignalContext类，默认传参1号位参数（可省略）
  - PluginListenerRegister类新增merge操作（+运算符），可在一个plugin中合并多个PluginListenerRegister
- 1.1.2
  - 支持异步/多线程ctx 
  - 新增SignalResponse类，方便获取监听器返回值，如下
  - 移除ListenerManager的modify_listener方法，现在可以使用如下方法来便捷更改Listener
  - 通过ctx，方便访问触发器上下文
- 1.1.3
  - 更改注册变量名，符合python标准
- 1.1.4
  - 实现注册类自动装配功能，不再需要在__init__.py中声明listenerRegister
  - 实现配置系统，默认路径下configs/xxx/xxx.toml