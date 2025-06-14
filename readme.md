# plugin_manager.py

创建监听器管理器
```python
from plugin_manager import ListenerManager
LISTENER = ListenerManager()

sys.path.append(folder_path)
LISTENER.load_plugins(folder_path)
ctx = LISTENER.load()

# 发出example信号，不携带参数
ctx.example()
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
```python
from plugin_manager import PluginListenerRegister
lr = PluginListenerRegister()

@lr('echo')
def echo(_, raw_input: str):
    return raw_input

@lr('listener')
def listener(ctx, raw_input):
    listener_result = ctx.on_input(raw_input)[echo]
    # 也可以直接使用listener_id
    listener_result = ctx.on_input(raw_input)[echo.listener_id]
```
  - 移除ListenerManager的modify_listener方法，现在可以使用如下方法来便捷更改Listener
```python
from plugin_manager import PluginListenerRegister
lr = PluginListenerRegister()

@lr('echo')
def stout_output(ctx, msg: str):
    def _on_input(_ctx, _msg: str):
        print('Be modified')

    stout_output.func = _on_input
    print(msg)
```
  - 通过ctx，方便访问触发器上下文

```python
from plugin_manager import PluginListenerRegister

lr = PluginListenerRegister()
signal_a = lr.for_trigger('a')


@signal_a(order=1)
def a1(ctx):
  return 'a1'


@signal_a(order=0)
def a2(ctx):
  previous_result = ctx.context[a1]
  print(previous_result)
  return 'a2'
```