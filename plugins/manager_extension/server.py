import socket
import threading

import msgpack

from contextlib import suppress
import itertools
from functools import partial

from typing import List, Dict

from plugin_manager import PluginListenerRegister
from main import logger

lr = PluginListenerRegister()

_server_thread = None
_server_sock: socket.socket = None
_clients: List[socket.socket] = []
_server_loop_event = threading.Event()

_client_sock: socket.socket = None
_client_recv_thread = None
_id_counter = itertools.count(1)
_pending_rsp: Dict[int, threading.Event] = {}


def _broadcast(data: bytes, exclude: socket.socket = None):
    """把 data 同步广播给所有 client；写失败的 client 会被剔除"""
    global _clients
    stale = []
    for c in _clients:
        if c is exclude:
            continue
        with suppress(BrokenPipeError, ConnectionResetError):
            c.sendall(data)
        stale.append(c)
    for c in stale:               # 移除失活连接
        _clients.remove(c)
        with suppress(Exception):
            c.close()

def _server_accept_loop(ctx, host: str, port: int):
    global _server_sock
    _server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    _server_sock.bind((host, port))
    _server_sock.listen(5)
    ctx.echo(f"[server] listening on {host}:{port}")
    while not _server_loop_event.is_set():
        conn, addr = _server_sock.accept()
        _clients.append(conn)
        ctx.echo(f"[server] new client {addr}")
        threading.Thread(target=_client_handler, args=(ctx, conn, addr), daemon=True).start()

@lr('_on_remote_handle', single=True)
async def on_handle(ctx, signal: str, parameters, feedback):
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
                feedback(e)

def _client_handler(ctx, conn: socket.socket, addr):
    unpacker = msgpack.Unpacker(raw=False)

    def conn_send(_conn, _msg_id, _str):
        payload = {
            "type": "rsp",
            "id": _msg_id,
            "result": _str,
        }
        packed = msgpack.packb(payload)
        with suppress(BrokenPipeError, ConnectionResetError):
            conn.sendall(packed)
    with conn:
        while True:
            try:
                chunk = conn.recv(4096)
            except (ConnectionResetError, OSError):
                _clients.remove(conn)
                with suppress(Exception):
                    conn.close()
            if not chunk:
                break
            unpacker.feed(chunk)
            for obj in unpacker:
                if isinstance(obj, int):
                    continue
                if obj.get("type") == "cmd":
                    msg_id = obj.get("id")
                    signal = obj.get("signal")
                    params = obj.get("parameters", [])
                    logger.info(f'recv from {addr}: {signal}')
                    ctx._on_remote_handle(signal, params, partial(conn_send, conn, msg_id))
                else:
                    ctx.echo(f"\n[remote {addr}] {obj!r}")
    ctx.echo(f"[server] client {addr} disconnected")

def _client_recv_loop(ctx, sock: socket.socket):
    global _client_sock
    unpacker = msgpack.Unpacker(raw=False)
    while True:
        try:
            chunk = sock.recv(4096)
        except (ConnectionResetError, OSError):
            if _client_sock:
                with suppress(Exception):
                    _client_sock.shutdown(socket.SHUT_RDWR)
                    _client_sock.close()
                _client_sock = None
                logger.info('[server] Connect closed')
            return
        if not chunk:
            break
        unpacker.feed(chunk)
        for obj in unpacker:
            if isinstance(obj, int):
                continue
            if obj.get("type") == "rsp":
                msg_id = obj.get("id")
                result = obj.get("result")
                ctx.echo(f"\n[server→rsp #{msg_id}]\n{result}")
                # 若有同步等待，唤醒
                ev = _pending_rsp.pop(msg_id, None)
                if ev:
                    ev.result = result
                    ev.set()

@lr('server', single=True)
def start_server(ctx, block: str='False', host: str='0.0.0.0', port: str='9000'):
    global _server_thread
    if _server_thread and _server_thread.is_alive():
        logger.info("[server] already running")
        return
    _server_thread = threading.Thread(
        target=_server_accept_loop,
        args=(ctx, host, int(port)),
        daemon=True,
    )
    _server_thread.start()
    if bool(block):
        _server_loop_event.wait()

@lr('connect', single=True)
def connect_server(ctx, host: str = '127.0.0.1', port: str = '9000'):
    global _client_sock, _client_recv_thread
    if _client_sock and _client_sock.fileno() != -1:
        logger.info("[client] already connected — use 'disconnect' first")
        return
    _client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        _client_sock.connect((host, int(port)))
    except OSError as e:
        logger.info(f"[client] connection failed: {e}")
        _client_sock.close()
        _client_sock = None
        return
    logger.info(f"[client] connected to {host}:{port}")
    _client_recv_thread = threading.Thread(
        target=_client_recv_loop, args=(ctx, _client_sock), daemon=True)
    _client_recv_thread.start()

@lr('send', single=True)
def send_message(ctx, *message_parts):
    """send <msg…> —— 把消息发到服务器"""
    if not _client_sock or _client_sock.fileno() == -1:
        logger.info("[client] not connected — use 'connect' first")
        return
    msg = " ".join(message_parts) + '\n'
    if msg:
        try:
            _client_sock.sendall(msg.encode())
        except OSError as e:
            logger.info(f"[client] send failed: {e}")

@lr('disconnect', single=True)
def disconnect_client(ctx):
    global _client_sock
    if _client_sock:
        with suppress(Exception):
            _client_sock.shutdown(socket.SHUT_RDWR)
            _client_sock.close()
        _client_sock = None
        logger.info("[client] disconnected")

@lr('call', single=True)
def call_server(_, signal: str, *params):
    if not _client_sock or _client_sock.fileno() == -1:
        logger.info("[client] not connected — use 'connect' first")
        return

    msg_id = next(_id_counter)
    payload = {
        "type": "cmd",
        "id": msg_id,
        "signal": signal,
        "parameters": list(params),
    }
    packed = msgpack.packb(payload)
    try:
        _client_sock.sendall(packed + b"\n")
    except OSError as e:
        logger.info(f"[client] send failed: {e}")
        return

    # —— 可选同步等待：30 s 超时 —— #
    ev = threading.Event()
    _pending_rsp[msg_id] = ev
    if ev.wait(timeout=30):
        # ctx.echo(f"[client] sync result: {ev.result}")
        ...
    else:
        logger.info("[client] waited 30 s, no response (still listening async)")