#!/usr/bin/env python3
"""Mock JF (JSF) service discovery server.

Simulates JD's JF service registry: register/unregister/discover with
TTL-based heartbeat expiry. Designed for KVCache Worker/Coordinator
independent deployment testing.

Usage:
    python3 mock_jf_server.py --port 9999 --ttl-default 30

HTTP API:
    POST /register     {"service":"...","port":31501,"ttl":30}  -> {"ok":true,"generation":N}
    POST /heartbeat    {"service":"...","port":31501}           -> {"ok":true,"ttl":30,"remaining_ttl":30}
    POST /unregister   {"service":"...","port":31501}           -> {"ok":true}
    GET  /discover/<name>                                       -> {"instances":["10.0.0.1:31501"]}
    GET  /events                                                -> [{"service","address","action","ts","reason"}]
"""
import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
# Two-stream design mirrors deploy_common.py: stdout logger carries
# request-level info lines (register/heartbeat/discover/...); stderr logger
# carries startup failures (bind/fork) that must be visible to ``kubectl
# exec`` BEFORE ``_daemonize`` redirects the fds. In ``--background`` mode,
# ``_daemonize`` does ``os.dup2(log_fd, 1)`` and ``os.dup2(log_fd, 2)``, so
# both streams land in the ``--log`` file in the child; the parent's stderr
# stays the original terminal/pipe so kubectl exec sees bind/fork errors
# directly. Format includes a timestamp (ISO8601, seconds) because this is a
# long-running server log, not CLI output -- deploy_common uses bare
# ``%(message)s`` for CLI greps, but a server log needs ordering evidence.

_stdout_logger = logging.getLogger('jf_mock.stdout')
_stderr_logger = logging.getLogger('jf_mock.stderr')
for _lg in (_stdout_logger, _stderr_logger):
    _lg.handlers = []
    _lg.propagate = False

_log_fmt = logging.Formatter('[%(asctime)s] %(message)s', datefmt='%Y-%m-%dT%H:%M:%S')

_stdout_handler = logging.StreamHandler(sys.stdout)
_stdout_handler.setFormatter(_log_fmt)
_stdout_logger.addHandler(_stdout_handler)
_stdout_logger.setLevel(logging.INFO)

_stderr_handler = logging.StreamHandler(sys.stderr)
_stderr_handler.setFormatter(_log_fmt)
_stderr_logger.addHandler(_stderr_handler)
_stderr_logger.setLevel(logging.WARNING)


def _log(msg, *args):
    """Info-level log to stdout (redirected to ``--log`` file in background).

    Drop-in for ``print(f'[{ts}] {msg}')``. Thread-safe: ``logging.Handler``
    has its own lock, so the HTTP handler thread and the TTL sweeper thread
    can call this concurrently without interleaving partial lines. The
    registry lock is NOT held while logging (``_remove_expired_locked``
    returns the expired list so callers log outside the lock).
    """
    if args:
        _stdout_logger.info(msg, *args)
    else:
        _stdout_logger.info(msg)


def _log_error(msg, *args):
    """Error-level log to stderr (visible to ``kubectl exec`` before redirect).

    Used for startup failures (bind/fork) that the caller must see to
    understand why the server did not come up. After ``_daemonize`` redirects
    fd 2, these also land in the ``--log`` file alongside ``_log`` lines.
    """
    if args:
        _stderr_logger.error(msg, *args)
    else:
        _stderr_logger.error(msg)


@dataclass
class ServiceInstance:
    ip: str
    port: int
    ttl_seconds: float
    last_heartbeat: float
    generation: int

    @property
    def address(self):
        return f"{self.ip}:{self.port}"


@dataclass
class JfEvent:
    service: str
    address: str
    action: str  # register | heartbeat | unregister | expire
    ts: float
    reason: str = ""


class JFRegistry:
    def __init__(self, ttl_default=30):
        self._lock = threading.Lock()
        self._services = {}  # service_name -> list[ServiceInstance]
        self._events = []  # list[JfEvent]
        self._register_counts = {}  # (service, address) -> count
        self._unregister_counts = {}
        self._gen_counters = {}  # (service, address) -> next generation
        self._ttl_default = ttl_default
        self._running = True

    def register(self, service, ip, port, ttl=None):
        if ttl is None:
            ttl = self._ttl_default
        address = f"{ip}:{port}"
        with self._lock:
            key = (service, address)
            gen = self._gen_counters.get(key, 0) + 1
            self._gen_counters[key] = gen
            self._register_counts[key] = self._register_counts.get(key, 0) + 1

            instances = self._services.setdefault(service, [])
            instances = [i for i in instances if i.address != address]
            inst = ServiceInstance(
                ip=ip, port=port, ttl_seconds=ttl,
                last_heartbeat=time.time(), generation=gen
            )
            instances.append(inst)
            self._services[service] = instances

            self._events.append(JfEvent(service, address, "register", time.time()))
            return gen

    def heartbeat(self, service, ip, port):
        address = f"{ip}:{port}"
        with self._lock:
            instances = self._services.get(service, [])
            for inst in instances:
                if inst.ip == ip and inst.port == port:
                    now = time.time()
                    if now - inst.last_heartbeat > inst.ttl_seconds:
                        return False, "not found or expired"
                    remaining = inst.ttl_seconds - (now - inst.last_heartbeat)
                    inst.last_heartbeat = now
                    self._events.append(JfEvent(service, address, "heartbeat", now))
                    return True, {"ttl": int(inst.ttl_seconds), "remaining_ttl": int(max(0, remaining))}
            return False, "not found or expired"

    def unregister(self, service, ip, port):
        address = f"{ip}:{port}"
        with self._lock:
            key = (service, address)
            self._unregister_counts[key] = self._unregister_counts.get(key, 0) + 1
            instances = self._services.get(service, [])
            before = len(instances)
            self._services[service] = [i for i in instances if i.address != address]
            removed = before - len(self._services[service])
            if removed > 0:
                self._events.append(JfEvent(service, address, "unregister", time.time()))
            return removed > 0

    def discover(self, service):
        with self._lock:
            expired = self._remove_expired_locked()
            instances = self._services.get(service, [])
            alive = [inst.address for inst in instances if self._is_alive_locked(inst)]
        for service_name, address, ttl in expired:
            _log(f'expire service={service_name} address={address} '
                 f'reason=ttl_expired({int(ttl)}s)')
        return alive

    def get_events(self, service_filter=None):
        with self._lock:
            if service_filter:
                return [
                    {"service": e.service, "address": e.address,
                     "action": e.action, "ts": e.ts, "reason": e.reason}
                    for e in self._events if e.service == service_filter
                ]
            return [
                {"service": e.service, "address": e.address,
                 "action": e.action, "ts": e.ts, "reason": e.reason}
                for e in self._events
            ]

    def _is_alive_locked(self, inst):
        return (time.time() - inst.last_heartbeat) <= inst.ttl_seconds

    def _remove_expired_locked(self):
        """Drop expired instances, return [(service, address, ttl_seconds)].

        Returns the expired list so the caller can ``_log`` each expiry
        OUTSIDE the registry lock (``_log`` does I/O via ``print``; holding
        the registry lock across I/O is fine for a mock but returning the
        list keeps the locked section tight and the log path lock-free).
        """
        now = time.time()
        expired = []
        for service, instances in list(self._services.items()):
            alive = []
            for inst in instances:
                if now - inst.last_heartbeat > inst.ttl_seconds:
                    self._events.append(JfEvent(
                        service, inst.address, "expire", now,
                        f"ttl_expired({int(inst.ttl_seconds)}s)"
                    ))
                    expired.append((service, inst.address, inst.ttl_seconds))
                else:
                    alive.append(inst)
            self._services[service] = alive
        return expired

    def ttl_sweeper_loop(self):
        while self._running:
            time.sleep(1)
            with self._lock:
                expired = self._remove_expired_locked()
            for service_name, address, ttl in expired:
                _log(f'expire service={service_name} address={address} '
                     f'reason=ttl_expired({int(ttl)}s) (sweeper)')

    def stop(self):
        self._running = False


registry = None


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw)
        except Exception:
            return {}

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        body = self._read_body()
        client_ip = self.client_address[0]

        if path == "/register":
            service = body.get("service", "")
            port = int(body.get("port", 0))
            ttl = body.get("ttl", None)
            if not service or port <= 0:
                self._send_json(400, {"ok": False, "error": "service and port required"})
                _log(f'register 400 from={client_ip} reason=missing_service_or_port '
                     f'service={service!r} port={port}')
                return
            gen = registry.register(service, client_ip, port, ttl)
            self._send_json(200, {"ok": True, "generation": gen})
            _log(f'register 200 from={client_ip} service={service} port={port} '
                 f'ttl={ttl} gen={gen}')

        elif path == "/heartbeat":
            service = body.get("service", "")
            port = int(body.get("port", 0))
            if not service or port <= 0:
                self._send_json(400, {"ok": False, "error": "service and port required"})
                _log(f'heartbeat 400 from={client_ip} reason=missing_service_or_port '
                     f'service={service!r} port={port}')
                return
            ok, result = registry.heartbeat(service, client_ip, port)
            if ok:
                self._send_json(200, {"ok": True, **result})
                _log(f'heartbeat 200 from={client_ip} service={service} port={port} '
                     f'remaining_ttl={result.get("remaining_ttl")}')
            else:
                self._send_json(404, {"ok": False, "error": result})
                _log(f'heartbeat 404 from={client_ip} service={service} port={port} '
                     f'reason={result}')

        elif path == "/unregister":
            service = body.get("service", "")
            port = int(body.get("port", 0))
            if not service or port <= 0:
                self._send_json(400, {"ok": False, "error": "service and port required"})
                _log(f'unregister 400 from={client_ip} reason=missing_service_or_port '
                     f'service={service!r} port={port}')
                return
            removed = registry.unregister(service, client_ip, port)
            self._send_json(200, {"ok": True})
            _log(f'unregister 200 from={client_ip} service={service} port={port} '
                 f'removed={removed}')

        else:
            self._send_json(404, {"ok": False, "error": "not found"})
            _log(f'POST 404 from={client_ip} path={path} reason=unknown_endpoint')

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        client_ip = self.client_address[0]

        if path.startswith("/discover/"):
            service = path[len("/discover/"):]
            instances = registry.discover(service)
            self._send_json(200, {"instances": instances})
            _log(f'discover 200 from={client_ip} service={service} '
                 f'instances={len(instances)}')

        elif path == "/events":
            qs = parse_qs(parsed.query)
            svc = (qs.get("service") or [None])[0]
            events = registry.get_events(svc)
            self._send_json(200, events)
            _log(f'events 200 from={client_ip} service_filter={svc} '
                 f'count={len(events)}')

        elif path == "/health":
            self._send_json(200, {"ok": True})
            _log(f'health 200 from={client_ip}')

        else:
            self._send_json(404, {"ok": False, "error": "not found"})
            _log(f'GET 404 from={client_ip} path={path} reason=unknown_endpoint')

    def log_message(self, fmt, *args):
        pass  # suppress default BaseHTTPRequestHandler access logging; _log covers it


def _daemonize(log_path=None):
    """Fork into background. Parent prints child PID to stdout and exits.

    Child creates a new session (``os.setsid``) and redirects stdin/stdout/stderr
    to ``log_path`` (or /dev/null) so it does not hold the caller's pipe open.
    This lets ``kubectl exec`` / ssh return immediately instead of hanging
    until timeout — same pattern as ``tools/procmon.py:_daemonize``.

    Must be called AFTER binding the listening port: the parent prints the
    PID only after the port is bound, so when ``kubectl exec`` returns the
    server is already accepting connections (no separate port-ready poll
    needed by the caller).
    """
    devnull = os.open('/dev/null', os.O_RDONLY)
    os.dup2(devnull, 0)

    try:
        pid = os.fork()
    except OSError as e:
        os.close(devnull)
        _log_error(f'mock_jf_server: fork failed: {e}')
        sys.exit(1)

    if pid > 0:
        # Parent: print PID and exit so kubectl exec returns immediately.
        # This is a PROTOCOL output (deploy_jf._start_jf_mock parses it as
        # the child PID), not a log -- must stay as bare print, not _log.
        print(pid, flush=True)
        os._exit(0)

    # Child: new session, detach from controlling terminal.
    os.setsid()

    # Redirect stdout/stderr to log file (or /dev/null if no log given).
    if log_path:
        log_fd = os.open(log_path,
                         os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    else:
        log_fd = devnull
    os.dup2(log_fd, 1)
    os.dup2(log_fd, 2)
    if log_path:
        os.close(log_fd)
    os.close(devnull)


def main():
    global registry
    parser = argparse.ArgumentParser(description="Mock JF service discovery server")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--ttl-default", type=int, default=30)
    parser.add_argument("--background", action="store_true",
                        help="Daemonize: fork into background, print child PID to "
                             "stdout, parent exits. Lets kubectl exec / ssh return "
                             "immediately instead of hanging. Must be combined with "
                             "port binding in the parent so the printed PID implies "
                             "port-ready (caller does not need a separate poll).")
    parser.add_argument("--log",
                        help="File to redirect stdout+stderr when --background is "
                             "used (default: /dev/null). Pass the same path the "
                             "caller would cat on failure so startup errors are "
                             "captured.")
    args = parser.parse_args()

    registry = JFRegistry(ttl_default=args.ttl_default)

    # Bind the port BEFORE forking. If bind fails, the parent exits with a
    # visible error (kubectl exec sees non-zero rc + stderr); no daemonization
    # happens. If bind succeeds and --background is set, the parent prints the
    # PID and exits — at that point the port is already listening, so the
    # caller knows the server is ready (no separate port-ready poll needed).
    try:
        server = HTTPServer(("0.0.0.0", args.port), Handler)
    except OSError as e:
        _log_error(f"mock_jf_server: failed to bind port {args.port}: {e}")
        sys.exit(1)

    if args.background:
        _daemonize(args.log)
        # Child continues here; parent has already printed PID and exited.

    # Start sweeper thread. In --background mode this runs in the child
    # (after fork); only the calling thread survives fork, so the sweeper
    # must be started here, not before _daemonize().
    sweeper = threading.Thread(target=registry.ttl_sweeper_loop, daemon=True)
    sweeper.start()
    _log(f'JF mock server listening on 0.0.0.0:{args.port} '
         f'(ttl_default={args.ttl_default}) sweeper started')

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        registry.stop()
        server.server_close()
        _log('JF mock server stopped')


if __name__ == "__main__":
    main()
