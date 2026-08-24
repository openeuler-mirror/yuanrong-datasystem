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
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs


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
            self._remove_expired_locked()
            instances = self._services.get(service, [])
            return [inst.address for inst in instances if self._is_alive_locked(inst)]

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
        now = time.time()
        for service, instances in list(self._services.items()):
            alive = []
            for inst in instances:
                if now - inst.last_heartbeat > inst.ttl_seconds:
                    self._events.append(JfEvent(
                        service, inst.address, "expire", now,
                        f"ttl_expired({int(inst.ttl_seconds)}s)"
                    ))
                else:
                    alive.append(inst)
            self._services[service] = alive

    def ttl_sweeper_loop(self):
        while self._running:
            time.sleep(1)
            with self._lock:
                self._remove_expired_locked()

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
                return
            gen = registry.register(service, client_ip, port, ttl)
            self._send_json(200, {"ok": True, "generation": gen})

        elif path == "/heartbeat":
            service = body.get("service", "")
            port = int(body.get("port", 0))
            if not service or port <= 0:
                self._send_json(400, {"ok": False, "error": "service and port required"})
                return
            ok, result = registry.heartbeat(service, client_ip, port)
            if ok:
                self._send_json(200, {"ok": True, **result})
            else:
                self._send_json(404, {"ok": False, "error": result})

        elif path == "/unregister":
            service = body.get("service", "")
            port = int(body.get("port", 0))
            if not service or port <= 0:
                self._send_json(400, {"ok": False, "error": "service and port required"})
                return
            registry.unregister(service, client_ip, port)
            self._send_json(200, {"ok": True})

        else:
            self._send_json(404, {"ok": False, "error": "not found"})

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path.startswith("/discover/"):
            service = path[len("/discover/"):]
            instances = registry.discover(service)
            self._send_json(200, {"instances": instances})

        elif path == "/events":
            qs = parse_qs(parsed.query)
            svc = (qs.get("service") or [None])[0]
            events = registry.get_events(svc)
            self._send_json(200, events)

        elif path == "/health":
            self._send_json(200, {"ok": True})

        else:
            self._send_json(404, {"ok": False, "error": "not found"})

    def log_message(self, fmt, *args):
        pass  # suppress default logging


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
        print(f'mock_jf_server: fork failed: {e}', file=sys.stderr, flush=True)
        sys.exit(1)

    if pid > 0:
        # Parent: print PID and exit so kubectl exec returns immediately.
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
        print(f"mock_jf_server: failed to bind port {args.port}: {e}",
              file=sys.stderr, flush=True)
        sys.exit(1)

    if args.background:
        _daemonize(args.log)
        # Child continues here; parent has already printed PID and exited.

    # Start sweeper thread. In --background mode this runs in the child
    # (after fork); only the calling thread survives fork, so the sweeper
    # must be started here, not before _daemonize().
    sweeper = threading.Thread(target=registry.ttl_sweeper_loop, daemon=True)
    sweeper.start()

    print(f"JF mock server listening on 0.0.0.0:{args.port} "
          f"(ttl_default={args.ttl_default})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        registry.stop()
        server.server_close()


if __name__ == "__main__":
    main()
