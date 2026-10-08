import logging
import os
import threading
import time
from typing import Callable


_DEFAULT_EXIT = getattr(os, "_exit")

# Mirrors K_DEFAULT_READ_LEASE_TTL_MS in include/datasystem/transfer_engine/data_plane_backend.h
# and the parse rules of AscendBackend::GetEnvI32 in
# src/internal/backend/ascend/ascend_backend.cpp, which resolves the TTL the owner grants.
DEFAULT_READ_LEASE_TTL_MS = 30000
_INT32_MAX = 2147483647
_ASCII_DIGITS = frozenset("0123456789")


def read_lease_ttl_seconds(getenv: Callable[[str], object] = os.getenv) -> float:
    """Resolve the lease TTL the way AscendBackend::GetEnvI32 does, never raising.

    The C++ parser walks bytes and rejects anything outside ASCII '0'-'9', so
    str.isdigit is too permissive here: it accepts other Unicode decimal digits
    and superscripts. Any unparsable value falls back to the default, because a
    raise would escape the caller's cleanup and let registered buffers unwind.
    """
    try:
        raw = getenv("YR_TE_HIXL_READ_LEASE_TTL_MS")
        if raw is None:
            return DEFAULT_READ_LEASE_TTL_MS / 1000.0
        text = str(raw)
        if not text or any(ch not in _ASCII_DIGITS for ch in text):
            return DEFAULT_READ_LEASE_TTL_MS / 1000.0
        value = 0
        for ch in text:
            value = value * 10 + ord(ch) - ord("0")
            if value > _INT32_MAX:
                return DEFAULT_READ_LEASE_TTL_MS / 1000.0
        if value == 0:
            return DEFAULT_READ_LEASE_TTL_MS / 1000.0
        return value / 1000.0
    except BaseException:
        return DEFAULT_READ_LEASE_TTL_MS / 1000.0


def flush_queue_before_exit(queue, timeout_seconds: float = 10.0) -> bool:
    """Push a queue's pending payloads into the pipe before a possible os._exit.

    os._exit skips the feeder thread flush, which would silently drop a worker's
    diagnostic payload. The join is bounded and run off-thread: a payload larger
    than the pipe buffer only completes once the parent drains it, and a parent
    that never drains must not wedge the worker here.
    """
    queue.close()
    joiner = threading.Thread(target=queue.join_thread, daemon=True)
    joiner.start()
    joiner.join(timeout_seconds)
    return not joiner.is_alive()


def finalize_for_cleanup(engine, role: str, not_ready_code, *, extra_wait_seconds: float = 5.0,
                         ttl_seconds: float = None, retry_delay_seconds: float = 0.1,
                         exit_func: Callable[[int], None] = _DEFAULT_EXIT,
                         monotonic_func: Callable[[], float] = time.monotonic,
                         sleep_func: Callable[[float], None] = time.sleep) -> bool:
    """Finalize before the caller allows device-backed buffers to leave scope.

    A cleanup that does not reach a successful finalize terminates the owning
    process through ``exit_func`` and never returns normally. The default is
    os._exit so Python does not destruct device tensors while the transfer engine
    may still hold their registered addresses.

    Each finalize blocks up to K_FINALIZE_LEASE_WAIT_TIMEOUT_MS draining read
    leases, so retries are bounded by a deadline derived from the configured
    lease TTL rather than by a call count: a lost release RPC has to be waited
    out until the lease expires, and the TTL is operator-configurable.
    """
    def terminate(message):
        try:
            logging.error(message)
        finally:
            exit_func(1)
        return False

    try:
        window = read_lease_ttl_seconds() if ttl_seconds is None else ttl_seconds
        deadline = monotonic_func() + window + extra_wait_seconds
    except BaseException as exc:
        return terminate(f"[ERROR] {role} cleanup window unresolved: {exc}")

    while True:
        try:
            rc = engine.finalize()
            if rc.is_ok():
                return True
            if rc.get_code() == not_ready_code and monotonic_func() < deadline:
                sleep_func(retry_delay_seconds)
                continue
            message = f"[ERROR] {role} finalize failed: {rc.to_string()}"
        except BaseException as exc:  # Cleanup interruption must not release registered tensors.
            message = f"[ERROR] {role} finalize interrupted: {exc}"
        return terminate(message)
