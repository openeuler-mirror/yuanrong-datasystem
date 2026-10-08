import json
import subprocess
import sys
import textwrap
import unittest

from .cleanup import DEFAULT_READ_LEASE_TTL_MS, finalize_for_cleanup, read_lease_ttl_seconds


class FakeResult:
    def __init__(self, code, text):
        self.code = code
        self.text = text

    def is_ok(self):
        return self.code == "ok"

    def get_code(self):
        return self.code

    def to_string(self):
        return self.text


class FakeEngine:
    def __init__(self, results, trailing=None):
        self.results = list(results)
        self.trailing = trailing
        self.finalize_calls = 0

    def finalize(self):
        self.finalize_calls += 1
        if self.results:
            return self.results.pop(0)
        if self.trailing is None:
            raise AssertionError("finalize called more times than the test scripted")
        return self.trailing


class FakeClock:
    """Advances one finalize-worth of blocking time per read."""

    def __init__(self, step_seconds):
        self.step_seconds = step_seconds
        self.now = 0.0

    def monotonic(self):
        value = self.now
        self.now += self.step_seconds
        return value


class FakeAllocation:
    def __init__(self):
        self.released = False

    def release(self):
        self.released = True


class CleanupLifecycleTest(unittest.TestCase):
    def test_default_failure_exit_does_not_run_buffer_destructors(self):
        script = textwrap.dedent("""
            from tests.python.cleanup import finalize_for_cleanup
            class Engine:
                def finalize(self):
                    raise RuntimeError("injected failure")
            class Buffer:
                def __del__(self):
                    print("buffer-destructor-ran", flush=True)
            def run():
                buffer = Buffer()
                try:
                    pass
                finally:
                    finalize_for_cleanup(Engine(), "fake-owner", "not_ready")
            run()
        """)
        result = subprocess.run([sys.executable, "-B", "-c", script], capture_output=True,
                                text=True, timeout=5, check=False)
        self.assertEqual(result.returncode, 1)
        self.assertNotIn("buffer-destructor-ran", result.stdout)
        self.assertIn("injected failure", result.stderr)

    def test_not_ready_retry_completes_before_buffers_are_released(self):
        engine = FakeEngine([
            FakeResult("not_ready", "active lease"),
            FakeResult("ok", ""),
        ])
        allocation = FakeAllocation()
        sleeps = []
        exits = []

        finalized = finalize_for_cleanup(
            engine,
            "fake-requester",
            "not_ready",
            retry_delay_seconds=0.25,
            sleep_func=sleeps.append,
            exit_func=exits.append,
        )
        if finalized:
            allocation.release()

        self.assertTrue(finalized)
        self.assertEqual(engine.finalize_calls, 2)
        self.assertEqual(sleeps, [0.25])
        self.assertEqual(exits, [])
        self.assertTrue(allocation.released)

    def test_retry_window_covers_a_long_configured_lease_ttl(self):
        # Each finalize blocks 30s draining leases; a 90s TTL must not be cut short.
        clock = FakeClock(step_seconds=30.0)
        engine = FakeEngine([FakeResult("not_ready", "active lease")] * 3,
                            trailing=FakeResult("ok", ""))
        exits = []

        finalized = finalize_for_cleanup(
            engine,
            "fake-owner",
            "not_ready",
            ttl_seconds=90.0,
            sleep_func=lambda _: None,
            monotonic_func=clock.monotonic,
            exit_func=exits.append,
        )

        self.assertTrue(finalized)
        self.assertGreater(engine.finalize_calls, 2)
        self.assertEqual(exits, [])

    def test_deadline_expiry_terminates_without_release(self):
        clock = FakeClock(step_seconds=30.0)
        engine = FakeEngine([], trailing=FakeResult("not_ready", "active lease"))
        allocation = FakeAllocation()
        exits = []

        finalized = finalize_for_cleanup(
            engine,
            "fake-owner",
            "not_ready",
            ttl_seconds=30.0,
            sleep_func=lambda _: None,
            monotonic_func=clock.monotonic,
            exit_func=exits.append,
        )
        if finalized:
            allocation.release()

        self.assertFalse(finalized)
        self.assertEqual(exits, [1])
        self.assertFalse(allocation.released)

    def test_fatal_finalize_terminates_before_buffer_release(self):
        engine = FakeEngine([FakeResult("runtime_error", "backend unavailable")])
        allocation = FakeAllocation()
        exits = []
        observed = []

        def exit_func(code):
            observed.append((code, allocation.released))
            exits.append(code)

        finalized = finalize_for_cleanup(
            engine,
            "fake-owner",
            "not_ready",
            exit_func=exit_func,
        )
        if finalized:
            allocation.release()

        self.assertFalse(finalized)
        self.assertEqual(engine.finalize_calls, 1)
        self.assertEqual(exits, [1])
        self.assertEqual(observed, [(1, False)])
        self.assertFalse(allocation.released)

    def test_finalize_exception_terminates_without_release(self):
        class RaisingEngine:
            @staticmethod
            def finalize():
                raise RuntimeError("finalize crashed")

        allocation = FakeAllocation()
        exits = []
        finalized = finalize_for_cleanup(
            RaisingEngine(),
            "fake-owner",
            "not_ready",
            exit_func=exits.append,
        )
        if finalized:
            allocation.release()

        self.assertFalse(finalized)
        self.assertEqual(exits, [1])
        self.assertFalse(allocation.released)

    def test_retry_interruption_terminates_instead_of_unwinding(self):
        engine = FakeEngine([FakeResult("not_ready", "active lease")])
        exits = []

        def interrupted_sleep(_):
            raise KeyboardInterrupt()

        self.assertFalse(finalize_for_cleanup(engine, "fake-owner", "not_ready",
                                              sleep_func=interrupted_sleep, exit_func=exits.append))
        self.assertEqual(exits, [1])

    def test_non_positive_window_still_terminates_instead_of_returning(self):
        for ttl in (0.0, -5.0):
            engine = FakeEngine([], trailing=FakeResult("not_ready", "active lease"))
            exits = []
            finalized = finalize_for_cleanup(
                engine,
                "fake-owner",
                "not_ready",
                ttl_seconds=ttl,
                extra_wait_seconds=ttl,
                sleep_func=lambda _: None,
                exit_func=exits.append,
            )
            self.assertFalse(finalized)
            self.assertEqual(engine.finalize_calls, 1)
            self.assertEqual(exits, [1], f"ttl={ttl} returned without terminating")


class WorkerCleanupRegressionTest(unittest.TestCase):
    def test_workers_finalize_when_queue_flush_is_interrupted(self):
        script = textwrap.dedent("""
            import json
            import sys
            import types

            class Result:
                def is_error(self):
                    return False

                def is_ok(self):
                    return True

                def get_code(self):
                    return "ok"

                def to_string(self):
                    return ""

            class ErrorResult(Result):
                def is_error(self):
                    return True

                def is_ok(self):
                    return False

                def get_code(self):
                    return "runtime_error"

                def to_string(self):
                    return "injected transfer failure"

            class ErrorCode:
                kNotReady = "not_ready"

            class Engine:
                last = None

                def __init__(self):
                    self.finalize_calls = 0
                    self.register_calls = 0
                    self.unregister_calls = 0
                    Engine.last = self

                def initialize(self, *_args):
                    return Result()

                def batch_register_memory(self, *_args):
                    self.register_calls += 1
                    return Result()

                def batch_transfer_sync_read(self, *_args):
                    return ErrorResult()

                def batch_unregister_memory(self, *_args):
                    self.unregister_calls += 1
                    return Result()

                def finalize(self):
                    self.finalize_calls += 1
                    return Result()

            class Tensor:
                next_address = 1000

                def __init__(self):
                    self.address = Tensor.next_address
                    Tensor.next_address += 1

                def data_ptr(self):
                    return self.address

            torch = types.ModuleType("torch")
            torch.uint8 = object()
            torch.device = lambda name: name
            torch.full = lambda *_args, **_kwargs: Tensor()
            torch.zeros = lambda *_args, **_kwargs: Tensor()
            torch.npu = types.SimpleNamespace(synchronize=lambda _device: None)
            torch_npu = types.ModuleType("torch_npu")
            yr = types.ModuleType("yr")
            yr.__path__ = []
            datasystem = types.ModuleType("yr.datasystem")
            datasystem.ErrorCode = ErrorCode
            datasystem.MemoryRegistration = object
            datasystem.TransferEngine = Engine
            yr.datasystem = datasystem
            sys.modules.update({
                "torch": torch,
                "torch_npu": torch_npu,
                "yr": yr,
                "yr.datasystem": datasystem,
            })

            from tests.python import cleanup
            from tests.python.st import test_python_api_st as st

            class Queue:
                def __init__(self):
                    self.items = []
                    self.closed = False

                def put(self, item):
                    self.items.append(item)

                def close(self):
                    self.closed = True

                def join_thread(self):
                    pass

            class StopEvent:
                def wait(self, timeout=None):
                    pass

            def run_worker(worker_name, failure):
                queue = Queue()
                original_start = cleanup.threading.Thread.start
                original_join = cleanup.threading.Thread.join
                if failure == "start":
                    def injected_start(_thread):
                        raise RuntimeError("injected Thread.start failure")

                    injected_join = original_join
                else:
                    def injected_start(_thread):
                        pass

                    def injected_join(_thread, timeout=None):
                        raise KeyboardInterrupt("injected Thread.join interruption")

                cleanup.threading.Thread.start = injected_start
                cleanup.threading.Thread.join = injected_join
                caught = None
                try:
                    if worker_name == "owner":
                        st._owner_worker("owner", 0, 4, 1, queue, StopEvent())
                    else:
                        st._requester_worker("requester", 1, "owner", [100], [4], [17], queue)
                except BaseException as exc:
                    caught = exc
                finally:
                    cleanup.threading.Thread.start = original_start
                    cleanup.threading.Thread.join = original_join
                engine = Engine.last
                return {
                    "exception_type": type(caught).__name__ if caught else None,
                    "finalize_calls": engine.finalize_calls,
                    "register_calls": engine.register_calls,
                    "unregister_calls": engine.unregister_calls,
                    "queue_closed": queue.closed,
                }

            results = {}
            for worker_name in ("owner", "requester"):
                for failure in ("start", "join"):
                    results[worker_name + ":" + failure] = run_worker(worker_name, failure)
            print("RESULT=" + json.dumps(results, sort_keys=True))
        """)
        result = subprocess.run([sys.executable, "-B", "-c", script], capture_output=True,
                                text=True, timeout=5, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        marker = "RESULT="
        self.assertIn(marker, result.stdout)
        results = json.loads(result.stdout.split(marker, 1)[1])
        for worker_name in ("owner", "requester"):
            for failure, exception_type in (("start", "RuntimeError"),
                                            ("join", "KeyboardInterrupt")):
                with self.subTest(worker=worker_name, failure=failure):
                    observed = results[worker_name + ":" + failure]
                    self.assertEqual(observed["exception_type"], exception_type)
                    self.assertEqual(observed["finalize_calls"], 1)
                    self.assertEqual(observed["register_calls"], 1)
                    self.assertEqual(observed["unregister_calls"], 0)
                    self.assertTrue(observed["queue_closed"])


class ReadLeaseTtlTest(unittest.TestCase):
    def test_unset_and_malformed_values_fall_back_to_the_default(self):
        default = DEFAULT_READ_LEASE_TTL_MS / 1000.0
        for raw in (None, "", "abc", "-1", "12.5", "0", " 30000", "30000 ", str(2 ** 31)):
            self.assertEqual(read_lease_ttl_seconds(lambda _, v=raw: v), default, f"raw={raw!r}")

    def test_non_ascii_digits_fall_back_like_the_cpp_parser(self):
        # AscendBackend::GetEnvI32 walks bytes and rejects anything outside ASCII '0'-'9';
        # str.isdigit would accept all of these, two of them raising inside int().
        # Failure messages go through ascii() so they print on a non-UTF-8 console.
        default = DEFAULT_READ_LEASE_TTL_MS / 1000.0
        for raw in ("٣٠٠٠٠", "۳۰", "\xb2", "\xb9\xb2"):
            self.assertEqual(read_lease_ttl_seconds(lambda _, v=raw: v), default,
                             f"raw={ascii(raw)}")

    def test_oversized_digit_strings_fall_back_instead_of_raising(self):
        # The parser must remain bounded by INT32_MAX without relying on int(), whose
        # conversion limit varies by Python version.
        digit_limit = getattr(sys, "get_int_max_str_digits", lambda: 4300)()
        default = DEFAULT_READ_LEASE_TTL_MS / 1000.0
        for length in (400, digit_limit + 100):
            self.assertEqual(read_lease_ttl_seconds(lambda _, v="9" * length: v), default,
                             f"length={length}")

    def test_long_leading_zeros_preserve_cpp_parser_semantics(self):
        prefix = "0" * 5000
        default = DEFAULT_READ_LEASE_TTL_MS / 1000.0
        cases = (
            (prefix + "90000", 90.0),
            (prefix + str(2 ** 31 - 1), (2 ** 31 - 1) / 1000.0),
            (prefix + str(2 ** 31), default),
        )
        for raw, expected in cases:
            self.assertEqual(read_lease_ttl_seconds(lambda _, v=raw: v), expected,
                             f"raw length={len(raw)}")

    def test_getenv_failure_falls_back_instead_of_raising(self):
        def exploding_getenv(_):
            raise RuntimeError("env unavailable")

        self.assertEqual(read_lease_ttl_seconds(exploding_getenv),
                         DEFAULT_READ_LEASE_TTL_MS / 1000.0)

    def test_valid_override_is_honoured(self):
        self.assertEqual(read_lease_ttl_seconds(lambda _: "90000"), 90.0)


class CleanupWindowResolutionTest(unittest.TestCase):
    def test_unresolvable_window_terminates_instead_of_propagating(self):
        engine = FakeEngine([], trailing=FakeResult("ok", ""))
        exits = []

        def exploding_monotonic():
            raise RuntimeError("clock unavailable")

        finalized = finalize_for_cleanup(
            engine,
            "fake-owner",
            "not_ready",
            monotonic_func=exploding_monotonic,
            exit_func=exits.append,
        )

        self.assertFalse(finalized)
        self.assertEqual(engine.finalize_calls, 0)
        self.assertEqual(exits, [1])


if __name__ == "__main__":
    unittest.main()
