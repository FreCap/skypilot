"""Unit tests for skylet log_lib."""

from io import StringIO
import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

import psutil

from sky.skylet import log_lib


class TestLogBuffer(unittest.TestCase):
    """Test cases for LogBuffer class."""

    def test_initialization(self):
        """Test buffer initializes with correct defaults."""
        buffer = log_lib.LogBuffer()

        self.assertEqual(buffer.max_chars, log_lib.DEFAULT_LOG_CHUNK_SIZE)
        self.assertIsInstance(buffer._buffer, StringIO)
        self.assertEqual(buffer._buffer.getvalue(), '')

    def test_custom_parameters(self):
        """Test buffer initializes with custom parameters."""
        buffer = log_lib.LogBuffer(max_chars=1024)
        self.assertEqual(buffer.max_chars, 1024)

    def test_write_basic(self):
        """Test adding a single line to buffer."""
        buffer = log_lib.LogBuffer(max_chars=100)

        string = "Hello world\n"
        should_flush = buffer.write(string)

        self.assertFalse(should_flush)
        self.assertEqual(buffer._buffer.tell(), len(string))
        self.assertEqual(buffer._buffer.getvalue(), string)

    def test_write_triggers_size_flush(self):
        """Test that buffer flushes when size limit is reached."""
        buffer = log_lib.LogBuffer(max_chars=10)

        # Add a line that exceeds the size limit
        string = "This is a very long line that exceeds the buffer size\n"
        should_flush = buffer.write(string)

        self.assertTrue(should_flush)
        self.assertEqual(buffer._buffer.tell(), len(string))

    def test_flush_basic(self):
        """Test getting chunk from buffer."""
        buffer = log_lib.LogBuffer()

        buffer.write("Line 1\n")
        buffer.write("Line 2\n")
        buffer.write("Line 3\n")

        chunk = buffer.flush()

        self.assertEqual(chunk, "Line 1\nLine 2\nLine 3\n")
        self.assertEqual(buffer._buffer.tell(), 0)

    def test_flush_empty(self):
        """Test getting chunk from empty buffer."""
        buffer = log_lib.LogBuffer()

        chunk = buffer.flush()

        self.assertEqual(chunk, "")

    def test_unicode_characters(self):
        """Test buffer handles unicode characters correctly."""
        buffer = log_lib.LogBuffer()

        unicode_line = "Hello 🌍\n"
        buffer.write(unicode_line)

        # _buffer.tell() counts the number of characters,
        # not the number of bytes:
        # >>> len(unicode_line)
        # 8
        # >>> len(unicode_line.encode('utf-8'))
        # 11
        #
        # This is fine because our default chunk size is well below the
        # default grpc.max_receive_message_length which is 4MB.
        self.assertEqual(buffer._buffer.tell(), len(unicode_line))

        chunk = buffer.flush()
        self.assertEqual(chunk, unicode_line)

    def test_reset_after_flush(self):
        """Test that buffer is properly reset after getting chunk."""
        buffer = log_lib.LogBuffer()

        buffer.write("Line 1\n")
        buffer.write("Line 2\n")

        # Get chunk should reset everything
        chunk = buffer.flush()

        self.assertEqual(chunk, "Line 1\nLine 2\n")
        self.assertEqual(buffer._buffer.tell(), 0)


class TestRunWithLogTimeout(unittest.TestCase):
    """Test cases for run_with_log timeout functionality."""

    def test_process_stream_timeout_exceeded(self):
        """Test that timeout works with process_stream=True."""
        with tempfile.NamedTemporaryFile(suffix='.log', delete=False) as f:
            log_path = f.name

        # Command that sleeps longer than timeout
        cmd = ['sleep', '10']
        with self.assertRaises(subprocess.TimeoutExpired):
            log_lib.run_with_log(
                cmd,
                log_path,
                process_stream=True,
                timeout=1,
            )

    def test_process_stream_timeout_not_exceeded(self):
        """Test normal completion with process_stream=True and timeout set."""
        with tempfile.NamedTemporaryFile(suffix='.log', delete=False) as f:
            log_path = f.name

        # Command that completes quickly
        cmd = ['echo', 'hello']
        returncode = log_lib.run_with_log(
            cmd,
            log_path,
            process_stream=True,
            timeout=10,
        )
        self.assertEqual(returncode, 0)

    def test_no_stream_timeout_exceeded(self):
        """Test that timeout works with process_stream=False."""
        with tempfile.NamedTemporaryFile(suffix='.log', delete=False) as f:
            log_path = f.name

        # Command that sleeps longer than timeout
        cmd = ['sleep', '10']
        with self.assertRaises(subprocess.TimeoutExpired):
            log_lib.run_with_log(
                cmd,
                log_path,
                process_stream=False,
                timeout=1,
            )

    def test_no_stream_timeout_not_exceeded(self):
        """Test normal completion with process_stream=False and timeout set."""
        with tempfile.NamedTemporaryFile(suffix='.log', delete=False) as f:
            log_path = f.name

        # Command that completes quickly
        cmd = ['echo', 'hello']
        returncode = log_lib.run_with_log(
            cmd,
            log_path,
            process_stream=False,
            timeout=10,
        )
        self.assertEqual(returncode, 0)

    def test_bounded_capture_drains_large_stdout_and_stderr_without_helpers(
            self):
        cmd = [
            sys.executable, '-c', 'import sys; '
            'sys.stdout.write("o" * 131072); '
            'sys.stderr.write("e" * 131072)'
        ]
        capture = log_lib.BoundedSubprocessCapture(
            deadline_monotonic=time.monotonic() + 5,
            max_output_bytes=512 * 1024)
        with mock.patch.object(log_lib.subprocess_utils,
                               'kill_process_daemon') as watcher, \
             mock.patch.object(log_lib.threading, 'Timer') as timer, \
             mock.patch.object(log_lib.multiprocessing.pool,
                               'ThreadPool') as thread_pool:
            returncode, stdout, stderr = log_lib.run_with_log(
                cmd,
                os.devnull,
                require_outputs=True,
                process_stream=False,
                bounded_capture=capture)

        self.assertEqual(returncode, 0)
        self.assertEqual(len(stdout), 131072)
        self.assertEqual(len(stderr), 131072)
        watcher.assert_not_called()
        timer.assert_not_called()
        thread_pool.assert_not_called()

    def test_bounded_capture_timeout_kills_child_and_grandchild(self):
        with tempfile.NamedTemporaryFile(delete=False) as pid_file:
            pid_path = pid_file.name
        cmd = [
            sys.executable, '-c',
            ('import pathlib, subprocess, sys, time; '
             'child = subprocess.Popen(["sleep", "60"]); '
             'pathlib.Path(sys.argv[1]).write_text(str(child.pid)); '
             'time.sleep(60)'), pid_path
        ]
        started_at = time.monotonic()
        try:
            with self.assertRaises(subprocess.TimeoutExpired):
                log_lib.run_with_log(
                    cmd,
                    os.devnull,
                    require_outputs=True,
                    process_stream=False,
                    bounded_capture=log_lib.BoundedSubprocessCapture(
                        deadline_monotonic=time.monotonic() + 0.5,
                        max_output_bytes=1024))
            self.assertLess(time.monotonic() - started_at, 2.5)
            child_pid = int(open(pid_path, encoding='utf-8').read())
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                try:
                    child = psutil.Process(child_pid)
                    if (not child.is_running() or
                            child.status() == psutil.STATUS_ZOMBIE):
                        break
                except psutil.NoSuchProcess:
                    break
                time.sleep(0.02)
            else:
                self.fail('Bounded capture left its grandchild running.')
        finally:
            os.unlink(pid_path)

    def test_bounded_capture_output_limit_fails_closed(self):
        cmd = [sys.executable, '-c', 'print("x" * 1048576)']
        with self.assertRaises(log_lib.SubprocessOutputLimitExceeded):
            log_lib.run_with_log(
                cmd,
                os.devnull,
                require_outputs=True,
                process_stream=False,
                bounded_capture=log_lib.BoundedSubprocessCapture(
                    deadline_monotonic=time.monotonic() + 5,
                    max_output_bytes=64 * 1024))


if __name__ == '__main__':
    unittest.main()
