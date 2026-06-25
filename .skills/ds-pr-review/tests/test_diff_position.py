#!/usr/bin/env python3
from __future__ import annotations

import unittest

from script_imports import load_script_module


diff_position = load_script_module("diff_position")
absolute_line_for_position = diff_position.absolute_line_for_position
find_position = diff_position.find_position
parse_patch = diff_position.parse_patch


PATCH = """@@ -26,6 +26,7 @@
 #include "datasystem/common/flags/flags.h"
 #include "datasystem/common/log/spdlog/log_message.h"
 #include "datasystem/common/log/spdlog/log_param.h"
+#include "datasystem/common/log/spdlog/log_sampler.h"
<blank>
 DS_DECLARE_int32(v);
 DS_DECLARE_int32(minloglevel);
@@ -49,3 +50,6 @@
 // Basic Logging Macros
-#define LOG(severity) LOG_IF(severity, FLAGS_minloglevel <= DS_LOGS_LEVEL_##severity)
+#define LOG(severity) \\
+    if (DS_LOGS_LEVEL_##severity >= DS_LOGS_LEVEL_ERROR || \\
+        (FLAGS_minloglevel <= DS_LOGS_LEVEL_##severity && datasystem::g_IsPrintLog)) \\
+    LOG_IMPL(severity)
""".replace("\n<blank>\n", "\n \n")


class DiffPositionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.position_map = parse_patch(PATCH)

    def test_find_position_by_diff_index_resolves_to_absolute_new_line(self) -> None:
        entry = find_position(self.position_map, diff_line_index=10)
        self.assertIsNotNone(entry)
        self.assertEqual(absolute_line_for_position(entry), 51)

    def test_find_position_by_line_prefers_changed_new_line(self) -> None:
        entry = find_position(self.position_map, line=51)
        self.assertIsNotNone(entry)
        self.assertEqual(entry["line_type"], "add")
        self.assertEqual(absolute_line_for_position(entry), 51)

    def test_deleted_line_falls_back_to_old_line(self) -> None:
        entry = find_position(self.position_map, diff_line_index=9)
        self.assertIsNotNone(entry)
        self.assertEqual(entry["line_type"], "delete")
        self.assertEqual(absolute_line_for_position(entry), 50)


if __name__ == "__main__":
    unittest.main()
