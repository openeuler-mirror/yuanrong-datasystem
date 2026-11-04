# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Resource log columns remain aligned after removing the object ETCD queue."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "generate_resource_report.py"
spec = importlib.util.spec_from_file_location("resource_report_schema", SCRIPT)
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)


class ResourceReportSchemaTest(unittest.TestCase):
    def parse_record(self, legacy, etcd_rate="0.9"):
        parts = ["2026-01-01T00:00:00", "I", "source", "worker", "pid", "trace", "record"]
        parts += ["1/2/3/0.5/0/0", "4/5/6/0.7", "8", "9", "10"]
        parts += ["1/2/3/4/0.5"] * 4
        if legacy:
            parts += ["0/0/0"]
        parts += [etcd_rate, "0.8", "5/6/7/8/0.4", "11"]
        parts += ["0/0/0/0/0"] * 4
        parts += ["0.6", "12/13/14/0.2", "15/16/17/18/19"]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "resource.log"
            path.write_text("|".join(parts) + "\n")
            return report.parse_resource_logs({"worker": [str(path)]})["worker"][0][1]

    def test_current_columns(self):
        metrics = self.parse_record(False)
        self.assertEqual(metrics["etcd_success_rate"], 0.9)
        self.assertEqual(metrics["obs_success_rate"], 0.8)
        self.assertEqual(metrics["tp_async_maxRunning"], 5)
        self.assertEqual(metrics["stream_count"], 11)
        self.assertEqual(metrics["sc_cache_usage"], 12)
        self.assertEqual(metrics["oc_miss"], 19)

    def test_unavailable_etcd_rate_keeps_current_columns(self):
        metrics = self.parse_record(False, "-1")
        self.assertEqual(metrics["etcd_success_rate"], -1)
        self.assertEqual(metrics["obs_success_rate"], 0.8)
        self.assertEqual(metrics["stream_count"], 11)

    def test_legacy_queue_column_does_not_shift_metrics(self):
        self.assertEqual(self.parse_record(True), self.parse_record(False))


if __name__ == "__main__":
    unittest.main()
