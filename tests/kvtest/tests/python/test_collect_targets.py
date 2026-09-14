import argparse
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import log_collect


class TestCollectTargets(unittest.TestCase):
    def test_host_exclusions_preserve_remaining_nodes_and_input(self):
        nodes = [dict(name='worker-' + str(i), host_ip='192.0.2.' + str(i % 100 + 1)) for i in range(500)]
        result = log_collect.filter_collect_targets(nodes, dict(host_ips=[], exclude_host_ips=['192.0.2.49']))
        self.assertEqual(len(result), 495)
        self.assertTrue(all(n['host_ip'] != '192.0.2.49' for n in result))
        self.assertEqual(len(nodes), 500)

    def test_include_and_exclude_use_exact_ip_with_exclusion_winning(self):
        nodes = [dict(host_ip=ip) for ip in ['192.0.2.1', '192.0.2.10', '192.0.2.2']]
        result = log_collect.filter_collect_targets(nodes, dict(host_ips=['192.0.2.1', '192.0.2.2'],
                                                                exclude_host_ips=['192.0.2.2']))
        self.assertEqual(result, nodes[:1])

    def test_options_validate_addresses_and_keep_default_unfiltered(self):
        parser = argparse.ArgumentParser()
        log_collect.add_collect_filters(parser)
        self.assertIsNone(log_collect.host_selection_from_args(parser.parse_args([])))
        args = parser.parse_args(['--host-ip', '192.0.2.1', '--host-ip', '192.0.2.2',
                                  '--exclude-host-ip', '192.0.2.2'])
        self.assertEqual(log_collect.host_selection_from_args(args)['host_ips'], ['192.0.2.1', '192.0.2.2'])
        with self.assertRaises(ValueError):
            log_collect.host_selection_from_args(parser.parse_args(['--host-ip', '192.0.2']))


    def test_json_host_filter_merges_with_cli_and_rejects_typos(self):
        import json
        import tempfile
        parser = argparse.ArgumentParser()
        log_collect.add_collect_filters(parser)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'hosts.json'
            path.write_text(json.dumps({'include': ['192.0.2.1'], 'exclude': ['192.0.2.2']}))
            args = parser.parse_args(['--host-filter', str(path), '--host-ip', '192.0.2.3'])
            self.assertEqual(log_collect.host_selection_from_args(args),
                             dict(host_ips=['192.0.2.1', '192.0.2.3'], exclude_host_ips=['192.0.2.2']))
            for invalid in ({'excldue': ['192.0.2.1']}, {'exclude': '192.0.2.1'}, {'include': [123]}, []):
                path.write_text(json.dumps(invalid))
                with self.assertRaises(ValueError):
                    log_collect.host_selection_from_args(args)


    def test_worker_filters_hosts_before_prefix_and_batch(self):
        import deploy_worker
        pods = [dict(name=name, host_ip=host, ip='198.51.100.1') for name, host in
                [('worker-bad', '192.0.2.1'), ('other', '192.0.2.2'),
                 ('worker-a', '192.0.2.2'), ('worker-b', '192.0.2.2')]]
        with patch.object(sys, 'argv', ['deploy_worker.py', 'collect', '-p', 'worker-',
                                       '--exclude-host-ip', '192.0.2.1', '--count', '1', '--offset', '1']), \
             patch('deploy_worker.get_pods', return_value=pods), \
             patch('deploy_worker.cmd_collect', return_value=0) as collect:
            self.assertEqual(deploy_worker.main(), 0)
            self.assertEqual(collect.call_args.args[1], pods[3:])

    def test_client_uses_live_host_ip_before_prefix_and_batch(self):
        import tempfile
        from deploy_client import Deployer
        d = Deployer.__new__(Deployer)
        d.nodes = [dict(pod_name=name, instance_id=str(i), host_ip='192.0.2.99')
                   for i, name in enumerate(['client-bad', 'other', 'client-a', 'client-b'])]
        d.remote_work_dir = '/tmp/kvtest'
        d.listen_port = 9000
        addresses = {node['pod_name']: dict(ip='198.51.100.1', host_ip='192.0.2.1' if i == 0 else '192.0.2.2')
                     for i, node in enumerate(d.nodes)}
        with tempfile.TemporaryDirectory() as tmp, \
             patch.object(d, '_transport', return_value='kubectl'), \
             patch.object(d, '_namespace', return_value='default'), \
             patch.object(d, 'run_on') as remote, \
             patch('deploy_client.read_pod_addresses', return_value=addresses) as read, \
             patch('deploy_client.receive_archive', return_value=1) as receive:
            self.assertEqual(d.do_collect(output_dir=tmp, prefixes=['client-'], node_slice=(1, 1),
                             filters=dict(patterns=['*.log'], keywords=[], uncompressed_only=False),
                             host_selection=dict(exclude_host_ips=['192.0.2.1'])), 0)
            self.assertEqual(receive.call_args.args[0][2], 'client-b')
            read.assert_called_once_with('default')
            remote.assert_not_called()

    def test_unknown_host_ip_is_not_silently_included(self):
        with self.assertRaises(ValueError):
            log_collect.filter_collect_targets([dict(name='missing')], dict(exclude_host_ips=['192.0.2.1']))


if __name__ == '__main__':
    unittest.main()
