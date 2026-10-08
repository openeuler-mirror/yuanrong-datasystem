"""NUMA production scans gzip members once without conflating membership and file counts."""
import io
import tarfile
from unittest.mock import Mock

import pytest

from test_ds_trace_numa_analysis import load_module, write_run_contract


def test_build_analysis_opens_gzip_archive_once(tmp_path, monkeypatch):
    module = load_module()
    run_dir, model, archive = write_run_contract(tmp_path)
    opening = Mock(wraps=module.tarfile.open)
    monkeypatch.setattr(module.tarfile, 'open', opening)
    monkeypatch.setattr(tarfile.TarFile, 'extractfile', Mock(side_effect=AssertionError('member content read')))
    result = module.build_analysis(run_dir, model, archive, {'head': 'test'})
    assert result['aggregate']['unique_trace_count'] == 4
    assert result['aggregate']['archive_member_trace_files'] == 6
    assert opening.call_count == 1
    assert opening.call_args.args == (archive, 'r:*')


def test_uncompressed_tar_is_supported_when_inventory_cannot_be_reused(tmp_path):
    module = load_module()
    archive = tmp_path / 'members.tar'
    with tarfile.open(archive, 'w') as stream:
        member = tarfile.TarInfo('time/GET_5000/trace-a')
        member.size = 1
        stream.addfile(member, io.BytesIO(b'x'))

    assert module._scan_archive_cohorts(archive) == ({'trace-a': {'time/GET_5000'}}, 1)


def test_numa_analysis_accepts_uncompressed_tar_without_member_inventory(tmp_path):
    module = load_module()
    run_dir, model, compressed = write_run_contract(tmp_path)
    plain = tmp_path / 'input.tar'
    with tarfile.open(compressed, 'r:gz') as source, tarfile.open(plain, 'w') as target:
        for member in source:
            if member.isfile():
                target.addfile(member, source.extractfile(member))

    result = module.build_analysis(run_dir, model, plain, {'head': 'test'})

    assert result['aggregate']['unique_trace_count'] == 4
    assert result['aggregate']['archive_member_trace_files'] == 6


def test_scan_preserves_duplicate_members_and_cohort_filters(tmp_path):
    module = load_module()
    path = tmp_path / 'members.tar.gz'
    names = ['time/GET_5000/trace', 'time/GET_5000/trace', 'time/GET_5000/trace_1',
             'core/1004/trace', 'core/1004/unique_traces_1004.txt', 'trace']
    with tarfile.open(path, 'w:gz') as archive:
        for name in names:
            member = tarfile.TarInfo(name)
            member.size = 1
            archive.addfile(member, io.BytesIO(b'x'))
        for kind in (tarfile.DIRTYPE, tarfile.SYMTYPE, tarfile.LNKTYPE):
            member = tarfile.TarInfo('core/1004/non-file-' + kind.decode())
            member.type = kind
            member.linkname = 'core/1004/trace'
            archive.addfile(member)
    cohorts, count = module._scan_archive_cohorts(path)
    assert cohorts == {'trace': {'time/GET_5000', 'core/1004'}}
    assert count == 4
    assert module.build_cohort_index(path) == cohorts
    assert module.count_archive_trace_files(path) == count


def test_corrupted_gzip_still_raises(tmp_path):
    module = load_module()
    path = tmp_path / 'broken.tar.gz'
    path.write_bytes(b'not gzip')
    with pytest.raises(tarfile.ReadError):
        module._scan_archive_cohorts(path)
