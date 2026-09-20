"""Shared selective collection for the existing deployment CLIs."""

import json
import ipaddress
import logging
import os
import re
import shlex
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path, PurePosixPath


REMOTE_ARCHIVE = r"""
import bz2, fnmatch, gzip, json, lzma, os, stat, sys, tarfile, tempfile
cfg = json.loads(sys.argv[1])
keywords = [word.encode('utf-8') for word in cfg['keywords']]
compressed = ('.gz', '.bz2', '.xz', '.zip', '.zst', '.lz4', '.tgz', '.tar', '.7z', '.rar', '.z')
with tarfile.open(fileobj=sys.stdout.buffer, mode='w|gz' if cfg.get('compress', True) else 'w|') as archive:
    for entry in cfg['sources']:
        label, root, defaults = entry[:3]
        recursive = entry[3] if len(entry) > 3 else True
        if not root:
            raise ValueError('Collection source must not be empty')
        if not os.path.exists(root):
            continue
        if not os.path.isdir(root):
            raise ValueError('Not a directory: ' + root)
        def walk_error(error):
            raise error
        for parent, dirs, files in os.walk(root, followlinks=False, onerror=walk_error):
            dirs.sort()
            if not recursive:
                dirs[:] = []
            for name in sorted(files):
                if name in ('env', 'procmon.py'):
                    continue
                path = os.path.join(parent, name)
                rel = os.path.relpath(path, root).replace(os.sep, '/')
                def matches(patterns):
                    return any(fnmatch.fnmatchcase(rel if '/' in p else name, p) for p in patterns)
                if not matches(cfg['patterns'] or defaults) or (not recursive and not matches(defaults)):
                    continue
                lower = name.lower()
                packed = lower.endswith(compressed)
                if cfg['uncompressed_only'] and packed:
                    continue
                if not stat.S_ISREG(os.lstat(path).st_mode):
                    continue
                arcname = label + '/' + rel
                if not keywords:
                    archive.add(path, arcname=arcname, recursive=False)
                    continue
                opener = open
                if packed:
                    suffix = next((s for s in ('.gz', '.bz2', '.xz') if lower.endswith(s)), None)
                    if suffix is None:
                        raise ValueError('Keyword filtering unsupported for archive: ' + path)
                    opener = {'.gz': gzip.open, '.bz2': bz2.open, '.xz': lzma.open}[suffix]
                with opener(path, 'rb') as source, tempfile.TemporaryFile() as filtered:
                    for line in source:
                        if any(word in line for word in keywords):
                            filtered.write(line)
                    size = filtered.tell()
                    if not size:
                        continue
                    filtered.seek(0)
                    info = tarfile.TarInfo(arcname + '.matched')
                    info.size = size
                    archive.addfile(info, filtered)
"""


def add_collect_filters(parser):
    parser.add_argument('--host-filter', metavar='JSON',
                        help='Host IP selection JSON with include and exclude arrays')
    parser.add_argument('--pod-info', action='store_true',
                        help='Include current Pod IP and host IP in collection directory names')
    parser.add_argument('--file-pattern', action='append', default=[], metavar='GLOB',
                        help='Filename glob, e.g. "*access*.log", "*INFO*.log", '
                             '"*operation*.log", "*metrics*.log", "*request*.log", '
                             '"*resource*.log"')
    parser.add_argument('--keyword', action='append', default=[], metavar='TEXT',
                        help='Literal case-sensitive line substring; repeatable (OR), filtered remotely')
    parser.add_argument('--uncompressed-only', action='store_true',
                        help='Exclude compressed archives; retain all uncompressed rotations')


def host_selection_from_args(args):
    selection = {}
    path = getattr(args, 'host_filter', None)
    if path:
        try:
            with open(path, encoding='utf-8-sig') as source:
                selection = json.load(source)
        except (OSError, ValueError) as error:
            raise ValueError('Cannot read host filter ' + str(path) + ': ' + str(error)) from error
        if not isinstance(selection, dict) or set(selection) - {'include', 'exclude'}:
            raise ValueError('Host filter must be an object containing only include and exclude')
        for key, values in selection.items():
            if not isinstance(values, list) or any(not isinstance(ip, str) for ip in values):
                raise ValueError('Host filter ' + key + ' must be an array of IP strings')
    included = selection.get('include', [])
    excluded = selection.get('exclude', [])
    if not included and not excluded:
        return None
    return dict(host_ips=[str(ipaddress.ip_address(ip)) for ip in included],
                exclude_host_ips=[str(ipaddress.ip_address(ip)) for ip in excluded])


def filter_collect_targets(targets, selection, host_ip=None):
    if selection is None:
        return list(targets)
    included = {str(ipaddress.ip_address(ip)) for ip in selection.get('host_ips', [])}
    excluded = {str(ipaddress.ip_address(ip)) for ip in selection.get('exclude_host_ips', [])}
    result = []
    for target in targets:
        address = host_ip(target) if host_ip else target.get('host_ip')
        try:
            address = str(ipaddress.ip_address(address))
        except ValueError:
            name = target.get('name') or target.get('pod_name') or target.get('host') or 'unknown'
            logging.getLogger(__name__).warning(
                'Skipping collection target %s in namespace %s: missing or invalid host IP; '
                'check whether the Pod exists and has status.hostIP, or check host_ip for non-Kubernetes targets',
                name, target.get('namespace', 'unspecified'))
            continue
        if address not in excluded and (not included or address in included):
            result.append(target)
    return result


def read_pod_addresses(namespace):
    output = subprocess.check_output(['kubectl', 'get', 'pods', '-n', namespace, '-o', 'json'],
                                     text=True, timeout=30)
    return {item['metadata']['name']: dict(ip=item.get('status', {}).get('podIP', ''),
                                          host_ip=item.get('status', {}).get('hostIP', ''))
            for item in json.loads(output).get('items', [])}


def archive_options_from_args(args):
    compress, extract = getattr(args, 'compress', None), getattr(args, 'extract', None)
    if compress is None and extract is None:
        return None
    return dict(compress=True if compress is None else compress,
                extract=True if extract is None else extract)


def filters_from_args(args):
    return dict(patterns=getattr(args, 'file_pattern', []), keywords=getattr(args, 'keyword', []),
                uncompressed_only=getattr(args, 'uncompressed_only', False))


def has_filters(options):
    return any(options.values())


def select_targets(targets, names, key):
    if not names:
        return list(targets)
    missing = set(names) - {str(target.get(key, '')) for target in targets}
    if missing:
        raise ValueError('Unknown collection targets: ' + ', '.join(sorted(missing)))
    return [target for target in targets if str(target.get(key, '')) in names]


def pod_directory(name, pod_ip, host_ip):
    def safe(value):
        return re.sub(r'[^a-zA-Z0-9_.-]', '_', str(value or 'unknown'))
    return f'{safe(name)}__podip-{safe(pod_ip)}__hostip-{safe(host_ip)}'


def archive_command(sources, options, archive_options=None):
    if any(not value or '\x00' in value for key in ('patterns', 'keywords') for value in options[key]):
        raise ValueError('Collection patterns and keywords must be nonempty and contain no NUL')
    config = dict(sources=sources, **options)
    if archive_options is not None:
        config['compress'] = archive_options['compress']
    return shlex.join(['python3', '-c', REMOTE_ARCHIVE, json.dumps(config)])


def receive_archive(command, local_dir, timeout=120, shell=False, archive_options=None,
                    archive_name='logs', flatten=False):
    """Spool transfer output, then safely extract or atomically retain the archive."""
    options = archive_options or dict(compress=True, extract=True)
    with tempfile.TemporaryFile() as spool:
        result = subprocess.run(command, shell=shell, stdout=spool, stderr=subprocess.PIPE,
                                check=False, timeout=timeout)
        if result.returncode:
            detail = result.stderr.decode('utf-8', errors='replace')[-2000:]
            raise RuntimeError(f'Collection transport exited {result.returncode}: {detail}')
        spool.seek(0)
        root = Path(local_dir).resolve()
        count = 0
        regular_members = set()
        with tarfile.open(fileobj=spool, mode='r:gz' if options['compress'] else 'r:') as archive:
            for member in archive:
                rel = PurePosixPath(member.name)
                if not (member.isfile() or member.islnk()) or rel.is_absolute() or '..' in rel.parts or '\\' in member.name:
                    raise ValueError('Unsafe collection archive member: ' + member.name)
                if member.islnk():
                    link = PurePosixPath(member.linkname)
                    if (link.is_absolute() or '..' in link.parts or '\\' in member.linkname
                            or str(link) not in regular_members):
                        raise ValueError('Unsafe collection hardlink: ' + member.linkname)
                regular_members.add(str(rel))
                count += 1
                if not options['extract']:
                    continue
                target = root / rel.name if flatten else root.joinpath(*rel.parts)
                if os.path.commonpath([str(target.resolve()), str(root)]) != str(root):
                    raise ValueError('Collection target escapes output directory')
                target.parent.mkdir(parents=True, exist_ok=True)
                with archive.extractfile(member) as source, open(target, 'wb') as dest:
                    shutil.copyfileobj(source, dest)
        if not options['extract']:
            root.mkdir(parents=True, exist_ok=True)
            suffix = '.tar.gz' if options['compress'] else '.tar'
            target = root / (archive_name + suffix)
            temporary = None
            try:
                with tempfile.NamedTemporaryFile(dir=root, suffix='.part', delete=False) as saved:
                    temporary = saved.name
                    spool.seek(0)
                    shutil.copyfileobj(spool, saved)
                os.replace(temporary, target)
            finally:
                if temporary and os.path.exists(temporary):
                    os.unlink(temporary)
        return count


def copy_case_files(config_paths, output_dir):
    output = Path(output_dir).resolve()
    targets = {}
    for config_path in config_paths:
        source = Path(config_path).resolve(strict=True)
        if not source.is_file():
            raise ValueError('Configuration must be a file: ' + str(source))
        target = output / (source.parent.name or 'case') / source.name
        if target in targets and targets[target] != source:
            raise ValueError('Configuration archive name collision: ' + str(target))
        if target.resolve() == source:
            raise ValueError('Configuration archive destination is the source file')
        if os.path.commonpath([str(target.resolve()), str(output)]) != str(output):
            raise ValueError('Configuration archive escapes output directory')
        targets[target] = source
    for target, source in targets.items():
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
