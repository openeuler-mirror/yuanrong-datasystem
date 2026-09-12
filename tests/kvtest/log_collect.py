"""Shared selective collection for the existing deployment CLIs."""

import json
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
with tarfile.open(fileobj=sys.stdout.buffer, mode='w|gz') as archive:
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
    parser.add_argument('--pod-info', action='store_true',
                        help='Include current Pod IP and host IP in collection directory names')
    parser.add_argument('--file-pattern', action='append', default=[], metavar='GLOB',
                        help='Filename glob, e.g. "*access*.log"; repeatable (OR)')
    parser.add_argument('--keyword', action='append', default=[], metavar='TEXT',
                        help='Literal case-sensitive line substring; repeatable (OR), filtered remotely')
    parser.add_argument('--uncompressed-only', action='store_true',
                        help='Exclude compressed archives; retain all uncompressed rotations')


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


def archive_command(sources, options):
    if any(not value or '\x00' in value for key in ('patterns', 'keywords') for value in options[key]):
        raise ValueError('Collection patterns and keywords must be nonempty and contain no NUL')
    return shlex.join(['python3', '-c', REMOTE_ARCHIVE, json.dumps(dict(sources=sources, **options))])


def receive_archive(command, local_dir, timeout=120, shell=False):
    """Spool transport output to disk, then extract regular files inside the destination."""
    with tempfile.TemporaryFile() as spool:
        result = subprocess.run(command, shell=shell, stdout=spool, stderr=subprocess.PIPE,
                                check=False, timeout=timeout)
        if result.returncode:
            detail = result.stderr.decode('utf-8', errors='replace')[-2000:]
            raise RuntimeError(f'Collection transport exited {result.returncode}: {detail}')
        spool.seek(0)
        root = Path(local_dir).resolve()
        count = 0
        with tarfile.open(fileobj=spool, mode='r:gz') as archive:
            for member in archive:
                rel = PurePosixPath(member.name)
                if not member.isfile() or rel.is_absolute() or '..' in rel.parts or '\\' in member.name:
                    raise ValueError('Unsafe collection archive member: ' + member.name)
                target = root.joinpath(*rel.parts)
                if os.path.commonpath([str(target.resolve()), str(root)]) != str(root):
                    raise ValueError('Collection target escapes output directory')
                target.parent.mkdir(parents=True, exist_ok=True)
                with archive.extractfile(member) as source, open(target, 'wb') as dest:
                    shutil.copyfileobj(source, dest)
                count += 1
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
