# Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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

"""
A mock of obs service for test
"""
import os
import sys
import uuid
import logging
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import unquote, urlparse, parse_qs
from pathlib import Path
from datetime import datetime, timezone
import mimetypes
import xml.etree.ElementTree as ET


class MockOBSHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self.root_path = kwargs.pop('root_path')
        super().__init__(*args, **kwargs)

    def do_PUT(self):
        """ Handle PUT METHOD request """
        path = self._normalize_key(self.path).lstrip('/')
        if not path or '/' not in path:
            self._send_error(400, 'InvalidURI')
            return

        bucket, key = path.split('/', 1)
        obj_path = self._get_object_path(bucket, key)

        if 'uploads' in self.path:
            upload_id = str(uuid.uuid4())
            (obj_path.parent / f'.upload_{upload_id}').touch()
            response = {
                "InitiateMultipartUploadResult": {
                    "Bucket": bucket,
                    "Key": key,
                    "UploadId": upload_id
                }
            }
            self._send_xml(response)
            return

        try:
            obj_path.parent.mkdir(exist_ok=True, parents=True)
            self._write_file_content(obj_path)

            self.send_response(200)
            self.send_header('ETag', '"fake-etag-for-mock-obs"')
            self.send_header('Content-Length', '0')
            self.end_headers()
            return
        except Exception as e:
            self._send_error(500, 'InternalError', str(e))
            return

    def do_GET(self):
        """ Handle GET METHOD request """
        parsed = urlparse(self.path)
        path = self._normalize_key(parsed.path).lstrip('/')
        query = parse_qs(parsed.query)

        if 'prefix' in query:
            bucket = path.split('/')[0] if path else 'test'
            self._list_objects(bucket, query['prefix'][0], query.get('delimiter', [''])[0])
            return

        if not path or '/' not in path:
            self._send_error(400, 'InvalidURI')
            return

        bucket, key = path.split('/', 1)
        obj_path = self._get_object_path(bucket, key)
        if not obj_path.exists():
            self._send_error(404, 'NoSuchKey')
            return

        try:
            self.send_response(200)
            self.send_header('Content-Type', mimetypes.guess_type(str(obj_path))[0] or 'application/octet-stream')
            self.send_header('Content-Length', str(obj_path.stat().st_size))
            self.send_header('ETag', '"fake-etag-for-mock-obs"')
            self.end_headers()

            self._stream_file_content(obj_path)
            return
        except Exception as e:
            self._send_error(500, 'InternalError', str(e))
            return

    def do_DELETE(self):
        """ Handle DELETE METHOD request """
        path = self._normalize_key(self.path).lstrip('/')
        if not path or '/' not in path:
            self._send_error(400, 'InvalidURI')
            return

        bucket, key = path.split('/', 1)
        obj_path = self._get_object_path(bucket, key)

        if obj_path.exists():
            obj_path.unlink()
        self.send_response(204)
        self.send_header('Content-Length', '0')
        self.end_headers()
        return

    def do_POST(self):
        """ Handle POST METHOD request """
        if self.path.endswith('?delete'):
            self._batch_delete()
            return

        if 'uploadId' in self.path:
            path = self._normalize_key(self.path.split('?')[0]).lstrip('/')
            bucket, key = path.split('/', 1)

            query_params = parse_qs(urlparse(self.path).query)
            upload_id = query_params.get('uploadId', [None])[0]

            if not upload_id:
                self._send_error(400, 'MissingUploadId')
                return

            self._complete_upload(bucket, key, upload_id)
            return

        self._send_error(400, 'InvalidAction')
        return

    def _list_objects(self, bucket, prefix, delimiter):
        """ Handle LIST OBJECT METHOD request """
        objects = []
        common_prefixes = set()

        for root, _, files in os.walk(self._get_object_path(bucket)):
            rel_path = os.path.relpath(root, self._get_object_path(bucket))
            current_prefix = f"{rel_path}/" if rel_path != '.' else ""

            for file in files:
                if file.startswith('.'):
                    continue
                obj_key = f"{current_prefix}{file}"
                if prefix and not obj_key.startswith(prefix):
                    continue

                if delimiter and delimiter in obj_key[len(prefix):]:
                    common = prefix + obj_key[len(prefix):].split(delimiter)[0] + delimiter
                    common_prefixes.add(common)
                    continue

                file_path = Path(root) / file
                objects.append({
                    "Key": obj_key,
                    "LastModified": datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc).isoformat(),
                    "ETag": '"fake-etag-for-mock-obs"',
                    "Size": file_path.stat().st_size,
                    "StorageClass": "STANDARD"
                })

        return self._send_xml({
            "ListBucketResult": {
                "Name": bucket,
                "Prefix": prefix,
                "Marker": "",
                "MaxKeys": "1000",
                "Delimiter": delimiter,
                "IsTruncated": "false",
                "Contents": objects,
                "CommonPrefixes": [{"Prefix": p} for p in sorted(common_prefixes)]
            }
        })

    def _batch_delete(self):
        """ Handle BATCH DELETE METHOD request """
        try:
            content = self.rfile.read(int(self.headers['Content-Length'])).decode()
            deleted = []
            errors = []
            bucket = self.path.split('/')[1]

            self._seek_and_unlink(deleted, errors, bucket, ET.fromstring(content))

            response = {"DeleteResult": {"Deleted": deleted, "Error": errors}}
            self._send_xml(response)
        except Exception as e:
            self._send_error(400, 'MalformedXML', str(e))

    def _complete_upload(self, bucket, key, upload_id):
        """ Handle COMPLETE UPLOAD request """
        parts_dir = self._get_object_path(bucket)
        parts = sorted(parts_dir.glob(f'.part_{upload_id}_*'))

        if not parts:
            self._send_error(400, 'InvalidUploadId')
            return

        final_path = self._get_object_path(bucket, key)
        with open(final_path, 'wb') as f_out:
            for part in parts:
                with open(part, 'rb') as f_in:
                    f_out.write(f_in.read())
                part.unlink()

        (parts_dir / f'.upload_{upload_id}').unlink(missing_ok=True)

        response = {
            "CompleteMultipartUploadResult": {
                "Location": f"/{bucket}/{key}",
                "Bucket": bucket,
                "Key": key,
                "ETag": '"fake-etag-for-mock-obs"'
            }
        }
        self._send_xml(response)
        return

    def _get_object_path(self, bucket, key=None):
        bucket_dir = Path(self.root_path) / bucket
        bucket_dir.mkdir(exist_ok=True, parents=True)
        return bucket_dir / unquote(key) if key else bucket_dir

    def _normalize_key(self, key):
        key = unquote(key)
        return key.replace("%EF%BF%A53B", ";")

    def _generate_xml_response(self, data, root_tag):
        def build_xml(element, parent):
            if isinstance(element, dict):
                for k, v in element.items():
                    sub = ET.SubElement(parent, k)
                    build_xml(v, sub)
            elif isinstance(element, list):
                for item in element:
                    build_xml(item, parent)
            else:
                parent.text = str(element)

        root = ET.Element(root_tag)
        build_xml(data, root)
        return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding='unicode')

    def _write_file_content(self, file_path):
        with open(file_path, 'wb') as f:
            remaining = int(self.headers['Content-Length'])
            while remaining > 0:
                chunk_size = 10 * 1024 * 1024
                chunk = self.rfile.read(min(remaining, chunk_size))
                if not chunk:
                    break
                f.write(chunk)
                remaining -= len(chunk)

    def _seek_and_unlink(self, deleted, errors, bucket, root):
        for obj in root.findall('Object'):
            key = self._normalize_key(obj.find('Key').text)
            obj_path = self._get_object_path(bucket, key)

            try:
                if obj_path.exists():
                    obj_path.unlink()
                    deleted.append({"Key": key})
                else:
                    errors.append({
                        "Key": key,
                        "Code": "NoSuchKey",
                        "Message": "对象不存在"
                    })
            except Exception:
                errors.append({
                    "Key": key,
                    "Code": "InternalError",
                    "Message": "删除失败"
                })

    def _stream_file_content(self, file_path):
        with open(file_path, 'rb') as f:
            chunk_size = 10 * 1024 * 1024
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                self.wfile.write(chunk)

    def _send_xml(self, data):
        xml = self._generate_xml_response(data, list(data.keys())[0])
        self.send_response(200)
        self.send_header('Content-Type', 'application/xml')
        self.send_header('Content-Length', str(len(xml)))
        self.end_headers()
        self.wfile.write(xml.encode())

    def _send_error(self, code, error_code, message=''):
        error = {"Error": {"Code": error_code, "Message": message}}
        xml = self._generate_xml_response(error, "Error")
        self.send_response(code)
        self.send_header('Content-Type', 'application/xml')
        self.send_header('Content-Length', str(len(xml)))
        self.end_headers()
        self.wfile.write(xml.encode())


def run_server(host, port, root_path):
    """ start mock obs service """
    Path(root_path).mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer((host, port), lambda *args: MockOBSHandler(*args, root_path=root_path))
    logging.info(f"Mock OBS Service Start | Address: {host}:{port} | LocalPath: {root_path}")
    server.serve_forever()

if __name__ == '__main__':
    if len(sys.argv) != 4:
        logging.error("To Start OBS: python mock_obs.py <IP> <PORT> <ROOT_PATH>")
        sys.exit(1)
    run_server(sys.argv[1], int(sys.argv[2]), sys.argv[3])
