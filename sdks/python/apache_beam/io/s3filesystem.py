#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

""":class:`~apache_beam.io.filesystem.FileSystem` implementation for accessing
AWS S3 Distributed File System files."""

from __future__ import absolute_import

import logging
import posixpath
import re
from builtins import zip

try:
  import s3fs
  import boto3
except ImportError:
  pass

from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import CompressedFile
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import FileSystem
from apache_beam.io.filesystem import FileMetadata
from apache_beam.options.pipeline_options import S3FileSystemOptions
from apache_beam.options.pipeline_options import PipelineOptions


__all__ = ['S3FileSystem']


_S3_PREFIX = 's3:/'
_URL_RE = re.compile(r'^' + _S3_PREFIX + r'(/.*)')

class S3FileSystem(FileSystem):
  """``FileSystem`` implementation that supports S3.

  URL arguments to methods expect strings starting with ``s3://``.
  """

  def __init__(self, pipeline_options):
    """Initializes a connection to S3.

    Connection configuration is done by passing pipeline options.
    See :class:`~apache_beam.options.pipeline_options.S3FileSystemOptions`.
    """
    super(S3FileSystem, self).__init__(pipeline_options)
    logging.getLogger('s3fs').setLevel(logging.WARN)
    if pipeline_options is None:
      raise ValueError('pipeline_options is not set')
    if isinstance(pipeline_options, PipelineOptions):
      s3_options = pipeline_options.view_as(S3FileSystemOptions)
      aws_access_key_id = s3_options.aws_access_key_id
      aws_secret_access_key = s3_options.aws_secret_access_key
    else:
      aws_access_key_id = pipeline_options.get('aws_access_key_id')
      aws_secret_access_key = pipeline_options.get('aws_secret_access_key')

    if aws_access_key_id is None:
      logging.info('aws_access_key_id is not set')

    if aws_secret_access_key is None:
      logging.info('aws_secret_access_key is not set')
    self._s3_client = s3fs.S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)

  @classmethod
  def scheme(cls):
    return 's3'

  @staticmethod
  def _parse_url(url):
    """Verifies that url begins with s3:// prefix, strips it and adds a
    leading /.

    Raises:
      ValueError if url doesn't begin with s3://.

    Args:
      url: A URL in the form s3://path/...

    Returns:
      For an input of 's3://path/...', will return '/path/...'.
    """
    m = _URL_RE.match(url)
    if m is None:
      raise ValueError('Could not parse url: %s' % url)
    return m.group(1)

  def _get_bucket_and_key(self, path):
    path = self._s3_client._strip_protocol(path).rstrip('/')
    bucket =  path.split('/')[0]
    key = posixpath.join(*path.split('/')[1:])

    return bucket, key

  def _list(self, url):
    try:
      for res in self._s3_client.ls(url):
        path = 's3://' + res
        yield FileMetadata(path, self._s3_client.size(path))
    except Exception as e:  # pylint: disable=broad-except
      raise BeamIOError('List operation failed', {url: e})

  def join(self, base_url, *paths):
    """Join two or more pathname components.

    Args:
      base_url: string path of the first component of the path.
        Must start with s3://.
      paths: path components to be added

    Returns:
      Full url after combining all the passed components.
    """
    basepath = self._parse_url(base_url)
    return _S3_PREFIX + self._join(basepath, *paths)

  def _join(self, basepath, *paths):
    return posixpath.join(basepath, *paths)

  def split(self, url):
    rel_path = self._parse_url(url)
    head, tail = posixpath.split(rel_path)
    return _S3_PREFIX + head, tail

  def _mkdirs(self, path):
    s3 = self._s3_client
    if s3.isfile(path.rstrip('/')):
        print('Error: exist file {}'.format(path))
        return
    bucket, key = self._get_bucket_and_key(path)
    key = key + '/'
    s3.s3.put_object(Bucket=bucket, Key=key)

    s3.invalidate_cache()
    if not s3.exists(path):
        print('Failed to mkdir {}'.format(path))

  def mkdirs(self, url):
    if self._s3_client.exists(url):
      raise BeamIOError('Path already exists: %s' % url)

    self._mkdirs(url)

  def has_dirs(self):
    return True

  def _path_open(self, url, mode, mime_type, compression_type):
    fs = s3fs.S3File(self._s3_client, url, mode=mode)
    # fs.setxattr(copy_kwargs={'ContentType': mime_type})
    if compression_type == CompressionTypes.AUTO:
      compression_type = CompressionTypes.detect_compression_type(url)
    if compression_type != CompressionTypes.UNCOMPRESSED:
        return CompressedFile(fs, compression_type=compression_type)

    return fs

  def create(self, url, mime_type='application/octet-stream',
             compression_type=CompressionTypes.AUTO):
    """
    Returns:
      A Python File-like object.
    """
    return self._path_open(url, 'wb', mime_type, compression_type)

  def open(self, url, mime_type='application/octet-stream',
           compression_type=CompressionTypes.AUTO):
    """
    Returns:
      A Python File-like object.
    """
    return self._path_open(url, 'rb', mime_type, compression_type)

  def _copy(self, src, dest):
      if not self._s3_client.exists(src):
          print('The input file {} not exists'.format(src))
      src = src.rstrip('/')
      dest = dest.rstrip('/')
      if self._s3_client.isfile(src):
          self._s3_client.cp(src, dest)
          return

      for path, dirs, files in self._s3_client.walk(src):
          for dir in dirs:
              new_dir = posixpath.join(dest, dir)
              if not self._s3_client.exists(new_dir):
                  self._mkdirs(new_dir)
          for file in files:
              if file == '':
                  continue
              src_file = 's3://{}/{}'.format(path, file)
              dest_file = src_file.replace(src, dest, 1)
              self._s3_client.cp(src_file, dest_file)

  def copy(self, source_file_names, destination_file_names):
    """
    It is an error if any file to copy already exists at the destination.

    Raises ``BeamIOError`` if any error occurred.

    Args:
      source_file_names: iterable of URLs.
      destination_file_names: iterable of URLs.
    """
    if len(source_file_names) != len(destination_file_names):
      raise BeamIOError(
          'source_file_names and destination_file_names should '
          'be equal in length: %d != %d' % (
              len(source_file_names), len(destination_file_names)))

    exceptions = {}
    for source, destination in zip(source_file_names, destination_file_names):
      try:
        self._copy(source, destination)
      except Exception as e:  # pylint: disable=broad-except
        exceptions[(source, destination)] = e

    if exceptions:
      raise BeamIOError('Copy operation failed', exceptions)

  # TODO(yajunwong): Speed this operation
  def _rename(self, old_name, new_name):
    """Rename from old_name to new_name

    Note:
      This operation may be very slow
    """
    self._copy(old_name, new_name)
    self._delete(old_name)

    self._s3_client.invalidate_cache()
    assert self._s3_client.exists(new_name)

  def rename(self, source_file_names, destination_file_names):
    exceptions = {}
    for source, destination in zip(source_file_names, destination_file_names):
      try:
        self._rename(source, destination)
      except Exception as e:  # pylint: disable=broad-except
        exceptions[(source, destination)] = e

    if exceptions:
      raise BeamIOError('Rename operation failed', exceptions)

  def exists(self, url):
    """Checks existence of url in S3.

    Args:
      url: String in the form s3://...

    Returns:
      True if url exists as a file or directory in s3.
    """
    return self._s3_client.exists(url)

  def size(self, url):
    return self._s3_client.size(url)

  def last_updated(self, url):
    raise self._s3_client.info(url)['LastModified'].timestamp()

  def checksum(self, url):
    """Fetches a checksum description for a URL.

    Returns:
      String describing the checksum.
    """
    return self._s3_client.checksum(url)

  def _delete(self, path):
    if not self._s3_client.exists(path):
      return

    if self._s3_client.isfile(path):
      self._s3_client.rm(path)
    else:
      s3 = boto3.resource('s3')
      bucket, key = self._get_bucket_and_key(path)
      bucket = s3.Bucket(bucket)
      prefix = key + '/'
      bucket.objects.filter(Prefix=prefix).delete()

    assert not self._s3_client.exists(path)

  def delete(self, urls):
    exceptions = {}
    for url in urls:
      try:
        self._delete(url)
      except Exception as e:  # pylint: disable=broad-except
        exceptions[url] = e

    if exceptions:
      raise BeamIOError("Delete operation failed", exceptions)


