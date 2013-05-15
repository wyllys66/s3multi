#!/usr/bin/python
# Copyright (c) 2013 EVault, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# S3 Multipart Upload filter factory for the SWIFT proxy-server
#

from setuptools import setup

import s3multi

setup(name='s3multi',
      version=s3multi.version,
      description='Amazon S3 Multipart Upload Middleware for Swift',
      license='Apache License (2.0)',
      author='Wyllys Ingersoll, EVault Inc',
      author_email='wyllys.ingersoll@evault.com',
      packages=['s3multi'],
      requires=['swift(>=1.8.0)', 'swift3(>=1.7.0)'],
      entry_points={'paste.filter_factory':
                    ['s3multi=s3multi.middleware:filter_factory']})
