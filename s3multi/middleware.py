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
"""
S3 MultiUpload API support for Swift.

Requires
--------
1) swift3, s3token, and the authentication middleware must be in the
   proxy-config pipeline BEFORE this module

This module implements support for the following S3 multi-upload operations:

- Initiate Multipart Upload
    http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadInitiate.html
- Upload Part
    http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html
- Upload Part (Copy)
    http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPartCopy.html
- Complete Multipart Upload
    http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html
- Abort Multipart Upload
    http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadAbort.html
- List Parts
    http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadListParts.html

Reference
---------
http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuoverview.html

Notes
-----
This filter translates the S3 multipart requests into the correct SWIFT calls,
which in some cases may result in multiple swift API calls for a single
multipart S3 API request.  A completed S3 multipart upload will look just
like a completed multipart SWIFT upload.  The uploaded object will appear in
the requested container as a single object, but a separate container (named
as the original container name + '_segments') will be created to hold the
individual parts, exactly as it is done when doing a segmented SWIFT upload.

The folder name of the segments is slightly different than for swift, but the
result is the same.

Just as with SWIFT segmented uploads, if individual segments are deleted,
downloads of the aggregated object will result in an incomplete object.  Also,
deleting the aggregate object does not automatically delete the segmented
container or segments in the container.
"""

from urllib import unquote
from xml.sax.saxutils import escape as xml_escape
import urlparse

from simplejson import loads
import os
import md5
import time

from swift.common.utils import split_path
from swift.common.utils import get_logger
from swift.common.wsgi import WSGIContext
from swift.common.swob import Request, Response
from swift.common.http import HTTP_OK, HTTP_CREATED,\
    HTTP_BAD_REQUEST, HTTP_UNAUTHORIZED, HTTP_FORBIDDEN, \
    HTTP_NOT_FOUND, HTTP_CONFLICT, is_success, \
    HTTP_NOT_IMPLEMENTED, HTTP_LENGTH_REQUIRED, HTTP_SERVICE_UNAVAILABLE


def get_err_response(code):
    """
    Given an HTTP response code, create a properly formatted xml error response

    :param code: error code
    :returns: webob.response object
    """
    error_table = {
        'AccessDenied':
        (HTTP_FORBIDDEN, 'Access denied'),
        'BucketAlreadyExists':
        (HTTP_CONFLICT, 'The requested bucket name is not available'),
        'BucketNotEmpty':
        (HTTP_CONFLICT, 'The bucket you tried to delete is not empty'),
        'InvalidArgument':
        (HTTP_BAD_REQUEST, 'Invalid Argument'),
        'InvalidBucketName':
        (HTTP_BAD_REQUEST, 'The specified bucket is not valid'),
        'InvalidURI':
        (HTTP_BAD_REQUEST, 'Could not parse the specified URI'),
        'InvalidDigest':
        (HTTP_BAD_REQUEST, 'The Content-MD5 you specified was invalid'),
        'BadDigest':
        (HTTP_BAD_REQUEST, 'The Content-Length you specified was invalid'),
        'NoSuchObject':
        (HTTP_NOT_FOUND, 'The specified object does not exist'),
        'NoSuchBucket':
        (HTTP_NOT_FOUND, 'The specified bucket does not exist'),
        'NoSuchUpload':
        (HTTP_NOT_FOUND, 'The specified bucket does not exist'),
        'SignatureDoesNotMatch':
        (HTTP_FORBIDDEN, 'The calculated request signature does not '
            'match your provided one'),
        'RequestTimeTooSkewed':
        (HTTP_FORBIDDEN, 'The difference between the request time and the'
            ' current time is too large'),
        'NoSuchKey':
        (HTTP_NOT_FOUND, 'The resource you requested does not exist'),
        'Unsupported':
        (HTTP_NOT_IMPLEMENTED, 'The feature you requested is not yet'
            ' implemented'),
        'MissingContentLength':
        (HTTP_LENGTH_REQUIRED, 'Length Required'),
        'ServiceUnavailable':
        (HTTP_SERVICE_UNAVAILABLE, 'Please reduce your request rate')}

    resp = Response(content_type='text/xml')
    resp.status = error_table[code][0]
    resp.body = '<?xml version="1.0" encoding="UTF-8"?>\r\n<Error>\r\n  ' \
                '<Code>%s</Code>\r\n  <Message>%s</Message>\r\n</Error>\r\n' \
                % (code, error_table[code][1])
    return resp


class S3MultiMiddleware(WSGIContext):

    def __init__(self, app, conf, *args, **kwargs):
        self.app = app
        self.conf = conf
        self.logger = get_logger(self.conf, log_route='s3multi')

    def __call__(self, env, start_response):
        try:
            return self.handle_request(env, start_response)
        except Exception, e:
            self.logger.exception(e)
        return get_err_response('ServiceUnavailable')(env, start_response)

    def getObjNum(self, object):
        try:
            num = int(os.path.basename(object['name']))
        except:
            num = 0
        return num

    def get_bucket_objects(self, req, env):
        parts = urlparse.urlparse(req.url)
        version, auth, container, obj = split_path(parts.path, 0, 4, True)

        env['REQUEST_METHOD'] = 'GET'
        env['PATH_INFO'] = ('/%s/%s/%s' % (version, auth, container))
        env['RAW_PATH_INFO'] = ('/%s/%s/%s' % (version, auth, container))
        env['QUERY_STRING'] = 'format=json&limit=1001&&delimiter=/'
        env['SCRIPT_NAME'] = ''

        app_iter = self._app_call(env)
        status = self._get_status_int()
        if is_success(status):
            objects = loads(''.join(list(app_iter)))
        else:
            objects = ()
        return objects

    def get_object_size(self, env, objname):
        req = Request(env)
        parts = urlparse.urlparse(req.url)
        version, auth, container, obj = split_path(parts.path, 0, 4, True)

        env['REQUEST_METHOD'] = 'HEAD'
        env['PATH_INFO'] = ('/%s/%s/%s/%s' % (version, auth, container,
                            objname))
        env['RAW_PATH_INFO'] = ('/%s/%s/%s/%s' % (version, auth, container,
                                objname))
        env['SCRIPT_NAME'] = ''
        env['QUERY_STRING'] = 'format=json&limit=1001&&delimiter=/'

        self._app_call(env)
        headers = headers = dict(self._response_headers)
        if 'content-length' in headers:
            return int(headers['content-length'])

        return 0

    def HEAD(self, env, start_response):
        req = Request(env)

        body_iter = self._app_call(env)
        status = self._get_status_int()
        headers = dict(self._response_headers)

        parts = urlparse.urlparse(req.url)
        version, auth, container, obj = split_path(parts.path, 0, 4, True)
        #
        # Check for tell-tale sign that the caller wants HEAD status of
        # a DLO container.
        #
        if is_success(status) and container is not None and obj is None and \
            'x-container-object-count' in headers and \
                'x-container-bytes-used' in headers:
            objcount = headers['x-container-object-count']
            bytesused = headers['x-container-bytes-used']
            objsize = 0
            if int(objcount) > 0 and int(bytesused) == 0:
                objects = self.get_bucket_objects(req, env)
                for o in objects:
                    objsize += self.get_object_size(env, o['name'])

            headers['x-container-bytes-used'] = ('%s' % objsize)

        resp = Response(status=status, headers=headers, app_iter=body_iter)
        return resp(env, start_response)

    def GET_uploads(self, env, start_response):
        if 'QUERY_STRING' in env:
            args = dict(urlparse.parse_qsl(env['QUERY_STRING'], 1))
        else:
            args = {}

        req = Request(env)

        parts = urlparse.urlparse(req.url)
        version, auth, container, obj = split_path(parts.path, 0, 4, True)

        env['PATH_INFO'] = ('/%s/%s/%s_segments/' % (version, auth, container))
        env['RAW_PATH_INFO'] = ('/%s/%s/%s_segments/' % (version, auth,
                                container))

        keyword, info = req.headers['Authorization'].split(' ')
        account, signature = info.rsplit(':', 1)

        maxuploads = 1000
        prefix = ''
        delimiter = ''
        keymarker = ''
        uploadid = ''
        if 'max-uploads' in args:
            try:
                maxuploads = int(args['max-uploads'])
            except:
                pass
        if 'key-marker' in args:
            keymarker = args['key-marker']
        if 'upload-id-marker' in args:
            uploadid = args['upload-id-marker']
        if 'prefix' in args:
            prefix = '&prefix=%s' % args['prefix']
        if 'delimiter' in args:
            delimiter = '&delimiter=%s' % args['delimiter']

        if uploadid and keymarker:
            prefix = '&prefix=%s/%s/' % (keymarker, uploadid)
        elif keymarker:
            prefix = '&prefix=%s/' % keymarker

        env['QUERY_STRING'] = 'format=json&limit=%d%s%s' % \
                              (maxuploads, prefix, delimiter)
        body_iter = self._app_call(env)
        status = self._get_status_int()

        if is_success(status) is False:
            if status == HTTP_UNAUTHORIZED:
                return get_err_response('AccessDenied')
            elif status == HTTP_NOT_FOUND:
                return get_err_response('NoSuchBucket')
            else:
                return get_err_response('InvalidURI')

        objects = loads(''.join(list(body_iter)))
        objdict = {}
        for o in objects:
            parts = split_path('/' + o['name'], 1, 4)
            if parts[1] not in objdict:
                objdict[parts[1]] = {'key': parts[0],
                                     'last_modified': o['last_modified']}

        objkeys = objdict.keys()
        if maxuploads > 0 and len(objkeys) > maxuploads:
            truncated = True
        else:
            truncated = False

        nextkeymarker = ''
        nextuploadmarker = ''
        if len(objkeys) > 1:
            objid = objkeys[1]
            o = objdict[objid]
            nextuploadmarker = o.keys()[0]
            nextkeymarker = o['key']

        body = ('<?xml version="1.0" encoding="UTF-8"?>'
                '<ListMultipartUploadsResult '
                'xmlns="http://s3.amazonaws.com/doc/2006-03-01">'
                '<Bucket>%s</Bucket>'
                '<KeyMarker>%s</KeyMarker>'
                '<UploadIdMarker>%s</UploadIdMarker>'
                '<NextKeyMarker>%s</NextKeyMarker>'
                '<NextUploadIdMarker>%s</NextUploadIdMarker>'
                '<MaxUploads>%d</MaxUploads>'
                '<IsTruncated>%s</IsTruncated>' %
                (xml_escape(container),
                 xml_escape(keymarker),
                 xml_escape(uploadid),
                 nextkeymarker, nextuploadmarker,
                 maxuploads,
                 'true' if truncated else 'false'))

        looped = "".join(['<Upload>'
                          '<Key>%s</Key>'
                          '<UploadId>%s</UploadId>'
                          '<Initiator><ID>%s</ID><DisplayName>%s</DisplayName>'
                          '</Initiator>'
                          '<Owner><ID>%s</ID>'
                          '<DisplayName>%s</DisplayName></Owner>'
                          '<StorageClass>STANDARD</StorageClass>'
                          '<Initiated>%s</Initiated>'
                          '</Upload>'
                          % (objdict[i]['key'], i, account, account,
                             account, account, objdict[i]['last_modified'])
                          for i in objdict.keys()])

        body = body + looped + '</ListMultipartUploadsResult>'

        return Response(status=HTTP_OK, body=body,
                        content_type='application/xml')

    def GET(self, env, start_response):
        if 'QUERY_STRING' in env:
            args = dict(urlparse.parse_qsl(env['QUERY_STRING'], 1))
        else:
            args = {}

        #
        # If 'uploadId' parameter is not present, then pass it along as a
        # standard 'GET' request
        #
        if 'uploadId' not in args and 'uploads' not in args:
            return self.app(env, start_response)

        req = Request(env)
        if 'uploadId' in args:
            uploadId = args['uploadId']

        if 'Authorization' not in req.headers:
            return get_err_response('AccessDenied')(env, start_response)
        try:
            keyword, info = req.headers['Authorization'].split(' ')
        except:
            return get_err_response('AccessDenied')(env, start_response)

        if keyword != 'AWS':
            return get_err_response('AccessDenied')(env, start_response)
        try:
            account, signature = info.rsplit(':', 1)
        except:
            return get_err_response('InvalidArgument')(env, start_response)

        if 'uploads' in args:
                return self.GET_uploads(env, start_response)

        maxparts = 1000
        partNumMarker = 0
        if 'max-parts' in args:
            try:
                maxparts = int(args['max-parts'])
            except:
                maxparts = 1000
        if 'part-number-marker' in args:
            try:
                partNumMarker = int(args['part-number-marker'])
            except:
                partNumMarker = 0

        parts = urlparse.urlparse(req.url)
        version, auth, container, obj = split_path(parts.path, 0, 4, True)

        env['PATH_INFO'] = ('/%s/%s/%s_segments/' % (version, auth, container))
        env['RAW_PATH_INFO'] = ('/%s/%s/%s_segments/' % (version, auth,
                                container))
        env['QUERY_STRING'] = 'format=json&limit=1001&prefix=%s/%s/' \
                              '&delimiter=/' % (obj, uploadId)
        body_iter = self._app_call(env)
        status = self._get_status_int()

        if is_success(status) is False:
            if status == HTTP_UNAUTHORIZED:
                return get_err_response('AccessDenied')
            elif status == HTTP_NOT_FOUND:
                return get_err_response('NoSuchBucket')
            else:
                return get_err_response('InvalidURI')

        objects = loads(''.join(list(body_iter)))

        lastPart = ''
        firstPart = ''

        objList = []
        #
        # If the caller requested a list starting at a specific part number,
        # construct a sub-set of the object list.
        #
        if partNumMarker > 0 and len(objects) > 0:
            for o in objects:
                num = self.getObjNum(o)
                if num >= partNumMarker:
                    objList.append(o)
        else:
            objList = objects

        if maxparts > 0 and len(objList) == (maxparts + 1):
            truncated = True
            o = objList[-1]
            lastPart = os.path.basename(o['name'])
        else:
            truncated = False

        body = ('<?xml version="1.0" encoding="UTF-8"?>'
                '<ListPartsResult '
                'xmlns="http://s3.amazonaws.com/doc/2006-03-01">'
                '<Bucket>%s</Bucket>'
                '<Key>%s</Key>'
                '<UploadId>%s</UploadId>'
                '<Initiator><ID>%s</ID><DisplayName>%s</DisplayName>'
                '</Initiator>'
                '<Owner><ID>%s</ID><DisplayName>%s</DisplayName></Owner>'
                '<StorageClass>STANDARD</StorageClass>'
                '<IsTruncated>%s</IsTruncated>'
                '<MaxParts>%d</MaxParts>' %
                (xml_escape(container),
                 xml_escape(obj),
                 uploadId,
                 account, account, account, account,
                 'true' if truncated else 'false',
                 maxparts))

        if len(objList) > 0:
            o = objList[0]
            firstPart = os.path.basename(o['name'])
            looped = "".join(['<Part><PartNumber>%s</PartNumber>'
                              '<ETag>%s</ETag>'
                              '<LastModified>%s</LastModified>'
                              '<Size>%s</Size></Part>'
                             % (xml_escape(unquote(os.path.basename(
                                                   i['name']))),
                                i['hash'], i['last_modified'], i['bytes'])
                     for i in objList[:maxparts] if 'subdir' not in i])

            body = body + ('<PartNumberMarker>%s</PartNumberMarker>' %
                           firstPart) + \
                ('%s' % ('<NextPartNumberMarker>%s</NextPartNumberMarker>' %
                         lastPart) if truncated else '') + \
                looped

        body = body + '</ListPartsResult>'

        return Response(status=HTTP_OK, body=body,
                        content_type='application/xml')

    def completeMultipartUpload(self, env, start_response):
        req = Request(env)
        if 'QUERY_STRING' in env:
            args = dict(urlparse.parse_qsl(env['QUERY_STRING'], 1))
        else:
            args = {}

        uploadId = args['uploadId']

        urlparts = urlparse.urlparse(req.url)
        version, auth, ignored = split_path(urlparts.path, 2, 3, True)

        # We must get the actual container/object info from the RAW_PATH_INFO
        path = env['RAW_PATH_INFO']
        container, obj = split_path(path, 0, 2, True)
        if obj is None:
            obj = os.path.basename(env['RAW_PATH_INFO'])

        #
        # Query for the objects in the segments area to make sure it completed
        #
        env['REQUEST_METHOD'] = 'GET'
        env['PATH_INFO'] = ('/%s/%s/%s_segments' % (version, auth, container))
        env['RAW_PATH_INFO'] = ('/%s/%s/%s_segments' % (version, auth,
                                                        container))
        env['QUERY_STRING'] = 'format=json&limit=1001&prefix=%s/%s/' \
                              '&delimiter=/' % (obj, uploadId)
        env['SCRIPT_NAME'] = ''

        req = Request(env)

        body_iter = self._app_call(env)
        status = self._get_status_int()

        objinfo = loads(''.join(list(body_iter)))

        if len(objinfo) == 0:
            return get_err_response('NoSuchBucket')

        #
        # Tell Swift the manifest info
        # The content length should be 0 and the manifest should point to
        # the segment container.
        #
        req.method = 'PUT'
        req.headers['X-Object-Manifest'] = ('%s_segments/%s/%s' % (container,
                                            obj, uploadId))
        req.headers['Content-Length'] = '0'
        env['PATH_INFO'] = ('/%s/%s/%s/%s' % (version, auth, container, obj))
        env['RAW_PATH_INFO'] = ('/%s/%s/%s/%s' %
                                (version, auth, container, obj))
        del env['QUERY_STRING']
        env['SCRIPT_NAME'] = ''
        req.body = ''

        body_iter = self._app_call(env)
        status = self._get_status_int()

        if status != HTTP_OK and status != HTTP_CREATED:
            if status == HTTP_UNAUTHORIZED:
                return get_err_response('AccessDenied')
            elif status == HTTP_NOT_FOUND:
                return get_err_response('NoSuchBucket')
            else:
                return get_err_response('InvalidURI')

        o = objinfo[0]

        body = ('<?xml version="1.0" encoding="UTF-8"?>'
                '<CompleteMultipartUploadResult '
                'xmlns="http://s3.amazonaws.com/doc/2006-03-01">'
                '<Location>%s://%s/%s/%s</Location>'
                '<Bucket>%s</Bucket>'
                '<Key>%s</Key>'
                '<ETag>"%s"</ETag>'
                '</CompleteMultipartUploadResult>' %
                (urlparts.scheme, urlparts.netloc, container, obj, container,
                obj, o['hash']))

        resp = Response(body=body, content_type="application/xml")

        return resp

    def POST(self, env, start_response):
        req = Request(env)

        if 'QUERY_STRING' in env:
            args = dict(urlparse.parse_qsl(env['QUERY_STRING'], 1))
        else:
            args = {}
        if 'uploads' in args:
            #
            # return multi-upload body start
            #
            parts = urlparse.urlparse(req.url)
            version, auth, cont = split_path(parts.path, 0, 3, True)

            path = env['RAW_PATH_INFO']
            container, obj = split_path(path, 0, 2, True)

            #
            # Create a unique S3 upload id from the request string and salt
            # it with the current time to avoid duplicates.
            #
            m = md5.new(('/%s/%s/%s/%s-%d' %
                        (version, auth, container, obj, time.time())))
            upload_id = m.hexdigest()

            req.method = 'PUT'
            env['PATH_INFO'] = ('/v1/%s/%s_segments' % (auth, container))
            env['RAW_PATH_INFO'] = ('/v1/%s/%s_segments' % (auth, container))
            env['SCRIPT_NAME'] = ''
            env['QUERY_STRING'] = ''

            self._app_call(env)
            status = self._get_status_int()

            if not is_success(status):
                if status == HTTP_UNAUTHORIZED:
                    return get_err_response('AccessDenied')
                elif status == HTTP_NOT_FOUND:
                    return get_err_response('NoSuchBucket')
                else:
                    return get_err_response('InvalidURI')

            #
            # Return the S3 response
            #
            body = ('<?xml version="1.0" encoding="UTF-8"?>\r\n'
                    '<InitiateMultipartUploadResult xmlns='
                    '"http://doc.s3.amazonaws.com/2006-03-01/">\r\n'
                    '<Bucket>%s</Bucket>\r\n'
                    '<Key>%s</Key>\r\n'
                    '<UploadId>%s</UploadId>\r\n'
                    '</InitiateMultipartUploadResult>\r\n'
                    % (container, obj, upload_id))

            resp = Response(status=200, body=body,
                            content_type='application/xml')
            return resp
        elif 'uploadId' in args:
            # Handle an individual S3 multipart upload segment
            return self.completeMultipartUpload(env, start_response)
        else:
            return self.app(env, start_response)

    def DELETE(self, env, start_response):
        req = Request(env)

        def deleteSegmentObj(env, auth, container, obj):
            req.method = 'DELETE'

            env['PATH_INFO'] = ('/v1/%s/%s_segments/%s' %
                                (auth, container, obj['name']))
            env['RAW_PATH_INFO'] = ('/v1/%s/%s_segments/%s' %
                                    (auth, container, obj['name']))
            env['SCRIPT_NAME'] = ''

            self.app(env, start_response)
            status = self._get_status_int()

            return status

        if 'QUERY_STRING' in env:
            args = dict(urlparse.parse_qsl(env['QUERY_STRING'], 1))
        else:
            args = {}

        if 'uploadId' not in args:
            return self.app(env, start_response)

        uploadId = args['uploadId']

        parts = urlparse.urlparse(req.url)
        version, auth, container, obj = split_path(parts.path, 0, 4, True)
        #
        # First check to see if this multi-part upload was already
        # completed.  Look in the primary container, if the object exists,
        # then it was completed and we return an error here.
        #
        req.method = 'GET'
        env['PATH_INFO'] = ('/%s/%s/%s/%s' % (version, auth, container, obj))
        env['RAW_PATH_INFO'] = ('/%s/%s/%s/%s' % (version, auth,
                                                  container, obj))
        del env['QUERY_STRING']
        env['SCRIPT_NAME'] = ''

        body_iter = self._app_call(env)
        status = self._get_status_int()

        if is_success(status):
            # The object was found, so we must return an error
            return get_err_response('NoSuchUpload')
        elif status == HTTP_UNAUTHORIZED:
            return get_err_response('AccessDenied')
        elif status != HTTP_NOT_FOUND:
            return get_err_response('InvalidURI')

        #
        # The completed object was not found so this
        # must be a multipart upload abort.
        # We must delete any uploaded segments for this UploadID and then
        # delete the object in the main container as well
        #
        req.method = 'GET'
        env['PATH_INFO'] = ('/%s/%s/%s_segments/' % (version, auth, container))
        env['RAW_PATH_INFO'] = ('/%s/%s/%s_segments/' %
                                (version, auth, container))
        env['QUERY_STRING'] = 'format=json&limit=1001&prefix=%s/%s/' \
                              '&delimiter=/' % (obj, uploadId)
        env['SCRIPT_NAME'] = ''

        body_iter = self._app_call(env)
        status = self._get_status_int()

        if not is_success(status):
            if status == HTTP_UNAUTHORIZED:
                return get_err_response('AccessDenied')
            elif status == HTTP_NOT_FOUND:
                return get_err_response('NoSuchUpload')
            else:
                return get_err_response('InvalidURI')

        #
        #  Iterate over the segment objects and delete them individually
        #
        del env['QUERY_STRING']
        objects = loads(''.join(list(body_iter)))
        for o in objects:
            status = deleteSegmentObj(env, auth, container, o)
            if not is_success(status):
                if status == HTTP_UNAUTHORIZED:
                    return get_err_response('AccessDenied')
                elif status == HTTP_NOT_FOUND:
                    return get_err_response('NoSuchObject')
                else:
                    return get_err_response('InvalidURI')

        #
        # Delete the object from the segment container
        #
        req.method = 'DELETE'
        env['PATH_INFO'] = ('/%s/%s/%s_segments/%s' %
                            (version, auth, container, obj))
        env['RAW_PATH_INFO'] = ('/%s/%s/%s_segments/%s' %
                                (version, auth, container, obj))
        env['SCRIPT_NAME'] = ''

        body_iter = self._app_call(env)
        status = self._get_status_int()
        if is_success(status) is False and status != HTTP_NOT_FOUND:
            if status == HTTP_UNAUTHORIZED:
                return get_err_response('AccessDenied')
            else:
                return get_err_response('InvalidURI')

        resp = Response(status=204, body='')
        return resp(env, start_response)

    def handle_request(self, env, start_response):
        req = Request(env)
        self.logger.debug('Calling S3Multipart Helper Middleware')
        #self.logger.debug(req.__dict__)
        #self.logger.info('S3MULTI --')
        #self.logger.info('S3MULTI: %s %s' % (req.method, req.url))
        #self.logger.info('S3MULTI --')

        #
        # Check that the request has the correct AA headers
        #
        if 'AWSAccessKeyId' in req.params:
            try:
                req.headers['Date'] = req.params['Expires']
                req.headers['Authorization'] = \
                    'AWS %(AWSAccessKeyId)s:%(Signature)s' % req.params
            except KeyError:
                return get_err_response('InvalidArgument')(env, start_response)

        if 'Authorization' not in req.headers:
            return self.app(env, start_response)
        try:
            keyword, info = req.headers['Authorization'].split(' ')
        except:
            return get_err_response('AccessDenied')(env, start_response)

        if keyword != 'AWS':
            return get_err_response('AccessDenied')(env, start_response)
        try:
            account, signature = info.rsplit(':', 1)
        except:
            return get_err_response('InvalidArgument')(env, start_response)

        if req.method == 'GET':
            return self.GET(env, start_response)
        elif req.method == 'HEAD':
            return self.HEAD(env, start_response)
        elif req.method == 'POST':
            return self.POST(env, start_response)
        elif req.method == 'DELETE':
            return self.DELETE(env, start_response)
        elif req.method != 'PUT':
            return self.app(env, start_response)

        #
        # If none of the above, it must be a PUT
        #
        if 'QUERY_STRING' in env:
            args = dict(urlparse.parse_qsl(env['QUERY_STRING'], 1))
        else:
            args = {}

        path = None

        if 'partNumber' in args:
            partNo = args['partNumber']

        uploadId = None
        if 'uploadId' in args:
            uploadId = args['uploadId']
        if 'QUERY_STRING' in env:
            del env['QUERY_STRING']

        if 'HTTP_CONTENT_MD5' in env.items():
            value = env['HTTP_CONTENT_MD5']
            if value == '':
                return get_err_response('InvalidDigest')
            try:
                env['HTTP_ETAG'] = value.decode('base64').encode('hex')
            except:
                return get_err_response('InvalidDigest')

            if env['HTTP_ETAG'] == '':
                return get_err_response('SignatureDoesNotMatch')

        #
        # If this is part of an S3 multipart upload, then put the parts
        # into the proper segment container instead of the primary container
        # that was requested.  This mimics the SWIFT multipart upload behavior
        # and leaves the objects so that they can be accessed by SWIFT or S3
        # API calls.
        #
        path = env['PATH_INFO']
        if path is not None and uploadId is not None and partNo is not None:
            version, account, container, obj = split_path(path, 0, 4, True)
            env['PATH_INFO'] = ('/%s/%s/%s_segments/%s/%s/%d' %
                                (version, account, container, obj, uploadId,
                                 int(partNo)))
            env['RAW_PATH_INFO'] = ('/%s_segments/%s/%s/%08d' %
                                    (container, obj, uploadId, int(partNo)))

        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    """Standard filter factory to use the middleware with paste.deploy"""
    conf = global_conf.copy()
    conf.update(local_conf)

    def s3multi_filter(app):
        return S3MultiMiddleware(app, conf)

    return s3multi_filter
