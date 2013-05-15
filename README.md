S3Multi
-------

This is a PasteDeploy based application filter for the swift-proxy filter chain that will
enable support for the Swift S3 Multipart Upload API.   This filter must be added to the
SWIFT proxy-server.conf (/etc/swift/proxy-server.conf) file AFTER the swift3, s3token, and any 
authentication filters (keystone, tempauth, etc) in the pipeline, but prior to the proxy-server itself.

PasteDeploy reference:
http://pythonhosted.org/twod.wsgi/manual/paste-factory.html

Dependencies
------------
This module depends heavily on the fujita swift3 (http://github.com/fujita/swift3) version 1.7.0 or later.

Install
-------
1) Install S3Multi with ``sudo python setup.py install`` or ``sudo python
   setup.py develop`` or via whatever packaging system you may be using.

2) Edit the Swift proxy-server pipeline (/etc/swift/proxy-server.conf) and put the 's3multi' entry in the pipeline AFTER the authentication and swift3 and s3token middleware filters.

Example Using Keystone:

	Was:
	[pipeline:main]
		pipeline = catch_errors cache swift3 s3token tempurl authtoken keystone proxy-logging proxy-server
       
	New:
	[pipeline:main]
		pipeline = catch_errors cache swift3 s3token tempurl authtoken keystone s3multi proxy-logging proxy-server
		                                                                        ^^^^^^^

3) Add the s3multi filter definition to the end of /etc/swift/proxy-server.conf:

	[filter:s3multi]
	use = egg:s3multi#s3multi

S3 Multipart Upload Operations Supported
----------------------------------------
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
This filter translates the S3 multipart requests into the correct SWIFT calls, which in some cases may result in
multiple swift API calls for a single multipart S3 API request.  A completed S3 multipart upload will look just
like a completed multipart SWIFT upload.  The uploaded object will appear in the requested container as a 
single object, but a separate container (named as the original container name + '_segments') will be
created to hold the individual parts, exactly as it is done when doing a segmented SWIFT upload.  The folder
name of the segments is slightly different than for swift, but the result is the same.

Just as with SWIFT segmented uploads, if individual segments are deleted, downloads of the aggregated object
will result in an incomplete object.  Also, deleting the aggregate object does not automatically delete the
segmented container or segments in the container.

TBD
---
- Write unit test modules
