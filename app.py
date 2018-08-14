"""
dos-dss-lambda
This lambda proxies requests to the necessary DSS endpoints and converts them
to DOS messages.

"""
import logging
import os
import urlparse

from chalice import Chalice, Response
import hca.dss
import requests
import yaml

# If DSS_ENDPOINT is set, make sure it doesn't have a trailing /
DSS_ENDPOINT = os.environ.get('DSS_ENDPOINT', 'https://commons-dss.ucsc-cgp-dev.org/v1')

# tweak, which underpins the :class:`~hca.HCAConfig` object, will try to
# create a config directory if one does not exist. This is fine if we're
# running locally, but not so much if this is deployed on AWS Lambda as
# the home directory is read-only. AWS Lambda provides /tmp as a non-persistent
# staging area, so we set the `_user_config_home` variable to /tmp so that
# dos-dss-lambda doesn't die when we try to instantiate :class:`~hca.dss.DSSClient`.
hca.HCAConfig._user_config_home = '/tmp/'
config = hca.HCAConfig(save_on_exit=False, autosave=False)
config['DSSClient'].swagger_url = DSS_ENDPOINT + '/swagger.json'
dss = hca.dss.DSSClient(config=config)

app = Chalice(app_name='dos-dss-lambda', debug=True)
app.log.setLevel(logging.DEBUG)


def dss_file_to_dos(data_object_id, dss_file):
    """
    Converts a DSS file header into a Data Object.

    List of headers

    ['Content-Type', 'Content-Length', 'Connection', 'Date',
     'x-amzn-RequestId', 'X-DSS-SHA1',
     'Access-Control-Allow-Origin', 'X-DSS-S3-ETAG',
     'X-DSS-SHA256', 'X-DSS-BUNDLE-UUID',
     'Access-Control-Allow-Headers', 'X-DSS-CONTENT-TYPE',
     'X-DSS-CRC32C', 'X-DSS-CREATOR-UID', 'X-DSS-VERSION',
     'X-Amzn-Trace-Id', 'X-DSS-SIZE', 'X-Cache', 'Via',
     'X-Amz-Cf-Id']

    :param data_object_id:
    :param dss_file:
    :return:
    """

    data_object = {}
    data_object['id'] = data_object_id
    sha256 = {'checksum': dss_file.get('X-DSS-SHA256', None), 'type': 'sha256'}
    etag = {'checksum': dss_file.get('X-DSS-S3-ETAG', None), 'type': 'etag'}
    sha1 = {'checksum': dss_file.get('X-DSS-SHA1', None), 'type': 'sha1'}
    crc32c = {'checksum': dss_file.get('X-DSS-CRC32C', None), 'type': 'crc32c'}
    checksums = [sha256, etag, sha1, crc32c]
    data_object['checksums'] = checksums
    data_object['version'] = dss_file.get('X-DSS-VERSION', None)
    data_object['content_type'] = dss_file.get('X-DSS-CONTENT-TYPE', None)
    data_object['urls'] = make_urls(data_object_id, 'files')
    return data_object


def dss_list_bundle_to_dos(dss_bundle):
    """
    Converts a DSS bundle to DOS bundle messages by splitting the ID.

    :param bundle_list:
    :return:
    """
    dos_bundle = {}
    dos_bundle['id'] = dss_bundle['bundle_fqid'].split('.')[0]
    dos_bundle['version'] = dss_bundle['bundle_fqid'].split('.')[1]
    # full_bundle = requests.get(
    #     "{}/bundles/{}?replica=aws&version={}".format(
    #         DSS_URL, dos_bundle['id'], dos_bundle['version'])).json()
    # if full_bundle and full_bundle.get('files', None):
    #     for file in full_bundle.get('files'):
    #         dos_bundle['data_object_ids'].append(file['uuid'])
    return dos_bundle


def dss_bundle_to_dos(dss_bundle):
    """
    Converts a fully formatted DSS bundle into a DOS bundle.

    :param dss_bundle:
    :return:
    """
    dos_bundle = {}
    dos_bundle['id'] = dss_bundle['uuid']
    dos_bundle['version'] = dss_bundle['version']
    dos_bundle['data_object_ids'] = [x['uuid'] for x in dss_bundle['files']]
    return dos_bundle


@app.route('/swagger.json', cors=True)
def swagger():
    """
    An endpoint for returning the swagger api description.

    :return:
    """
    # FIXME replace with one hosted here
    req = requests.get("https://ga4gh.github.io/data-object-service-schemas/swagger/data_object_service.swagger.yaml")
    swagger_dict = yaml.load(req.content)

    swagger_dict['basePath'] = '/api/ga4gh/dos/v1'
    return swagger_dict


def make_urls(object_id, path):
    """
    Makes a list of URLs for each replica for a DOS message.
    :param object_id:
    :param path:
    :return:
    """
    replicas = ['aws', 'azure', 'gcp']
    urls = map(
        lambda replica: {'url': '{}/{}/{}?replica={}'.format(
            DSS_ENDPOINT, path, object_id, replica)},
        replicas)
    return urls


def convert_reference_json(reference_json, data_object):
    """
    Converts the reference JSON download from DSS into the DOS message
    and returns the DOS message.
    :param reference_json:
    :param data_object:
    :return:
    """
    # {u'content-type': u'application/octet-stream',
    # u'crc32c': u'e2a2bc04',
    # u'size': 25955827488,
    # u'url': [u'gs://topmed-irc-share/genomes/NWD145710.b38.irc.v1.cram',
    # u's3://nih-nhlbi-datacommons/NWD145710.b38.irc.v1.cram']}
    data_object['size'] = reference_json['size']
    data_object['urls'] = map(lambda x: {'url': x}, reference_json['url'])
    data_object['checksums'] = [{'checksum': reference_json['crc32c'], 'type': 'crc32c'}]
    data_object['content_type'] = reference_json['content-type']
    return data_object


@app.route('/ga4gh/dos/v1/dataobjects/{data_object_id}', methods=['GET'], cors=True)
def get_data_object(data_object_id):
    """
    This endpoint returns DataObjects by their identifier by proxying the
    request to files in DSS.
    :param data_object_id:
    :return:
    """
    dss_response = dss.head_file(uuid=data_object_id, replica='aws')
    dss_file = dss_response.headers
    if not dss_response.status_code == 200:
        return Response({'msg': 'Data Object with data_object_id {} was not found.'.format(data_object_id)}, status_code=404)
    data_object = dss_file_to_dos(data_object_id, dss_file)
    # FIXME download the extra metadata if its a file by reference
    content_key = 'fileref'
    if data_object['content_type'].find(content_key) != -1:
        reference_json = dss.get_file(replica='aws', uuid=data_object_id)
        try:
            data_object = convert_reference_json(reference_json, data_object)
        except Exception as e:
            return Response({'msg': 'Data Object with data_object_id {} was not found. {}'.format(data_object_id, str(e))},
                            status_code=404)
    else:
        replicas = ['aws', 'azure', 'gcp']
        for replica in replicas:
            # TODO make async
            try:
                data_bundle = dss.get_bundle(uuid=dss_file['X-DSS-BUNDLE-UUID'], replica=replica)
                url = filter(lambda x: x['uuid'] == data_object_id, data_bundle.json()['bundle']['files'])[0]['url']
                data_object['urls'].append({'url': url})
            except Exception as e:
                pass
    return {'data_object': data_object}


@app.route('/ga4gh/dos/v1/dataobjects', methods=['GET'], cors=True)
def list_data_objects():
    """
    This endpoint translates DOS List requests into requests against DSS
    and converts the responses into GA4GH messages.

    :return:
    """
    return Response(body='', status_code=405)


@app.route('/ga4gh/dos/v1/databundles', methods=['GET'], cors=True)
def list_data_bundles():
    """
    This endpoint translates DOS List requests into requests against DSS
    and converts the responses into GA4GH messages.

    :return:
    """
    req_body = app.current_request.query_params
    per_page = 10
    page_token = None
    next_page_token = None
    if req_body and req_body.get('page_size', None):
        per_page = req_body['page_size']
    if req_body and req_body.get('page_token', None):
        page_token = req_body['page_token']
    # We use DSSClient().post_search._request to expose the underlying
    # :class:`~requests.Request` object so that we can access the pagination
    # headers. :func:`~DSSClient().post_search._request` is undocumented.
    # The source code for the method is here:
    # https://github.com/HumanCellAtlas/dcp-cli/blob/aa811490d3c680018f6c1abeef3292098556b0ea/hca/util/__init__.py#L119
    if page_token:
        res = dss.post_search._request(dict(replica='aws', per_page=per_page,
                                            search_after=page_token, es_query={}))
    else:
        res = dss.post_search._request(dict(replica='aws', per_page=per_page, es_query={}))
    # We need to page using the github style
    if res.links.get('next', None):
        try:
            # first search_after item of the query string in the link
            # header of the response
            next_page_token = urlparse.parse_qs(
                urlparse.urlparse(
                    res.links['next']['url']).query)['search_after'][0]
        except Exception as e:
            print(e)
    # And convert the fqid message into a DOS id and version
    response = {}
    response['next_page_token'] = next_page_token
    try:
        response['data_bundles'] = map(dss_list_bundle_to_dos, res.json()['results'])
    except Exception as e:
        response = e
    finally:
        return response


@app.route('/ga4gh/dos/v1/databundles/{data_bundle_id}', methods=['GET'], cors=True)
def get_data_bundle(data_bundle_id):
    """
    This endpoint translates DOS List requests into requests against DSS
    and converts the responses into GA4GH messages.

    :return:
    """
    version = None
    if app.current_request.query_params:
        version = app.current_request.query_params.get('version', None)
    bdl = dss.get_bundle(version=version, replica='aws', uuid=data_bundle_id)
    return {'data_bundle': dss_bundle_to_dos(bdl['bundle'])}


@app.route('/')
def index():
    message = "<h1>Welcome to the DOS lambda, send requests to /ga4gh/dos/v1/</h1>"
    return Response(body=message,
                    status_code=200,
                    headers={'Content-Type': 'text/html'})
