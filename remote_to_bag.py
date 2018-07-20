#!/usr/bin/env python3

from bdbag import bdbag_api
import json
import os
import shutil
import requests
import tempfile
from pprint import pprint

"""Create a BDBag using a remote-file-manifest (RFM)
(see https://github.com/fair-research/bdbag/blob/master/doc/config.md). 

The input is a list of DSS Data Bundles and iterates over those bundles. Each
bundle contains a list of DSS Data Object ID. It iterates over the data
objects (which are dictionaries). The code then reformats keys and values
in those dictionaries and creates a list where each item represents the URL
to the data object and additional information required by the 
remote-manifest-file.

The remote-manifest-file is then used as a configuration file to create a BDBag.
"""


def make_bag(data_bundles, service_url, base_url):
    """
    Takes a list of remote-file-manifest compliant dictionaries, writes
    them to a temporary file, and creates a BDBag from it.
    The inputs to the bdbag_api method is a bit confusing. 
    :param self: 
    :param data_bundles: 
    :param rfm_fname: 
    :return: 
    """
    L = create_list_of_dicts_for_rfm(data_bundles, service_url, base_url)

    bag_path = os.path.join(os.getcwd(), 'bag_path')
    old_path = os.getcwd()
    # Create temporary directory and write out JSON file.
    tmp_dir = tempfile.mkdtemp()
    os.chdir(tmp_dir)
    rfm_fname = os.path.join(os.getcwd(), 'remote-file-manifest.json')
    with open(rfm_fname, 'w') as fp:
        json.dump(L, fp)
    # Create BDBag in temporary directory.
    bdbag_api.ensure_bag_path_exists(bag_path)
    bdbag_api.make_bag(bag_path,
                       algs=['sha256'],
                       remote_file_manifest=rfm_fname)
    os.chdir(old_path)
    shutil.rmtree(tmp_dir)

    #return os.path.abspath(rfm_fname)


def create_list_of_dicts_for_rfm(data_bundles, service_url, base_url):
    """Returns a list of dictionaries, which are compliant with the 
    remote-file-manifest format.
    :param data_bundles: 
    :param service_url: 
    :param base_url: 
    :return data_object_ids: (list) of RFM dictionaries
    """
    data_object_ids = []  # list of data object IDs
    for bundle_id in data_bundles:
        bundle = DSSBundle(service_url, base_url, bundle_id['id'])
        data_object_ids.extend(bundle.get_data_object_list())

    L = []  # to sample list of dictionaries
    for fname_id, data_object_id in enumerate(data_object_ids):
        data_object = DSSDataObject(base_url, service_url, data_object_id)
        L.append(create_dict_for_rfm(data_object, fname_id))

    return L


def create_dict_for_rfm(data_object, local_fname_id):
    """Returns a single dictionary of a remote-file-manifest.
     :parameter data_object: (obj) DSS Data Object
     :parameter local_fname_id: (int) used a suffix to `file_name`
     :returns: (dict): a dictionary whose keys are compliant with the RFM"""

    url_ = data_object.data_object_url
    length_ = data_object.get_file_size()
    filename_ = 'dss_data_object_' + str(local_fname_id)

    keys = ['url', 'length', 'filename']
    vals = [url_, length_, filename_]
    d1 = dict(zip(keys, vals))
    d2 = data_object.get_checksums()

    return dict(d1, **d2)  # concatenate


class DSSDataObject:
    """Contains methods to process DSS data objects to facilitate creation 
    BDBags using remote-file-manifest. """

    def __init__(self, base_url, service_url, data_object_id):
        self.base_url = base_url
        self.service_url = service_url
        self.data_object_id = data_object_id
        self.data_object_url = os.path.join(self.service_url,
                                            self.base_url,
                                            'dataobjects',
                                            self.data_object_id)
        self.data_object = requests.get(self.data_object_url).json()['data_object']

    def get_object(self):
        """
        :return: DSS Data Object 
        """
        return self.data_object

    def get_file_size(self):
        """
        :returns: file size or length of the data object"""

        # Some data objects have a size attribute, check first...
        d = self.data_object  # is a dict
        if 'size' in d.keys():
            if d['size']:
                return d['size']
        elif d['urls']:  # ...while in other objects we check in the first file.
            if d['urls'][0]['url']:
                file_url = d['urls'][0]['url']
                r = requests.head(file_url)
                if r.status_code == 200:
                    return r.headers['X-DSS-SIZE']
                else:
                    r.raise_for_status()

    def get_checksums(self):
        L = self.data_object  # list
        if L['checksums']:
            L = L['checksums']
            checksum_types = [d['type'] for d in L]
            check_sums = [d['checksum'] for d in L]
            return dict(zip(checksum_types, check_sums))

    def to_disk(self, json_fname):
        """
        :return: writes data object to disk as JSON file 
        """
        data_object_json = json.dumps(self.data_object)

        with open(json_fname, 'w') as fp:
            json.dump(data_object_json, fp)


class DSSBundle:
    """Has a methods that operate on DSS Data Bundles.
    """

    def __init__(self, service_url, base_url, data_bundle_id):
        self.bundle_url = os.path.join(service_url, base_url,
                                  'databundles', data_bundle_id)
        self.data_bundle = requests.get(self.bundle_url).json()['data_bundle']

    def display(self):
        return pprint(self.data_bundle)

    def get_url(self):
        """Return URL to data bundle.
        :returns: (string) URL"""
        return self.bundle_url

    def get_data_object_list(self):
        """Returns list of DSS data bundles.
        TODO: expand status code error checking.
        :returns: (list) containing data object IDs"""
        return self.data_bundle['data_object_ids']


    def get_num_data_objects(self):
        """
        :return n_data_objects: (int) number of DSS Data Objects 
        """
        return len(self.data_bundle['data_object_ids'])

    def __get_bundle(self, idx):
        """
        :returns (dict) one DSS data bundle"""

        data_bundles = self.get_list()
        try:
            bundle = data_bundles[idx]
        except IndexError:
            bundle = 'index out of range'
        return bundle

    def get_data_bundle(self, bundle_idx):
        """
        :param bundle_idx: (integer) the index of the bundle in the list
        :return: (dict) DSS data bundle
        """
        bundle_id = self.__get_bundle(bundle_idx)['id']
        bundle_url = os.path.join(self.service_url,
                                  self.base_url,
                                  'databundles',
                                  bundle_id)
        bundle = requests.get(bundle_url).json()['data_bundle']
        return bundle

    def list_data_object_ids(self, bundle_idx):
        """   
        :param bundle_idx: 
        :return: (list) of DSS data object IDs
        """
        bundle = self.get_data_bundle(bundle_idx)
        return bundle['data_object_ids']
