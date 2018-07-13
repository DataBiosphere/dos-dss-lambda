#!/usr/bin/env python3


from bdbag import bdbag_api
import json
import os
import requests

# Code creates a BDBag from data bundles from the DSS. The bag is created
# froma remote-file-manifest (see
# https://github.com/fair-research/bdbag/blob/master/doc/config.md). The
# input is a data bundle.
#
# This class has a methods that processes the content of a list of DSS
# data bundles, breaks it down to the level of data objects, and creates a
# remote-manifest-file from those data objects. The remote-manifest-file is
# then used as a configuration file to create a BDBag.
# derived from a list of bundles, and assembles a remote-file-manifest
# configuration file that's used to create a bdbag.


def create_dict_for_rfm(base_url, service_url, data_object_id, local_fname_id):
    """Returns a single dictionary of a remote-file-manifest, which is a 
     list of dictionaries.
    :parameter: 
    :returns: (dict)"""

    dataobject = DSSDataObject(base_url, service_url, data_object_id)
    url_ = dataobject.data_object_url
    length_ = dataobject.get_file_size()
    filename_ = 'dss_data_object_' + str(local_fname_id)

    keys = ['url', 'length', 'filename']
    vals = [url_, length_, filename_]
    d1 = dict(zip(keys, vals))
    d2 = dataobject.get_checksums()

    return dict(d1, **d2)  # concatenate

def make_bag(self, rfm_fname):
    '''data_to_json'''
    rfm_fname = 'remote-file-manifest.json'
    with open(rfm_fname, 'w') as fp:
        json.dump(self.data_obj, fp)


class DSSDataObject:
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
        :return: 
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

    def __init__(self, base_url, service_url):
        self.base_url = base_url
        self.service_url = service_url

    def get_url(self):
        """Return URL to DSS data bundles.
        :returns: (string) URL"""
        return os.path.join(self.service_url,
                            self.base_url,
                            'databundles')

    def get_list(self):
        """Returns list of DSS data bundles.
        TODO: expand status code error checking.
        :returns: (list) response object"""
        data_bundles_url = self.get_url()
        r = requests.get(data_bundles_url)
        if r.status_code == 200:
            return r.json()['data_bundles']
        else:
            return r

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
