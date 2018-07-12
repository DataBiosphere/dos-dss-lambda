#!/usr/bin/env python3


from bdbag import bdbag_api
import json
import os
import requests

""", derived from a list of
    bundles, and assembles a remote-file-manifest configuration file
    that's used to create a bdbag."""

def create_rfm_from_data_objects(base_url, service_url, data_object_id):
    """Returns a remote-file-manifest from a DSS data objects ID.

    :parameter: (dict) data_object from DOS
    :returns: JSON file written to local system containing the input"""

    rfm = {}  # dict to hold the manifest

    dataobject = DSSDataObject(base_url, service_url, data_object_id)

    return dataobject.get_object()['id']

def make_bag(self, rfm_fname):
    '''data_to_json'''
    rfm_fname = 'remote-file-manifest.json'
    with open(rfm_fname, 'w') as fp:
        json.dump(self.data_obj, fp)


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


class DSSDataObject:

    def __init__(self, base_url, service_url, data_object_id):
        self.base_url = base_url
        self.service_url = service_url
        self.data_object_id = data_object_id

    def __get_data_object_url(self):
        """
        :return: (string) URL
        """
        data_object_url = os.path.join(self.service_url,
                                       self.base_url,
                                       'dataobjects',
                                       self.data_object_id)
        return data_object_url

    def get_object(self):
        """
        
        :return: 
        """
        data_object_url = self.__get_data_object_url()
        data_object = requests.get(data_object_url).json()['data_object']
        return data_object


# class RemoteToBag:
#     """Creates a BDBag from a remote-file-manifest (see
#     https://github.com/fair-research/bdbag/blob/master/doc/config.md). The
#     input is a data bundle.
#
#     This class has a methods that processes the content of a list of DSS
#     data bundles, breaks it down to the level of data objects, and creates a
#     remote-manifest-file from those data objects. The remote-manifest-file is
#     then used as a configuration file to create a BDBag.
#     """
#
#     def __init__(self, base_url, service_url):
#         self.base_url = base_url
#         self.service_url = service_url
#
#     def get_data_object(self, data_bundle_idx):
#         """
#         :parameter idx: (integer) index to data bundle
#         :return: (dict) data object
#         """
#         data_bundle = self.get_data_bundle(data_bundle_idx)
#         return data_bundle
#         # data_object_url = "{}/{}/dataobjects/{}".format(self.service_url,
#         #                                                 self.base_url,
#         #                                                 data_bundle['id'])
#         # r = requests.get(data_object_url)
#         # if r.status_code == 200:
#         #     return r.json()['data_object']
#         # else:
#         #     return {'status_code': r.status_code}
#
