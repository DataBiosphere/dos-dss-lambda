#!/usr/bin/env python3


from bdbag import bdbag_api
import json


class RemoteToBag:
    """Creates a BDBag from a remote-file-manifest (see 
    https://github.com/fair-research/bdbag/blob/master/doc/config.md)
    """

    def __init__(self, data_object):
        self.data_obj = data_object

    def _create_manifest(self):
        pass

    def make_bag(self, rfm_fname):
        '''data_to_json'''
        rfm_fname = 'remote-file-manifest.json'
        with open(rfm_fname, 'w') as fp:
            json.dump(self.data_obj, fp)