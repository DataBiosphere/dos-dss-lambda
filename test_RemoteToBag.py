#!/usr/bin/env python3


import unittest
import os
import json
import requests
from pprint import pprint
from remote_to_bag import RemoteToBag
from bdbag import bdbag_api

class Test_RemoteToBag(unittest.TestCase):

    def setUp(self):
        self.remote_file_manifest = [
            {
                "url":"https://raw.githubusercontent.com/fair-research/bdbag/master/profiles/bdbag-profile.json",
                "length":699,
                "filename":"bdbag-profile.json",
                "sha256":"eb42cbc9682e953a03fe83c5297093d95eec045e814517a4e891437b9b993139"
            },
            {
                "url":"ark:/88120/r8059v",
                "length": 632860,
                "filename": "minid_v0.1_Nov_2015.pdf",
                "sha256": "cacc1abf711425d3c554277a5989df269cefaa906d27f1aaa72205d30224ed5f"
            }
        ]

        self.rfm_fname = os.path.join(os.getcwd(), 'rfm.json')
        with open(self.rfm_fname, 'w') as fp:
            json.dump(self.remote_file_manifest, fp)
        self.bag_path = os.path.join(os.getcwd(), 'bag_path')

        # Get data objects.
        service_url = "https://ekivlnizh1.execute-api.us-west-2.amazonaws.com/api"
        base_url = "ga4gh/dos/v1"
        list_data_bundles_url = "{}/{}/{}".format(service_url, base_url,
                                                  "databundles")
        data_bundles = requests.get(list_data_bundles_url).json()['data_bundles']
        data_bundle_url = "{}/{}/databundles/{}".format(service_url, base_url,
                                                        data_bundles[0]['id'])
        self.data_bundle = requests.get(data_bundle_url).json()['data_bundle']
        self.data_object_id = self.data_bundle['data_object_ids'][0]
        data_object_url = "{}/{}/dataobjects/{}".format(service_url, base_url,
                                                        self.data_object_id)
        self.data_object = requests.get(data_object_url).json()['data_object']

    def tearDown(self):
        os.remove('rfm.json')

    def test_list_from_loaded_jsonfile(self):
        with open(self.rfm_fname) as fp:
            rfm = json.load(fp)
        self.assertListEqual(self.remote_file_manifest, rfm)

    def test_make_bag_api(self):
        """This is NOT a test of a method in RemoteToBag class!! It only
        tests whether we can create a bag from the remote-file-manifest at
        all."""
        bdbag_api.ensure_bag_path_exists(self.bag_path)
        bdbag_api.make_bag(self.bag_path,
                           algs=['sha256'],
                           remote_file_manifest=self.rfm_fname)

    def test_import_object(self):
        pprint(self.data_object)

    def test_create_manifest(self):
        pass
