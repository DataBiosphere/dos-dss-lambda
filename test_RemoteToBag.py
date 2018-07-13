#!/usr/bin/env python3


import unittest
import os
import json
import requests
from pprint import pprint
from remote_to_bag import DSSBundle as Bundle
from remote_to_bag import DSSDataObject as DataObject
from remote_to_bag import create_rfm_from_data_objects
from bdbag import bdbag_api

class Test_RemoteToBag(unittest.TestCase):

    def setUp(self):

        # It turns out that the creation of a bag with this package
        # only works if each file has the same number of checksums.
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

        # Set URLs to data bundles.
        self.service_url = \
            "https://ekivlnizh1.execute-api.us-west-2.amazonaws.com/api"
        self.base_url = "ga4gh/dos/v1"

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

    def test_create_rfm_from_data_objects(self):

        #databundle = Bundle(self.base_url, self.service_url)
        #pprint(databundle.get_url())
        #pprint(databundle.get_list())
        #pprint(databundle.get_data_bundle(0))
        #pprint(databundle.list_data_object_ids(0))

        data_object_id = '8ff23235-4435-4929-8fb2-5d55b4564999'
        dataobject = DataObject(self.base_url, self.service_url, data_object_id)
        #pprint(dataobject.get_object())

        dataobjid = create_rfm_from_data_objects(self.base_url,
                                                 self.service_url,
                                                 data_object_id)
        print(dataobjid)

        json_fname = 'test_data_object.json'
        dataobject.to_disk(json_fname)

        # # Create remote-to-bag instance.
        # rtb = RemoteToBag(self.base_url, self.service_url)
        # print(rtb.get_data_bundles_url())
        # pprint(rtb.get_data_bundles_list())
        # bundle = rtb.get_data_bundle(1)
        # pprint(bundle)
        # pprint(bundle['id'])
        # pprint(rtb.get_data_object(data_bundle_idx=1))




