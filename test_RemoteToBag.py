#!/usr/bin/env python3


import unittest
import os
import json
import shutil
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

    def tearDown(self):
        #shutil.rmtree(self.bag_path)
        os.remove('rfm.json')

    def test_list_from_loaded_jsonfile(self):
        with open(self.rfm_fname) as fp:
            rfm = json.load(fp)
        self.assertListEqual(self.remote_file_manifest, rfm)

    def test_make_bag(self):
        bdbag_api.ensure_bag_path_exists(self.bag_path)
        bdbag_api.make_bag(self.bag_path,
                           algs=['sha256'],
                           remote_file_manifest=self.rfm_fname)
