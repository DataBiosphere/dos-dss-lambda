#!/usr/bin/env python3

import unittest
import os
import json
import requests
import shutil
from bdbag import bdbag_api
from remote_to_bag import DSSBundle as Bundle
from remote_to_bag import DSSDataObject as DataObject
from remote_to_bag import create_dict_for_rfm, \
    create_list_of_dicts_for_rfm, make_bag


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

        # Set up the file paths for BDBag creation.
        self.bag_path = os.path.join(os.getcwd(), 'bag_path')
        self.rfm_fname = os.path.join(os.getcwd(), 'rfm.json')
        with open(self.rfm_fname, 'w') as fp:
            json.dump(self.remote_file_manifest, fp)


        # Set URLs to data bundles.
        self.service_url = \
            "https://ekivlnizh1.execute-api.us-west-2.amazonaws.com/api"
        self.base_url = "ga4gh/dos/v1"

        self.data_object_id = '8ff23235-4435-4929-8fb2-5d55b4564999'
        self.aws_sample_url = 'https://commons-dss.ucsc-cgp-dev.org/v1/files/8ff23235-4435-4929-8fb2-5d55b4564999?replica=aws'

        self.create_dict_for_rfm = {
            'crc32c': '63439d51',
            'etag': '57db2e71deb4dab5e4b3f251ac9243b0',
            'filename': 'dss_data_object_5',
            'length': '5897',
            'sha1': '05f818a54510272c17dcda69c948f8d904b5aae3',
            'sha256': 'c873835a74cea9c811cc7799f8897ac480cccf84f631c99b5293900f7a071b53',
            'url': 'https://ekivlnizh1.execute-api.us-west-2.amazonaws.com/api/ga4gh/dos/v1/dataobjects/8ff23235-4435-4929-8fb2-5d55b4564999'}

        # List of bundles as retrieved in the notebook:
        self.list_of_bundles = [{'id': 'e9556c3d-53cb-4c24-856e-951085735d45',
                                 'version': '2018-06-07T001704'},
                                {'id': '15a0ce60-261d-4bc1-8463-f4d87aa483f0',
                                 'version': '2018-06-07T001659'},
                                {'id': 'b7003567-37a6-4f70-8be3-0e8ee5c1f020',
                                 'version': '2018-06-07T001844'}]

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

    def test_create_dict_for_rfm(self):
        data_object = DataObject(self.base_url,
                                 self.service_url, self.data_object_id)
        d = create_dict_for_rfm(data_object, local_fname_id=5)
        self.assertTrue(len(d), 7)
        self.assertDictEqual(d, self.create_dict_for_rfm)

    def test_create_list_of_dicts_for_rfm(self):
        L = create_list_of_dicts_for_rfm(self.list_of_bundles,
                                         self.service_url, self.base_url)
        self.assertTrue(type(L) == list)
        self.assertTrue(type(L[0] == dict))
        self.assertEqual(len(L), 9)

    def test_make_bag(self):
        # Write a bag into the current directory.
        # TODO: create better test, e.g., compare the bag to existing bag
        make_bag(self.list_of_bundles, self.service_url, self.base_url)
        with open('bag_path/fetch.txt', 'r') as fp:
            data = fp.read()
        # There are three URLs in the BDBag.
        self.assertEqual(data.count('\n'), 3)
        # Make sure each line starts with `https`.
        self.assertTrue(
            all([line.startswith('https') for line in data.splitlines()]))
        shutil.rmtree('bag_path')



class TestDSSDataBundle(unittest.TestCase):
    def setUp(self):
        # Set URLs to data bundles.
        self.service_url = \
            "https://ekivlnizh1.execute-api.us-west-2.amazonaws.com/api"
        self.base_url = "ga4gh/dos/v1"
        self.data_bundle_id = '41680495-06a3-4963-9d2c-9280c6e9979b'
        self.bundle = Bundle(self.service_url,
                             self.base_url, self.data_bundle_id)

    def test_get_data_object_list(self):
        L = ['e8d5b3b5-1a49-4765-8670-e39860cce6c5',
             '82ca0d40-1d74-4da8-a059-49bc61de919c',
             '4f321007-7560-4594-8c92-77b02d574c51']
        self.assertListEqual(self.bundle.get_data_object_list(), L)

    def test_get_num_data_objects(self):
        self.assertEqual(self.bundle.get_num_data_objects(), 3)


class TestDSSDataObject(unittest.TestCase):

    def setUp(self):

        # Set URLs to data bundles.
        self.service_url = \
            "https://ekivlnizh1.execute-api.us-west-2.amazonaws.com/api"
        self.base_url = "ga4gh/dos/v1"
        self.data_object_id1 = '8ff23235-4435-4929-8fb2-5d55b4564999'
        self.data_object_id2 = '46df9ac5-0e72-4aa5-8e6b-6b55b362c29a'

    def test_get_file_size(self):
        # Size from object.
        dataobject = DataObject(self.base_url,
                                self.service_url,
                                self.data_object_id2)
        d = dataobject.get_object()
        self.assertEqual(dataobject.get_file_size(), d['size'])

        # Size extracted first URL in self.data_object_id1.
        dataobject = DataObject(self.base_url,
                                self.service_url,
                                self.data_object_id1)
        s3_url = ('https://commons-dss.ucsc-cgp-dev.org/v1/files/'
                  '8ff23235-4435-4929-8fb2-5d55b4564999?replica=aws')
        r = requests.head(s3_url)
        size_from_s3_url = r.headers['X-DSS-SIZE']
        self.assertEqual(dataobject.get_file_size(), size_from_s3_url)

    def test_get_checksums(self):
        dataobject = DataObject(self.base_url,
                                self.service_url,
                                self.data_object_id1)
        d = dataobject.get_checksums()
        self.assertEqual(d['sha1'], '05f818a54510272c17dcda69c948f8d904b5aae3')

