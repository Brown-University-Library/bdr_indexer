import unittest

from bdr_solrizer.indexers import FitsIndexer
from . import test_data


class TestFITSIndexer(unittest.TestCase):

    def test_fields(self):
        indexer = FitsIndexer(test_data.FITS_XML)
        indexed_data = indexer.index_data()
        self.assertEqual(indexed_data['fits_image_width_lsi'], '3753')
        self.assertEqual(indexed_data['fits_image_height_lsi'], '5634')

    def test_sparse_fits(self):
        #make sure we don't crash if some/all fields are missing from FITS
        indexer = FitsIndexer(test_data.SPARSE_FITS_XML)
        indexed_data = indexer.index_data()
        self.assertEqual(indexed_data, {})


if __name__ == '__main__':
    unittest.main()

