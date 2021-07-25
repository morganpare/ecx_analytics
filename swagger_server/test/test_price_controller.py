# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from swagger_server.models.mid_price_response import MidPriceResponse  # noqa: E501
from swagger_server.models.symbol import Symbol  # noqa: E501
from swagger_server.test import BaseTestCase


class TestPriceController(BaseTestCase):
    """PriceController integration test stubs"""

    def test_mid_price_prediction(self):
        """Test case for mid_price_prediction

        Predicts mid price for supplied symbol and target date
        """
        query_string = [('symbol', Symbol())]
        response = self.client.open(
            '/price/mid_price_prediction',
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
