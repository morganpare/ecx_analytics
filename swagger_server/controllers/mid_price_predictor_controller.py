import connexion
import six
import datetime as dt

from swagger_server.models.mid_price_response import MidPriceResponse  # noqa: E501
from swagger_server.models.symbol import Symbol  # noqa: E501
from swagger_server.models.target_date import TargetDate  # noqa: E501
from swagger_server import util

import sys
import pathlib
import logging
logging.basicConfig(level = logging.INFO)

FILEPATH = pathlib.Path(__file__).parent.resolve()
PROJECTPATH = FILEPATH.parent.parent.resolve()

sys.path.append(str(PROJECTPATH))

from ecx_analytics.price_predictor.predict import ECXPredictor


def mid_price_prediction(symbol, target_date):  # noqa: E501
    """Predicts mid price for supplied symbol and target date

    Provides prediction for the mid price of a coffee (represented by a symbol) on a target date # noqa: E501

    :param symbol: Coffee identifying symbol
    :type symbol: dict | bytes
    :param target_date: Target date to predict for
    :type target_date: dict | bytes

    :rtype: List[MidPriceResponse]
    """

    if symbol not in ['LUBP4','LUBP3','ULK5','UFRAUG']:
        return "Symbol not supported, please supply one of ['LUBP4','LUBP3','ULK5','UFRAUG']", 404

    try:
        target_date = dt.datetime.strptime(target_date, '%Y-%m-%d')
    except:
        return "Can't parse date, please provide in iso date format", 400

    if target_date.year != 2018 or target_date.month != 4:
        return "Prediction only supported for April 2018, please provide date in this month"

    predictor = ECXPredictor(PROJECTPATH)
    prediction = predictor.predict(symbol, target_date)

    return [prediction]
