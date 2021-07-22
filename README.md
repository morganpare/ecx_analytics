![alt text](https://images.unsplash.com/photo-1524350876685-274059332603?ixid=MnwxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8&ixlib=rb-1.2.1&auto=format&fit=crop&w=751&q=80)

# Ethiopian Coffee Exchange (ECX) Analytics Suite

This project was designed to give a basic set of analytics tools for historical data relating to the Ethiopian Coffee Exchange. In particular the initial scope is to provide a proof-of-concept market price prediction API alongside a set of visualisations tool for understanding market characteristics in the past 7 years.

## Installation

This repo is python based, so you should use whichever environment manager you prefer to install the relavent packages found in the `requirements.txt` file.

You should also download the relevant dataset from Kaggle Datasets here https://www.kaggle.com/khalidsultan/cdatasets . Create a data directory in the home directory with the following structure:
- data
  - bronze
  - silver
  - gold
  - feature_store

Extract the excel files into the bronze directory and delete the original `.zip` file.

## Usage

The project has three core functionalities:
* Data Processor
* Price Predictor
* Visualisation

### Data Processor
To run the data processing step, use the `data_processor_main.py` script found within the respective layer of the `ecx_analytics` package. This will populate the data directory with the appropriate data. This data model mimics the delta lake pattern https://databricks.com/blog/2019/08/14/productionizing-machine-learning-with-delta-lake.html in a lightweight manner.

## Price Predictor

The price prediction functionality uses a swagger-based endpoint, this can be running by issuing the command `python -m swagger_server` whilst in the price prediction layer of the package. This will spawn a local server which can be accessed via the browser. 

Once the local server is running, you can issue REST API calls to the server endpoints. Specifying the warehouse ID and target date in April of 2018 will return a prediction for the mid price.

## Visualisation
TBC

## License

  
MIT License

Copyright (c) 2021 Morgan Pare

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

