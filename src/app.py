import os
from flask import Flask, jsonify, abort, request
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import logging


data = [
    {
          "dateRep" : "23/10/2022",
          "day" : 23,
          "month" : 10,
          "year" : 2022,
          "cases" : 3557,
          "deaths" : 0,
          "countriesAndTerritories" : "Austria",
          "geoId" : "AT",
          "countryterritoryCode" : "AUT",
          "popData2020" : "8901064",
          "continentExp" : "Europe"
       },
       {
          "dateRep" : "22/10/2022",
          "day" : 22,
          "month" : 10,
          "year" : 2022,
          "cases" : 5494,
          "deaths" : 4,
          "countriesAndTerritories" : "Austria",
          "geoId" : "AT",
          "countryterritoryCode" : "AUT",
          "popData2020" : "8901064",
          "continentExp" : "Europe"
       },
       {
          "dateRep" : "21/10/2022",
          "day" : 21,
          "month" : 10,
          "year" : 2022,
          "cases" : 7776,
          "deaths" : 4,
          "countriesAndTerritories" : "Austria",
          "geoId" : "AT",
          "countryterritoryCode" : "AUT",
          "popData2020" : "8901064",
          "continentExp" : "Europe"
       }
    ,
]

app = Flask(__name__)

#Logging configuration
logging.basicConfig(filename='logs/debug.log',level=logging.DEBUG,format='%(asctime)s %(levelname)s:%(message)s', datefmt='%m/%d/%Y %H:%M:%S')
logging.disable(logging.DEBUG)

conf = SparkConf().setAppName('Covid Data')
spark = SparkContext(conf=conf)
sqlContext = SQLContext(spark)

logging.info('Reading the input CSV file into DataFrame')
csv_file = '/input/data.csv'
# Code to read the latest file from S3
# csv_file = spark.read.csv("s3://my-bucket-name-in-s3/foldername/data.csv")

df = spark.read.options(header='true', inferschema='true').csv(csv_file)


@app.route("/", methods=["GET"])
def get_days():
    return jsonify(data)

@app.route('/rolling_five_days', methods=["GET"])

def rolling_five_days():
    
    country_Code = request.args.get('countryterritoryCode')
    #http://127.0.0.1:8080/query-rolling_five_days?countryterritoryCode=AUT

    try:
        df.createOrReplaceTempView("Coviddata")
        logging.info('Creating temp table Coviddata on DF inside rolling_five_days')

        result = spark.sql(""" SELECT * FROM Coviddata 
          WHERE countryterritoryCode = {country_Code} order by dateRep DESC LIMIT 5 """)
        
        logging.info('Getting data from temp table Coviddata by passing country_Code')

        if len(result) == 0:
            abort(404)

        return jsonify(result)
    
    except Exception as e:
        print(e)
        return e


@app.route('/total_data', methods=["GET"])

def total_data():

    try:
        df.createOrReplaceTempView("Coviddata")
        logging.info('Creating temp table Coviddata on DF inside total_data')

        result_total_data = spark.sql(""" SELECT countriesAndTerritories,
          SUM(cases) AS total_covid_cases 
          FROM Coviddata 
          GROUP BY countriesAndTerritories """)
        
        logging.info('Getting total cases for each country')
        

        if len(result_total_data) == 0:
            abort(404)

        return jsonify(result_total_data)
    
    except Exception as e:
        print(e)
        return e


@app.route("/", methods=["POST"])
def post_days():
    return jsonify({"success": True}), 201



if __name__ == "__main__":

    try:
        app.run(port=os.environ.get('FLASK_PORT', 8080), host='0.0.0.0')
        logging.info('Runnnig the Flask api ')

    finally:
        spark.stop()
        logging.info('Closing the spark context')
