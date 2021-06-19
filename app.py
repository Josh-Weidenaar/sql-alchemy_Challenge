import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.pool import QueuePool
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from flask import Flask, jsonify
import datetime as dt
from sqlalchemy import and_

engine = create_engine(r"sqlite:///C:\Users\JoshWeidenaar\Documents\Bootcamp\1_Homework\sql-alchemy_Challenge\Resources\hawaii.sqlite", poolclass=QueuePool)
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

measurement = Base.classes.measurement
station = Base.classes.station

app = Flask(__name__)

@app.route("/")
def routes():
    return(
        f"Routes Include:<br/><br/>`/api/v1.0/precipitation`<br/><br/>`/api/v1.0/stations`<br/><br/>`/api/v1.0/tobs`<br/><br/>`/api/v1.0/startDate` or `/api/v1.0/startDate/endDate`"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    session = Session(engine)

    # Find the most recent date in the data set.
    firstdate = session.query(measurement.date).order_by(measurement.date.desc()).first()
    session.query(measurement.date).all()

    firstdate = str(firstdate).strip("(',)")

    firstdate = firstdate.split("-")

    year_ago = dt.date(int(firstdate[0]),int(firstdate[1]),int(firstdate[2])) - dt.timedelta(days=365)

    results = session.query(measurement.date, measurement.prcp).filter(measurement.date > year_ago).order_by(measurement.date.desc()).all()

    session.close()

    resultsList = []
    for i in results:
        resultsDict = {}
        date = i[0]
        resultsDict[date] = i[1]
        resultsList.append(resultsDict)

    return jsonify(resultsList)

@app.route("/api/v1.0/stations")
def stations():
    session = Session(engine)

    stations = session.query(station.station).all()
    
    session.close()

    stationList = []
    for i in stations:
        stationDict = {}
        stationDict['stationName'] = i[0]
        stationList.append(stationDict)
    
    return jsonify(stationList)

@app.route("/api/v1.0/tobs")
def tobs():
    session = Session(engine)

    firstdate = session.query(measurement.date).order_by(measurement.date.desc()).first()
    session.query(measurement.date).all()

    firstdate = str(firstdate).strip("(',)")

    firstdate = firstdate.split("-")

    year_ago = dt.date(int(firstdate[0]),int(firstdate[1]),int(firstdate[2])) - dt.timedelta(days=365)

    df_station = session.query(measurement.date, measurement.prcp,measurement.station).\
    filter(measurement.date > year_ago).order_by(measurement.date.desc()).all()
    df_station = pd.DataFrame(df_station)
    # Design a query to find the most active stations (i.e. what stations have the most rows?)
    df_station_summary = pd.DataFrame(df_station.groupby(by='station').date.count())
    # List the stations and the counts in descending order.
    df_station_summary.sort_values(by='date',ascending=False,inplace=True)
    df_station_summary.rename(columns={'date':'Count'},inplace=True)
    maxstation = df_station_summary.index[0]

    
    # Using the most active station id from the previous query, calculate the lowest, highest, and average temperature.
    df_tobs = session.query(measurement.date,measurement.station,measurement.tobs).\
        filter(measurement.date > year_ago).\
        filter(measurement.station == maxstation).order_by(measurement.date.desc()).all()
    resultsList = []
    for i in df_tobs: 
        resultsDict = {}
        date = i[0]
        resultsDict[date] = i[2]
        resultsList.append(resultsDict)
    
    return jsonify(resultsList)

@app.route("/api/v1.0/<start_date>")
def start(start_date):
    session = Session(engine)

    startDate = str(start_date).strip("(',)").split("-")
    startDate = dt.datetime(int(startDate[2]),int(startDate[0]),int(startDate[1]))

    results = session.query(measurement.date, measurement.tobs).filter(measurement.date > startDate).order_by(measurement.date.desc()).all()
    
    dicts = {}
    dicts['StartDate'] = startDate
    
    session.close()

    try: 
        df = pd.DataFrame(results)
        TMax = df.tobs.max()
        TMin = df.tobs.min()
        TAvg = df.tobs.mean()
        dicts['TMax'] = TMax
        dicts['TMin'] = TMin
        dicts['TAvg'] = TAvg
    except:
        dicts['TMax'] = "error"
        dicts['TMin'] = "error"
        dicts['TAvg'] = "error"

    return jsonify(dicts)


@app.route("/api/v1.0/<start_date>/<end_date>")
def startend(start_date,end_date):
    session = Session(engine)

    startDate = str(start_date).strip("(',)").split("-")
    startDate = dt.datetime(int(startDate[2]),int(startDate[0]),int(startDate[1]))
    endDate = str(end_date).strip("(',)").split("-")
    endDate = dt.datetime(int(endDate[2]),int(endDate[0]),int(endDate[1]))

    results = session.query(measurement.date, measurement.tobs).filter(and_(measurement.date >= startDate, measurement.date <= endDate)).order_by(measurement.date.desc()).all()
    
    dicts = {}
    dicts['StartDate'] = startDate
    dicts['EndDate'] = endDate
    session.close()

    try: 
        df = pd.DataFrame(results)
        TMax = df.tobs.max()
        TMin = df.tobs.min()
        TAvg = df.tobs.mean()
        dicts['TMax'] = TMax
        dicts['TMin'] = TMin
        dicts['TAvg'] = TAvg
    except:
        dicts['TMax'] = "error"
        dicts['TMin'] = "error"
        dicts['TAvg'] = "error"

    return jsonify(dicts)
if __name__ == "__main__":
    app.run(debug=True)