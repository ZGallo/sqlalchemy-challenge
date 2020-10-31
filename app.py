import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import datetime as dt
from flask import Flask, jsonify
import pandas as pd
from dateutil.relativedelta import relativedelta

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################



@app.route("/")
def welcome():
    "List all available api routes"
    
    session = Session(engine)
    #calcul min and max date
    min_date = session.query(
        func.min(Measurement.date)).first()[0]
    max_date = session.query(
        func.max(Measurement.date)).first()[0]
    
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/year/month/day<br>"
        f"/api/v1.0/start_date/end_date<br/>"
        f"<br/>"
        f"Available Dates:<br>"
        f"choose dates between {min_date} and {max_date}"
    )
        
        
#/api/v1.0/precipitation
@app.route("/api/v1.0/precipitation")
def precipitation():
    
    session = Session(engine)
    
    #Convert the query results to a dictionary using date as the key and prcp as the value.

    #retrieving last 12 months
    last_date = dt.date(2017, 8, 23)
    last_twelve_months = last_date - relativedelta(months= 12)

    #query design
    last_twelve_data = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date > last_twelve_months).\
        order_by(Measurement.date)
    
    #data preparation
    dates = [date[0] for date in last_twelve_data]
    prcps =  [prcp[1] for prcp in last_twelve_data]

    #creating a data frame
    last_twelve_df = pd.DataFrame({
        "date": dates,
        "prcp": prcps
    })

    #data cleaning
    last_twelve_df = last_twelve_df.dropna().sort_values('date')
    last_twelve_dict = last_twelve_df.set_index('date').to_dict()    
    
    session.close()
    #Return the JSON representation of your dictionary.
    return jsonify(last_twelve_dict)
    


#/api/v1.0/stations
@app.route("/api/v1.0/stations")
def stations():
    
    session = Session(engine)
    
    #Return a JSON list of stations from the dataset.
    results = session.query(Station.station, Station.name).all()
    
    session.close()
    return jsonify(results)



#/api/v1.0/tobs
@app.route("/api/v1.0/tobs")
def tobs():
    
    session = Session(engine)
    
    #Query the dates and temperature observations of the most active station 
    #for the last year of data.
    
    #retrieving last 12 months date
    last_date = dt.date(2017, 8, 23)
    last_twelve_months = last_date - relativedelta(months= 12)
    
    #extracting last year data
    last_twelve_data2 = session.query(Measurement.station, Measurement.tobs).\
        filter(Measurement.date > last_twelve_months).all()

    #data preparation
       #data preparation
    names = [name[0] for name in last_twelve_data2]
    tobs =  [tob[1] for tob in last_twelve_data2]

    #creating a data frame
    tobs_df = pd.DataFrame({
        "name": names,
        "tobs": tobs
    })
    
    #identifying station 
    counts_df =  tobs_df.groupby('name').count().sort_values('tobs', ascending = False)
    target_station = counts_df.index[0]
    
    
    result = session.query( Measurement.date, Measurement.tobs).\
        filter(Measurement.date > last_twelve_months).\
        filter(Measurement.station == target_station).all()
    
    session.close()
    #Return a JSON list of temperature observations (TOBS) for the previous year.
    return jsonify(result)

    

#/api/v1.0/<start> and /api/v1.0/<start>/<end>
@app.route("/api/v1.0/<year>/<month>/<day>")
def date_data(year, month, day):

    #Return a JSON list of the minimum temperature, the average temperature, 
    #and the max temperature for a given start or start end range.

    #When given the start only, calculate TMIN, TAVG, and TMAX for all dates greater than and equal to the start date.
    
    session = Session(engine)
    
    initial_date = dt.date(int(year),int(month),int(day))
    
    max_temp = session.query(Measurement.station, func.max(Measurement.tobs)).\
        filter(Measurement.date >= initial_date).all()[0][1]

    min_temp = session.query(Measurement.station, func.min(Measurement.tobs)).\
        filter(Measurement.station >= initial_date).all()[0][1]

    avg_temp = session.query(Measurement.station, func.avg(Measurement.tobs)).\
        filter(Measurement.station == initial_date).all()[0][1]

    session.close()
    
    results = {"Max Temperature" : max_temp,
              "Min Temperature": min_temp,
              "Average Temperature": avg_temp}
    
    return jsonify(results)
    
#When given the start and the end date, calculate the TMIN, TAVG, and TMAX for dates between the start and end date inclusive.
@app.route("/api/v1.0/<start_date>/<end_date>")
def date_between(start_date, end_date):

    session = Session(engine)

    max_temp = session.query(Measurement.station, func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).\
        filter(Measurement.date <= end_date).all()[0][1]
    
    min_temp = session.query(Measurement.station, func.min(Measurement.tobs)).\
        filter(Measurement.date >= start_date).\
        filter(Measurement.date <= end_date).all()[0][1]
    

    avg_temp = session.query(Measurement.station, func.avg(Measurement.tobs)).\
        filter(Measurement.date >= start_date).\
        filter(Measurement.date <= end_date).all()[0][1]

    

    dictionary = {"Min temp":minimum,"Max temp":maximum,"Average":average}

    return jsonify(dictionary)

if __name__ == "__main__":
    app.run(debug=True)
