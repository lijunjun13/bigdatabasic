# -*- coding: utf-8 -*-
import json
import sys
import logging
import pandas as pd
import numpy as np
# import pandas.Se
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter, WeekdayLocator, DayLocator, MONDAY,YEARLY,HourLocator, MinuteLocator, SecondLocator
import matplotlib.dates as mdate
import datetime
import MySQLdb as mdb
import ConfigParser


class ScadaReader:
    inputfile = None

    def __init__(self, inputfile=None, *args, **kwargs):
        self.inputfile = inputfile

    def read_data(self):
        dataframe = pd.read_csv(filepath_or_buffer=self.inputfile)
        real_time = np.array(dataframe["real_time"])
        grTempOutdoor_1sec = np.array((dataframe["grTempOutdoor_1sec"]))
        # print real_time
        real_time_modified=[]
        for item in real_time:
            time_item = datetime.datetime.strptime(item,"%Y/%m/%d %H:%M:%S")
            real_time_modified.append(time_item)
        print real_time_modified
        # print grTempOutdoor_1sec
        # plt.plot(real_time,grTempOutdoor_1sec)
        # grTempHub_1sec.plot()
        minutes = MinuteLocator()
        seconds = SecondLocator()
        ax = plt.subplot(111)
        ax.xaxis.set_major_locator(minutes)
        ax.xaxis.set_major_formatter(mdate.DateFormatter('%Y-%m-%d %H:%M:%S'))
        plt.xticks(rotation=90)
        plt.plot(real_time_modified,grTempOutdoor_1sec)
        plt.show()
        # print


def plot_timeSeries(x,y):
    days = DayLocator()
    hours = HourLocator()
    minutes = MinuteLocator()
    seconds = SecondLocator()
    ax = plt.subplot(111)

    plt.xticks(rotation=45)
    ax.xaxis.set_major_locator(days)
    ax.xaxis.set_major_formatter(mdate.DateFormatter('%Y-%m-%d'))
    # ax.xaxis.set_minor_locator(hours)
    # ax.xaxis.set_minor_formatter(mdate.DateFormatter('%H'))
    plt.plot(x, y)
    plt.show()

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding("utf-8")
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%d %b %Y %H:%M:%S',
                        args=(sys.stderr, ))
    try:
        cf = ConfigParser.ConfigParser()
        cf.read("../conf/database.conf")
    except:
        logging.warning("unable to read config file!")

    try:
        conn = mdb.connect(host=cf.get("mysql", "host"), port=int(cf.get("mysql", "port")),
                           user=cf.get("mysql", "user"), passwd=cf.get("mysql", "password"),
                           db=cf.get("mysql", "db"), charset=cf.get("mysql", "charset"))
    except:
        logging.warning("unable to connect database")

    cursor = conn.cursor()
    sql="SELECT real_time,grTempOutdoor_1sec FROM turbine_scada ORDER BY real_time ASC;"
    cursor.execute(sql)
    rows = cursor.fetchall()
    times = []
    datas = []
    for row in rows:
        times.append(row[0])
        datas.append(row[1])

    plot_timeSeries(times,datas)
    # scada_reader = ScadaReader("../data/turbine_scada.csv")
    # scada_reader.read_data()