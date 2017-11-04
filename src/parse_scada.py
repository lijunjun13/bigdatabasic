# -*- coding:utf-8 -*-
import urllib
import MySQLdb as mdb
import ConfigParser
import logging
import sys


if __name__ == "__main__":
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

    with open("../data/windfarm_data/scada81_1min.csv") as f:
        line_id=-1
        for line in f:
            print line_id
            line = line.strip()
            line_id += 1
            if line_id==0:
                cols = line.split(",")
            else:
                line = line.replace('"', "")
                fields = line.split(",")
                for i in range(0, len(fields)):
                    if fields[i]=="":
                        fields[i]="0"

                fields[0]='"'+fields[0]+'"'
                fields[1] = '"' + fields[1] + '"'
                fields[3] = '"' + fields[3] + '"'
                fields = fields[0:-1]
                fields = ",".join(fields)
                sql = "insert into turbine_scada VALUES (%d, %s)" % (line_id,fields)
                cursor.execute(sql)
                conn.commit()