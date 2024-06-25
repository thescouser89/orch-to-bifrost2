from dateutil.relativedelta import relativedelta
from datetime import date, datetime, timedelta
import argparse

query = "\\echo Processing {start}\n\\copy (select id,endtime,temporarybuild,repourlog,buildlog from buildrecord where " \
        + "submittime > '{start}' and submittime < '{end}' order by submittime asc) to '{filename}' with csv;"

parser = argparse.ArgumentParser(
                    prog='query-migrate.py',
                    description='Migrate logs from PNC Orch DB to logstore',
                    epilog='Have a nice day!')

parser.add_argument('--start-date', help='Format: yyyy-mm-dd')  
parser.add_argument('--stop-date', help='Format yyyy-mm-dd')  
parser.add_argument('--automated', action='store_true', help='create a query from 1 day ago till now')
args = parser.parse_args()

if not args.automated:
    if start_date is None:
        raise Exception("Start date not specified")
    if stop_date is None:
        raise Exception("stop date not specified")

    start_date = args.start_date
    stop_date = args.stop_date
    delta_increment = relativedelta(days=7)
else:
    now = date.today()
    start_date = str(now - timedelta(days = 1))
    stop_date = str(now)
    delta_increment = relativedelta(days=1)


start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
stop_datetime = datetime.strptime(stop_date, "%Y-%m-%d")

while True:

    delta = stop_datetime - start_datetime
    if delta.total_seconds() < 0:
        # we are beyond the stop_datetime
        break

    end_datetime = start_datetime + delta_increment
    print(query.format(start=start_datetime.strftime("%Y-%m-%d"), end=end_datetime.strftime("%Y-%m-%d"), filename="/tmp/to-migrate/" + start_datetime.strftime("%Y-%m-%d") + ".csv"))

    start_datetime = end_datetime

