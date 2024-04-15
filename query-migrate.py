from dateutil.relativedelta import relativedelta
import datetime

query = "\\echo Processing {start}\n\\copy (select id,endtime,temporarybuild,repourlog,buildlog from buildrecord where " \
        + "submittime > '{start}' and submittime < '{end}' order by submittime asc) to '{filename}' with csv;"

start_date = "2023-01-01"
stop_date = "2024-04-05"

delta_increment = relativedelta(days=7)

start_datetime = datetime.datetime.strptime(start_date, "%Y-%m-%d")
stop_datetime = datetime.datetime.strptime(stop_date, "%Y-%m-%d")

while True:

    delta = stop_datetime - start_datetime
    if delta.total_seconds() < 0:
        # we are beyond the stop_datetime
        break

    end_datetime = start_datetime + delta_increment
    print(query.format(start=start_datetime.strftime("%Y-%m-%d"), end=end_datetime.strftime("%Y-%m-%d"), filename="/tmp/to-migrate/" + start_datetime.strftime("%Y-%m-%d") + ".csv"))

    start_datetime = end_datetime

