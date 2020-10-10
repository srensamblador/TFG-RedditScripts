import numpy as np
import progressbar as pb

from datetime import date
from datetime import timedelta
from datetime import datetime as dt

def print_date(*args):
    for date in args:
        print (date.strftime("%d-%m-%Y %H:%M:%S"))

timestamp = 1539897781

date = dt.fromtimestamp(timestamp)
print("Original: ", end="")
print_date(date)


beginning_day = dt(date.year, date.month, date.day, hour=00, minute=00, second=00)
end_day = dt(date.year, date.month, date.day, hour=23, minute=59, second=59)

half_day_beginning = max(beginning_day, date - timedelta(hours=12))
half_day_end = min(end_day, date + timedelta(hours=12))


print_date(half_day_beginning, half_day_end)