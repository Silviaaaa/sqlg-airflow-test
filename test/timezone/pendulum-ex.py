#
# https://towardsdatascience.com/pendulum-one-of-the-most-useful-python-libraries-you-have-ever-seen-e2ecc365c8c0
#
# (logical_date.astimezone('UTC')).strftime('%Y%m%d') error
# add on 2016, remove 2017 Improves compatibility with stdlib 
# 

import pendulum as pdl
from datetime import datetime

from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta


ex_count = 0

ex_count += 1
dt = pdl.datetime(2021, 11, 6)
print(f'\nex-{ex_count}=> Drop-In Replacement to datetime')
print(isinstance(dt, datetime))


ex_count += 1
print(f'\nex-{ex_count}=> Timezones')
dt_melbourne = pdl.datetime(2021, 11, 6, tz='Australia/Melbourne')
dt_brisbane = pdl.datetime(2021, 11, 6, tz='Australia/Queensland')
print(dt_melbourne)
print(dt_brisbane)


ex_count += 1
print(f'\nex-{ex_count}=> Timezones, compare with diff TZ')
dt_melbourne.diff(dt_brisbane).in_hours()
my_timezone = pdl.timezone('Australia/Melbourne')
dt_melbourne = pdl.datetime(2021, 11, 6, tz=my_timezone)
print(dt_melbourne)
print(dt_melbourne.timezone.name)


ex_count += 1
print(f'\nex-{ex_count}=> Datetime Parsing')
print(pdl.from_format('2021-11-06 22:00:00', 'YYYY-MM-DD HH:mm:ss'))
print(pdl.parse('2021-11-01 22:00:00'))
print(pdl.parse('21-11-06', strict=False))
print(pdl.parse('20211106'))
print(pdl.parse('2021-W44-6'))
print(pdl.parse('20211106', exact=True))
print(pdl.parse('22:00:00', exact=True))


ex_count += 1
print(f'\nex-{ex_count}=> String Formatting')
dt = pdl.now()
print(dt.to_date_string())  # with date only
print(dt.to_time_string())  # with time only
print(dt.to_formatted_date_string())  # month_abbr date, year
dt.to_day_datetime_string() # day, month_abbr date, year hh:mm am/pm
dt.to_iso8601_string()  # to ISO 9601 standard
dt.to_atom_string()  # to Atom format
print(dt.to_cookie_string())  # to cookie style format
print(dt.format('DD MMMM, YYYY dddd HH:mm:ss A'))

ex_count += 1
print(f'\nex-{ex_count}=> Human Readability')
dt1 = pdl.datetime(2021, 1, 1)
print(dt1.diff_for_humans())
dt2 = pdl.datetime(2021, 11, 7, 1)
print(dt2.diff_for_humans())

ex_count += 1
print(f'\nex-{ex_count}=> Find Relative Datetime-relativedelta')
print(datetime(2013, 2, 21) + relativedelta(day=31))
print(pdl.now().start_of('day'))  # find the start time of the day
print(pdl.now().start_of('month'))
print(pdl.now().end_of('day'))
print(pdl.now().end_of('month'))

ex_count += 1
print(f'\nex-{ex_count}=> Find Relative Datetime-timedelta')
print(datetime.now() + timedelta(days=(0-datetime.now().weekday()+7)%7))
print(pdl.now().next(pdl.MONDAY))
print(pdl.now().previous(pdl.TUESDAY))
print(pdl.now().previous(pdl.TUESDAY, keep_time=True))

ex_count += 1
print(f'\nex-{ex_count}=> Find Relative Datetime-timedelt')
print(pdl.yesterday())
print(pdl.today())
print(pdl.tomorrow())
print(pdl.now().format('dddd DD MMMM YYYY', locale='zh'))
print(pdl.datetime(1988, 1, 1).age)

ex_count += 1
print(f'\nex-{ex_count}=> Airflow template test')
print(pdl.now())
# print(pdl.now().astimezone('Asia/Taipei'))
print(pdl.now().astimezone(pdl.timezone('Asia/Taipei')))
print(pdl.now().astimezone(pdl.timezone('UTC')))
print(pdl.now().in_timezone('UTC'))

# Lesson Learnt
# - Know why string can use and why not, 
#   - trace pendulum.astimezone history
#   - trace how stdlib change, and why
# - Know how to use TZ string
# - Know why we don't use TZ string
