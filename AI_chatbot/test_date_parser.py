import dateparser
from datetime import datetime, timedelta

date_filter = "21/07/2025"

parsed_date = dateparser.parse(date_filter)

print(parsed_date)

print(datetime.now().date())