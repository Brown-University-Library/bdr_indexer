import datetime
import re


SOLR_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
STORAGE_SERVICE_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'


def utc_datetime_from_string(s):
    try:
        dt = datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%f%z')
    except ValueError:
        dt = datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%S%z')
    return dt.astimezone(tz=datetime.timezone.utc)


def utc_datetime_to_solr_string(dt):
    return dt.strftime(SOLR_DATE_FORMAT)


MONTH_MAP = {
    "01" : "January",
    "02" : "February",
    "03" : "March",
    "04" : "April",
    "05" : "May",
    "06" : "June",
    "07" : "July",
    "08" : "August",
    "09" : "September",
    "10" : "October",
    "11" : "November",
    "12" : "December",
}


class SolrDate:
    def __init__(self, date_string):
        self.date_string = date_string

    @property
    def year(self):
        return self.date_string[:4]

    @property
    def month(self):
        return self.date_string[5:7]

    @property
    def month_text(self):
        return MONTH_MAP.get(self.month)

    @property
    def day(self):
        return self.date_string[8:10]

    def __str__(self):
        return self.date_string

    def __bool__(self):
        return bool(self.date_string)


def get_solr_date(date_str):
    #try to construct a valid date string for solr
    # if not, return None and date will go into string field
    solr_date_string = None
    try:
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            dt = datetime.datetime.strptime(date_str, '%Y-%m-%d')
            solr_date_string = date_str + 'T00:00:00Z'
        elif re.match(r'^\d{4}-\d{2}$', date_str):
            dt = datetime.datetime.strptime(date_str, '%Y-%m')
            solr_date_string = date_str + '-01T00:00:00Z'
        elif re.match(r'^\d{4}$', date_str):
            dt = datetime.datetime.strptime(date_str, '%Y')
            solr_date_string = date_str + '-01-01T00:00:00Z'
    except ValueError:
        pass
    return SolrDate(solr_date_string)
