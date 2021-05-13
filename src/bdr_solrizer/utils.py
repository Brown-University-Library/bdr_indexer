import datetime


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
