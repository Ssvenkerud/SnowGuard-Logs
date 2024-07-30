import logging
from src.utils import stringcleanup


def locate_source(split_query, seperator=".", logger=logging.getLogger()):
    """locate the source object in a query.

    It does this by looking for the first occurance of a seperator in the query
    Source object is then cleaned up and returned.

    In this case a source object is the instance on which the query is executed
    """
    first_dot_found = False
    string_segment = None
    for segment in split_query:
        if (segment.find(seperator) != -1) & (first_dot_found is False):
            first_dot_found = True
            logger.debug(f"""dot found in {segment}""")
            string_segment = stringcleanup(segment)
    return string_segment
