"""Module containing the query parser class."""
from src.utils import stringcleanup
from src.LocateSource import locate_source


class QueryParser:
    """Parse a query and extract the query type and object.

    This is a custom parser and is not guaranteed to work for all queries.
    The snowflake query history table is used as the source of queries.
    It contains aproximatly 60 typed of queries.
    this parser attempts to generalise as far as possible.
    """

    def __init__(self, logger, query):
        """Initiate the query parser."""
        self.logger = logger
        self.query = query.upper()
        self.query_type = None
        self.source_object = []
        self.source_string = None
        self._parse()

    def _parse(self):
        """Parse the query and extract the query type and source object.

        It is the main function of the class.
        and is called by the __init__ function.
        it is not intended to be called directly.
        """
        try:
            split_query = str.split(self.query)
        except:
            split_query = [self.query]
        self.logger.debug(f"split query {split_query}")
        statement = None

        # Gather the Query type statement elements
        if split_query[0] in ["BEGIN"]:
            statement = split_query[0:2]

        elif split_query[0] in ["ALTER", "TRUNCATE", "EXECUTE", "DROP"]:
            if split_query[1] in ["ROW"]:
                statement = split_query[0:3]

            else:
                statement = split_query[0:2]

        elif split_query[0] in ["CREATE"]:
            if split_query[1] in [
                "USER",
                "ROLE",
                "TASK",
                "TABLE",
                "TAG",
                "SCHEMA",
            ]:
                statement = split_query[0:2]
            else:
                statement = split_query[0:3]

        if statement:
            self.query_type = stringcleanup(" ".join(statement))
        else:
            self.query_type = split_query[0]

        if split_query[0] in ["REVOKE", "GRANT", "CREATE"]:
            self.source_string = locate_source(split_query, seperator="_")
            self.logger.debug(f"""source string:{self.source_string}""")

        elif split_query[0] in ["SET"]:
            self.source_string = split_query[1]

        elif split_query[0] in [
            "TRUNCATE",
            "UNDROP",
            "MERGE",
            "INSERT",
            "COPY",
        ]:
            self.source_string = stringcleanup(split_query[2].split("(")[0])

        elif split_query[0] in [
            "DROP",
            "ALTER",
            "EXECUTE",
            "DESCRIBE",
            "LIST",
            "DELETE",
        ]:
            if split_query[1] in ["SESSION"]:
                self.source_object = ["UNPARCEBLE"]

            else:
                self.source_string = locate_source(split_query, seperator="_")

        elif split_query[0] in ["REMOVE", "CALL"]:
            self.source_string = stringcleanup(split_query[1].split("!")[0])
        elif split_query[0] in ["PUT", "BEGIN", "GET", "ROLLBACK", "COMMIT"]:
            self.source_string = "UNPARCEBLE"

        """Attempt to locate source_string based on the location of from statement"""
        try:
            from_position = split_query.index("FROM")
        except:
            from_position = None
        self.logger.debug(f"""from position:{from_position}""")

        if from_position:
            self.source_string = locate_source(split_query)
            self.logger.debug(f"""source string:{self.source_string}""")
            if self.source_string is None:
                self.source_string = locate_source(split_query, seperator="_")
                self.logger.debug(f"""source string:{self.source_string}""")

        """If source_string not found by now, attempt to find source string
        based on precence of.
        """

        if self.source_string is None:
            try:
                self.source_string = locate_source(split_query)
            except:
                self.source_string = None

        """Transfer the identified source string to source object for return"""
        try:
            self.source_object = self.source_string.lower().split(".")
        except:
            if self.source_string is None:
                self.source_object = ["Not identified"]
            elif self.source_string == []:
                self.source_object = []
            else:
                self.source_object = [self.source_string.lower()]
        self.logger.debug(f"source object: {self.source_object}")

    def get_query_object(self):
        """Return the source object as a list."""
        return list(self.source_object)
