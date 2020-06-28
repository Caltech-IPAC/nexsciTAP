# Copyright (c) 2020, Caltech IPAC.  

# License information at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE


import itertools
import sqlparse

from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML


class TableNames:

    """
    The simple TableNames class extracts a list of the database
    tables being used in a query by parsing them out of the FROM clause.
    The purpose of this is to have something we can use to get output
    column metadata, so we ignore subqueries and strip off AS renaming.
    """


    def __init__(self):

        return None


    def is_subselect(self, parsed):
        if not parsed.is_group:
            return False
        for item in parsed.tokens:
            if item.ttype is DML and item.value.upper() == 'SELECT':
                return True
        return False


    def extract_from_part(self, parsed):
        from_seen = False
        for item in parsed.tokens:
            if item.is_group:
                for x in self.extract_from_part(item):
                    yield x
            if from_seen:
                if self.is_subselect(item):
                    for x in self.extract_from_part(item):
                        yield x
                elif item.ttype is Keyword and item.value.upper() in \
                        ['ORDER', 'GROUP', 'BY', 'HAVING', 'GROUP BY']:
                    from_seen = False
                    StopIteration
                else:
                    yield item
            if item.ttype is Keyword and item.value.upper() == 'FROM':
                from_seen = True


    def extract_table_identifiers(self, token_stream):
        for item in token_stream:
            if isinstance(item, IdentifierList):
                for identifier in item.get_identifiers():
                    value = identifier.value.replace('"', '').lower()
                    yield value
            elif isinstance(item, Identifier):
                value = item.value.replace('"', '').lower()
                yield value


    def extract_tables(self, sql):
        extracted_tables = []
        statements = list(sqlparse.parse(sql))
        for statement in statements:
            if statement.get_type() != 'UNKNOWN':
                stream = self.extract_from_part(statement)
                extracted_tables.append(list(self.extract_table_identifiers(stream)))
        tables = list(itertools.chain(*extracted_tables))

        for idx, table in enumerate(tables):
            tables[idx] = tables[idx].split()[0]

        return tables
