"""DatabaseHandler class."""
# pylint: disable=invalid-name
# pylint: disable=import-error

import pandas as pd
from sqlalchemy import create_engine


class DatabaseHandler:
    """DatabaseHandler."""

    def __init__(self, db):
        """Docstring."""
        self.db = db
        self._engine = None
        self.dialect = None
        self.db_uri = None

    def _get_engine(self):
        """Docstring."""
        db_uri = self.db_uri
        if not self._engine:
            self._engine = create_engine(db_uri)
        return self._engine

    def _connect(self):
        """Docstring."""
        return self._get_engine().connect()

    @staticmethod
    def _cursor_columns(cursor):
        """Docstring."""
        if hasattr(cursor, "keys"):
            return cursor.keys()
        return [c[0] for c in cursor.description]

    def execute(self, sql, connection=None):
        """Docstring."""
        if connection is None:
            connection = self._connect()
        return connection.execute(sql)

    def insert_from_frame(self, df, table, if_exists="append"):
        """Docstring."""
        connection = self._connect()
        with connection:
            df = df.set_index("id")

            # dump a slice with changed rows to temporary MySQL table
            df.to_sql("tmp_table", connection, if_exists="replace", index=True)

            trans = connection.begin()

            try:
                # delete those rows that we are going to "upsert"
                connection.execute(
                    f"delete from {table} where id in (select id from tmp_table)"
                )
                trans.commit()

                # insert changed rows
                df.to_sql(table, connection, if_exists=if_exists, index=True)
            except:
                trans.rollback()
                raise

    def to_frame(self, *args, **kwargs):
        """Docstring."""
        cursor = self.execute(*args, **kwargs)
        if not cursor:
            return None
        data = cursor.fetchall()
        if data:
            df = pd.DataFrame(data, columns=self._cursor_columns(cursor))
        else:
            df = pd.DataFrame()
        return df

    def create_table(self, model):
        """Docstring."""
        return model.__table__.create(bind=self._get_engine(), checkfirst=True)
