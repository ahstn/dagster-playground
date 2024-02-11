from dagster._core.storage.db_io_manager import DbTypeHandler


class TrinoBaseTypeHandler(DbTypeHandler):
    """
    Base Type Handler Class. Should not be called directly, but 
    rather used through one of its sub-classes.
    """
    @property
    def requires_fsspec(self):
        return False