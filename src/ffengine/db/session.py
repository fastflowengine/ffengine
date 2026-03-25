class DBSession:
    """Driver-agnostic DB connection context manager."""
    
    def __init__(self, connection_params: dict, dialect):
        self.params = connection_params
        self.dialect = dialect
        self.conn = None

    def __enter__(self):
        self.conn = self.dialect.connect(self.params)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            try:
                if exc_type:
                    self.conn.rollback()
                else:
                    self.conn.commit()
            finally:
                self.conn.close()

    def cursor(self, server_side=False):
        if not self.conn:
            raise RuntimeError("Database connection is not open.")
        return self.dialect.create_cursor(self.conn, server_side)

    def health_check(self) -> bool:
        """Verifies if the database connection is alive."""
        if not self.conn:
            return False
        try:
            return self.dialect.health_check(self.conn)
        except Exception:
            return False
