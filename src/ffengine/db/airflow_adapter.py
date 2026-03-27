
class AirflowConnectionAdapter:
    """
    Resolves Airflow Connection ID into standard parameters for Dialects.
    """

    @staticmethod
    def get_connection_params(conn_id: str) -> dict:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection(conn_id)
        
        # Build base params
        params = {}
        if conn.host:
            params["host"] = conn.host
        if conn.port:
            params["port"] = conn.port
        if conn.login:
            params["user"] = conn.login
        if conn.password:
            params["password"] = conn.password
        if conn.schema: # database config in standard airflow SQL
            params["database"] = conn.schema
            
        # Airflow extra params
        if conn.extra_dejson:
            params["extra"] = conn.extra_dejson
            
        params["conn_type"] = conn.conn_type
        return params
