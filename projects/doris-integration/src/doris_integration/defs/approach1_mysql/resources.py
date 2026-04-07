import dagster as dg
import pandas as pd
import pymysql


class DorisResource(dg.ConfigurableResource):
    """Doris database resource via MySQL protocol.

    Usage:
        with doris.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
    """

    host: str
    port: int = 9030
    user: str
    password: str
    database: str

    def get_connection(self) -> pymysql.Connection:
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            cursorclass=pymysql.cursors.DictCursor,
        )

    def execute_query(self, query: str, params: dict | None = None) -> None:
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params or {})
            conn.commit()
        finally:
            conn.close()

    def fetch_dataframe(self, query: str, params: dict | None = None) -> pd.DataFrame:
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params or {})
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
        finally:
            conn.close()

    def insert_dataframe(
        self,
        table: str,
        df: pd.DataFrame,
        if_exists: str = "append",
        chunk_size: int = 1000,
    ) -> None:
        if df.empty:
            return

        if if_exists == "replace":
            self.execute_query(f"TRUNCATE TABLE `{table}`")

        columns = ", ".join(f"`{c}`" for c in df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_sql = f"INSERT INTO `{table}` ({columns}) VALUES ({placeholders})"

        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                for i in range(0, len(df), chunk_size):
                    chunk = df.iloc[i : i + chunk_size]
                    values = [
                        tuple(None if pd.isna(v) else v for v in row)
                        for row in chunk.values
                    ]
                    cursor.executemany(insert_sql, values)
            conn.commit()
        finally:
            conn.close()


class DorisIOManager(dg.ConfigurableIOManager):
    """IOManager that persists DataFrames to/from Doris tables.

    Table name is derived from the asset key path.
    """

    host: str
    port: int = 9030
    user: str
    password: str
    database: str

    def _get_resource(self) -> DorisResource:
        return DorisResource(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )

    def _table_name(self, context: dg.OutputContext) -> str:
        return "_".join(context.asset_key.path)

    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame) -> None:
        table = self._table_name(context)
        resource = self._get_resource()
        resource.insert_dataframe(table, obj, if_exists="replace")
        context.add_output_metadata(
            {
                "table_name": dg.MetadataValue.text(table),
                "row_count": dg.MetadataValue.int(len(obj)),
                "columns": dg.MetadataValue.text(", ".join(obj.columns.tolist())),
            }
        )

    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        table = self._table_name(context)
        resource = self._get_resource()
        return resource.fetch_dataframe(f"SELECT * FROM `{table}`")
