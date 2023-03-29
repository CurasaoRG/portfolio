from datetime import datetime

from lib.pg_connect import PgConnect

class StgRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def order_events_insert(self,
                            object_id: int,
                            object_type: str,
                            sent_dttm: datetime,
                            payload: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO stg.order_events (
                            object_id,
                            object_type, 
                            sent_dttm,
                            payload
                            )
                        values(
                            %(object_id)s, 
                            %(object_type)s,
                            %(sent_dttm)s,
                            %(payload)s
                        )
                        on conflict (object_id) do update
                        SET object_type = EXCLUDED.object_type, payload = EXCLUDED.payload, sent_dttm = EXCLUDED.sent_dttm;
                    """,
                    {
                        'object_id': object_id,
                        'object_type': object_type,
                        'sent_dttm': sent_dttm,
                        'payload': payload
                    }
                )