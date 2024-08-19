from datetime import datetime, timedelta
from django.core.management.base import BaseCommand, CommandError
from django.db import connections
from server.settings import logger
from server.util.custom_base_command import CustomBaseCommand
import pandas as pd
import psycopg2
import queue
import schedule
import sys

class Command(CustomBaseCommand):

    @logger.catch
    def run(self):
        logger.log("START", None)
        try:
            db = connections["10.17.66.121.iot.smt"].settings_dict
            with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"], options=db['OPTIONS']['options']) as conn:
                chunk_size = 100000  
                data = []
                with conn.cursor() as cur:
                    cur.execute("""
                        WITH intervals AS (
                            SELECT 
                                id,
                                modified_date,
                                mc,
                                prd,
                                "result",
                                is_check,
                                CASE 
                                    WHEN EXTRACT(HOUR FROM modified_date) BETWEEN 0 AND 3 THEN modified_date::DATE + INTERVAL '00:00:00'
                                    WHEN EXTRACT(HOUR FROM modified_date) BETWEEN 4 AND 7 THEN modified_date::DATE + INTERVAL '04:00:00'
                                    WHEN EXTRACT(HOUR FROM modified_date) BETWEEN 8 AND 11 THEN modified_date::DATE + INTERVAL '08:00:00'
                                    WHEN EXTRACT(HOUR FROM modified_date) BETWEEN 12 AND 15 THEN modified_date::DATE + INTERVAL '12:00:00'
                                    WHEN EXTRACT(HOUR FROM modified_date) BETWEEN 16 AND 19 THEN modified_date::DATE + INTERVAL '16:00:00'
                                    WHEN EXTRACT(HOUR FROM modified_date) BETWEEN 20 AND 23 THEN modified_date::DATE + INTERVAL '20:00:00'
                                END AS interval_start_time,
                                CASE 
                                    WHEN EXTRACT(HOUR FROM modified_date) BETWEEN 0 AND 3 THEN modified_date::DATE + INTERVAL '04:00:00'
                                    WHEN EXTRACT(HOUR FROM modified_date) BETWEEN 4 AND 7 THEN modified_date::DATE + INTERVAL '08:00:00'
                                    WHEN EXTRACT(HOUR FROM modified_date) BETWEEN 8 AND 11 THEN modified_date::DATE + INTERVAL '12:00:00'
                                    WHEN EXTRACT(HOUR FROM modified_date) BETWEEN 12 AND 15 THEN modified_date::DATE + INTERVAL '16:00:00'
                                    WHEN EXTRACT(HOUR FROM modified_date) BETWEEN 16 AND 19 THEN modified_date::DATE + INTERVAL '20:00:00'
                                    WHEN EXTRACT(HOUR FROM modified_date) BETWEEN 20 AND 23 THEN (modified_date::DATE + INTERVAL '1 day') + INTERVAL '00:00:00'
                                END AS interval_stop_time,
                                modified_date::DATE AS date_only
                            FROM smt.smt_mount_mount_log_pickuprate_m10
                            WHERE is_check = false OR is_check IS NULL
                        ),
                        daily_counts AS (
                            SELECT 
                                interval_start_time,
                                mc,
                                prd,
                                COUNT(*) AS count_input,
                                COUNT(CASE WHEN "result" = 'OK' THEN 1 END) AS count_ok,
                                COUNT(CASE WHEN "result" = 'Pickup error' THEN 1 END) AS count_pickup_error,
                                COUNT(CASE WHEN "result" = 'Vision error' THEN 1 END) AS count_vision_error
                            FROM intervals
                            GROUP BY interval_start_time, mc, prd
                        )
                        SELECT 
                            i.id,
                        --    i.modified_date,
                            i.mc,
                            i.prd,
                        --    i."result",
                            i.interval_start_time,
                            i.interval_stop_time,
                            dc.count_input,
                            dc.count_ok,
                            dc.count_pickup_error,
                            dc.count_vision_error
                        FROM intervals i
                        LEFT JOIN daily_counts dc
                        ON i.interval_start_time = dc.interval_start_time AND i.mc = dc.mc AND i.prd = dc.prd
                        ORDER BY i.interval_start_time;

                    """)

                    while True:
                        rows = cur.fetchmany(chunk_size)
                        if not rows:
                            break
                        chunk_df = pd.DataFrame(rows)
                        data.append(chunk_df)

                data_df = pd.concat(data, ignore_index=True)
                duplicates = data_df.drop_duplicates(subset=[2, 3, 4])
                tuple_insert = [tuple(x[1:]) for x in duplicates.itertuples(index=False, name=None)]  

                # Insert into the target database (10.17.66.122.iot.smt)      
                db_in = connections["10.17.66.122.iot.smt"].settings_dict
                with  psycopg2.connect(user=db_in["USER"], password=db_in["PASSWORD"], host=db_in["HOST"], port=db_in["PORT"], dbname=db_in["NAME"], options=db_in['OPTIONS']['options']) as conn:
                    with conn.cursor() as cur:
                        cur.executemany("""
                        INSERT INTO smt.smt_mount_pickuprate_m10
                        (mc, prd, interval_start_time, interval_stop_time, count_input, count_ok, count_pickup_error, count_vision_error)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (mc, prd, interval_start_time, interval_stop_time)
                        DO UPDATE SET 
                            count_input = EXCLUDED.count_input,
                            count_ok = EXCLUDED.count_ok,
                            count_pickup_error = EXCLUDED.count_pickup_error,
                            count_vision_error = EXCLUDED.count_vision_error
                    """, tuple_insert)
                    conn.commit()

                # Update is_check to true in the source database (10.17.66.121.iot.smt)
                chunk_row = 100000
                ids_to_update = data_df[0].tolist()
                db = connections["10.17.66.121.iot.smt"].settings_dict
                with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"], options=db['OPTIONS']['options']) as conn:
                    with conn.cursor() as cur:
                        for i in range(0, len(ids_to_update), chunk_row):
                            chunk = ids_to_update[i:i + chunk_row]
                            update_query = """
                                UPDATE smt.smt_mount_mount_log_pickuprate_m10
                                SET is_check = true
                                WHERE id = ANY(%s);
                                """
                            cur.execute(update_query, (chunk,))
                            conn.commit()
                        pass

        except Exception as ex:
            logger.exception(ex)


        logger.log("STOP", None)
        pass

    def handle(self, *args, **options):   
        self.run()
        schedule.every(4).hours.do(self.jobqueue.put, self.run)
        self.run_schedule(schedule) 