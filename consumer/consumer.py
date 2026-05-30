from kafka import KafkaConsumer
import json
import psycopg2

# PostgreSQL Connection
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="postgres123",
    host="localhost",
    port=5433
)

cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS raw_data (
        id             SERIAL PRIMARY KEY,
        order_id       INT,
        user_id        VARCHAR(50),
        product_id     INT,
        product_name   VARCHAR(100),
        category       VARCHAR(50),
        price          FLOAT,
        quantity       INT,
        event_type     VARCHAR(50),
        timestamp      VARCHAR(50),
        payment_method VARCHAR(50),
        city           VARCHAR(50)
    )
""")
conn.commit()
print("raw_data table ready.")

# Kafka Consumer
consumer = KafkaConsumer(
    'ecommerce_topic',
    bootstrap_servers='127.0.0.1:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consumer started...")

INSERT_COUNT = 0  # used for batching commits (FIX 3)

try:
    for message in consumer:
        try:
            data = message.value

            # FIX 1: Use .get() for ALL field access instead of data['key'].
            #
            # Why: producer's schema_drift() sometimes renames product_id → prod_id
            # and drops payment_method entirely. data['product_id'] raises KeyError
            # when the key simply doesn't exist in the dict.
            # data.get('product_id') safely returns None instead of crashing.
            #
            # For product_id specifically: also check the renamed key prod_id
            # because schema_drift uses that name for ~4% of messages.
            product_id = data.get('product_id') or data.get('prod_id')

            # FIX 2: Safe price cleaning that handles None.
            #
            # Why: producer's maybe_null() sends price=None ~9% of the time.
            # str(None) = "None", float("None") raises ValueError → row skipped.
            # Now we just store NULL in the DB for missing prices — that's correct
            # for raw data and the anomaly model handles it fine.
            raw_price = data.get('price')
            if raw_price is None:
                cleaned_price = None
            else:
                price_str = str(raw_price).replace(' INR', '').strip()
                try:
                    cleaned_price = float(price_str)
                except ValueError:
                    cleaned_price = None  # unparseable price → store as NULL

            cur.execute("""
                INSERT INTO raw_data
                (
                    order_id,
                    user_id,
                    product_id,
                    product_name,
                    category,
                    price,
                    quantity,
                    event_type,
                    timestamp,
                    payment_method,
                    city
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                data.get('order_id'),
                data.get('user_id'),
                product_id,                     

                data.get('product_name'),
                data.get('category'),
                cleaned_price,                 

                data.get('quantity'),
                data.get('event_type'),
                data.get('timestamp'),
                data.get('payment_method'),    
                
                data.get('city'),
            ))

            # FIX 3: Batch commits every 50 rows instead of every single row.
            #
            # Why: during burst events (200 messages at once), committing after
            # every row means 200 round-trips to PostgreSQL in 2 seconds.
            # The consumer falls behind Kafka and your "real-time" dashboard
            # shows data that's minutes old.
            # Committing every 50 rows is 4 trips instead of 200 — much faster.
            INSERT_COUNT += 1
            if INSERT_COUNT % 50 == 0:
                conn.commit()
                print(f"[{INSERT_COUNT}] Committed batch. Last order_id: {data.get('order_id')}")

        except Exception as e:
            conn.rollback()
            print(f"Error inserting row: {e}")


# Without this, PostgreSQL holds the connection open until TCP timeout (~10 min).
except KeyboardInterrupt:
    print("\n[CONSUMER] Shutting down cleanly...")
finally:
    conn.commit()     
    conn.close()
    consumer.close()
    print("[CONSUMER] Closed.")


    # ── AUTO CLEANING ────────────────────────────────
    print("\n[CLEANING] Starting automatic data cleaning...")
    try:
        with open('SQL/cleaning.sql', 'r') as f:
            sql = f.read()

        clean_conn = psycopg2.connect(
            dbname="postgres", user="postgres",
            password="postgres123",
            host="localhost", port=5433
        )
        clean_conn.autocommit = True
        cur = clean_conn.cursor()

        for stmt in [s.strip() for s in sql.split(';') if s.strip()]:
            if stmt.upper().lstrip().startswith('SELECT'):
                continue
            try:
                cur.execute(stmt)
            except Exception as e:
                print(f"  SQL warning: {e}")
                print(f"  Failed statement: {stmt[:100]}...")

        clean_conn.close()

        # verify
        check_conn = psycopg2.connect(
            dbname="postgres", user="postgres",
            password="postgres123",
            host="localhost", port=5433
        )
        cur2 = check_conn.cursor()
        cur2.execute("SELECT COUNT(*) FROM cleaned_data")
        count = cur2.fetchone()[0]
        check_conn.close()
        print(f"[CLEANING] Done — {count:,} rows in cleaned_data ✓")

    except Exception as e:
        print(f"[CLEANING] Failed: {e}")
    # ── END AUTO CLEANING ────────────────────────────