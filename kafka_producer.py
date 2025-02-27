import time
import json
import yfinance as yf
from confluent_kafka import Producer

# Konfigurasi Kafka producer
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',  # Ganti dengan server Kafka Anda
    'client.id': 'bitcoin-producer'
}

producer = Producer(KAFKA_CONFIG)
TOPIC = 'bitcoin-topic'  # Nama topik Kafka
INTERVAL = 30  # Interval pengiriman data dalam detik


def fetch_latest_btc_price():
    """Mengambil harga terbaru Bitcoin dalam interval 2 menit."""
    df = yf.download(tickers=["BTC-USD"], period="1d", interval="2m")
    df = df.xs("BTC-USD", level="Ticker", axis=1).reset_index()
    latest_data = df[['Datetime', 'Close']].tail(1)  # Ambil data terakhir
    latest_data['Datetime'] = latest_data['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return latest_data.to_dict(orient="records")[0]  # Konversi ke format JSON


def send_to_kafka(data):
    """Mengirim data ke Kafka dalam format JSON."""
    message = json.dumps(data).encode('utf-8')
    producer.produce(TOPIC, message, callback=on_send_success)
    producer.flush()


def on_send_success(err, msg):
    """Callback untuk menampilkan status pengiriman pesan."""
    if err:
        print(f"Error: {err}")
    else:
        print(f"Message sent: {msg.value().decode('utf-8')}")


while True:
    btc_data = fetch_latest_btc_price()
    send_to_kafka(btc_data)
    time.sleep(INTERVAL)
