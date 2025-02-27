import streamlit as st
import pandas as pd
import json
from confluent_kafka import Consumer, KafkaException, KafkaError
import matplotlib.pyplot as plt

pd.options.display.float_format = '{:,.3f}'.format

# Konfigurasi Kafka Consumer
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Ganti dengan server Kafka Anda
    'group.id': 'bitcoin-consumer-group',
    'auto.offset.reset': 'earliest',  # Membaca data dari awal
}
consumer = Consumer(consumer_config)

# Subscribe ke topik Kafka
topic_name = "bitcoin-topic"
consumer.subscribe([topic_name])

# Streamlit Dashboard
st.title("Real-Time Bitcoin Price Monitoring")
st.subheader("Streaming Data Harga Bitcoin dari Kafka")

# Placeholder untuk tabel dan plot
table_placeholder = st.empty()
plot_placeholder = st.empty()

# Data untuk menyimpan harga Bitcoin
btc_data = []
chart_data = []

# Inisialisasi plot untuk Bitcoin
fig, ax = plt.subplots()

try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Polling pesan dari Kafka
        if msg is None:
            continue  # Jika tidak ada pesan, lanjutkan loop
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                st.write("End of partition reached.") # Jika mencapai akhir partisi, lanjutkan loop
            else:
                raise KafkaException(msg.error())
        else:
            # Proses data dari Kafka
            btc_data_point = json.loads(msg.value().decode('utf-8'))
            
            # Mengonversi string kembali ke Timestamp
            btc_data_point['Datetime'] = pd.to_datetime(btc_data_point['Datetime'])
            timestamp = btc_data_point['Datetime']
            close_price = btc_data_point['Close']

            # Menambahkan data ke tabel dan chart
            btc_data.append([timestamp, close_price])
            chart_data.append([timestamp, close_price])

            # Update tabel secara dinamis
            df_btc = pd.DataFrame(btc_data, columns=["Time", "Price"])
            table_placeholder.write(df_btc)

            # Update grafik pergerakan harga Bitcoin
            df_chart = pd.DataFrame(chart_data, columns=["Time", "Price"])
            ax.clear()  # Clear plot sebelumnya
            df_chart.set_index("Time").plot(ax=ax)
            plot_placeholder.pyplot(fig)

except KeyboardInterrupt:
    st.write("Stopped monitoring.")
finally:
    consumer.close()
    st.write("Kafka consumer closed.")