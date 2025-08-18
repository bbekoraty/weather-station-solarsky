from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
pd.set_option('display.max_columns', None)

import ssl
import certifi
import logging

logging.basicConfig(level=logging.INFO)
pd.set_option('display.max_columns', None)

class InfluxDataBase:
    def __init__(self, url, token, org) -> None:
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        try:
            self.client = InfluxDBClient(url=url, token=token, org=org, ssl_context=ssl_context)
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            self.query_api = self.client.query_api()
            self.token = token
            self.org = org
            logging.info("Connected to InfluxDB successfully.")
        except Exception as e:
            logging.error(f"Error connecting to InfluxDB: {e}")
            raise

    def read_nearest(self, bucket, token, target_time: datetime):
        start = (target_time - timedelta(hours=1)).isoformat() + "Z"
        stop = (target_time + timedelta(hours=1)).isoformat() + "Z"

        topic = f"sensor/{token}"

        query = f'''
        from(bucket: "{bucket}")
        |> range(start: {start}, stop: {stop})
        |> filter(fn: (r) => r["topic"] == "{topic}")
        |> filter(fn: (r) => r["_field"] == "dir" or r["_field"] == "hm" or r["_field"] == "irr" or r["_field"] == "temp" or r["_field"] == "ws" or r["_field"] == "bat" or r["_field"] == "voltage_real")
        |> filter(fn: (r) => r["_measurement"] == "mqtt_consumer")
        '''

        result = self.query_api.query(org=self.org, query=query)

        nearest_records = {}
        nearest_diffs = {}
        common_time = None
        topic_name = None

        for table in result:
            for record in table.records:
                record_time = record.get_time()
                diff = abs((record_time.replace(tzinfo=None) - target_time).total_seconds())
                field = record.get_field()

                if field not in nearest_diffs or diff < nearest_diffs[field]:
                    nearest_diffs[field] = diff
                    nearest_records[field] = record.get_value()
                    common_time = record_time
                    topic_name = record.values.get("topic")

        if not nearest_records:
            return None

        return {
            "time": common_time,
            "topic": topic_name,
            "values": nearest_records
        }
        
        # json_results = []
        # if not results:
        #     return {"data":[]}
        # df = pd.DataFrame(results)
        # df = df[df['topic'].str.startswith('devices/iot/')].sort_values(by='time')
        # count_unique = df['field'].unique()   
        # for field in count_unique:
        #     buffer = df[(df["field"] == field)] 
        #     for name, group in buffer.groupby('topic'):
        #         mean_value = group['value'].mean()
        #         json_value = {"topic":name,group['field'].unique()[0]:mean_value}
        #         json_results.append(json_value)  
        # data = json_results
        # merged_data = {}
        # for item in data:
        #     topic = item['topic']
        #     if topic not in merged_data:
        #         merged_data[topic] = {}
        #     for key, value in item.items():
        #         if key != 'topic':
        #             merged_data[topic][key] = value

        # result_list = [{'topic': topic, **values} for topic, values in merged_data.items()]
        # return {"data":result_list}
    
    # def read_data_latest_bts(self):
    #     query = f'from(bucket: "devices")'  
    #     query += f'|> range(start: -120m)'
    #     query += f'|> filter(fn:(r) => r["_measurement"] == "mqtt_consumer")'
    #     query += f'|> last()'
    #     # query += f'|> filter(fn: (r) => r._field == "DO_value" or r._field == "temp")'
    #     # query=query+f'|> filter(fn: (r) => r["topic"] == "sgm/factory/1703407002")'
    #     result = self.query_api.query(org=self.org, query=query)
    #     results = []
    #     for table in result:
    #         for record in table.records:
    #             results.append({"time":record.get_time(),"field":record.get_field(),"value":record.get_value(),"topic":record.values.get("topic")})
    #     if not results:
    #         return {"data":[]}
    #     df = pd.DataFrame(results)
    #     result_dict = df.set_index('field').to_dict(orient='index')

    #     # Transform the dictionary to the desired format
    #     result_dict = {key[:-7]: {'status': value['value']} for key, value in result_dict.items()}

    #     # Print the result
    #     print(result_dict)
    #     return result_dict
    
    # def read_point1_latest(self,start="-60m"):
    #     query = f'from(bucket: "devices")'  
    #     query += f'|> range(start: {start})'
    #     query += f'|> filter(fn:(r) => r["_measurement"] == "mqtt_consumer")'
    #     query=query+f'|> filter(fn: (r) => r["topic"] == "devices/hatyai/1")'
    #     query += f'|> filter(fn: (r) => r._field == "DO_value" or r._field == "pH_value" or r._field == "temp")'
    #     result = self.query_api.query(org=self.org, query=query)
    #     results = []
    #     for table in result:
    #         for record in table.records:
    #             utc_time = record.get_time()
    #             thai_time = utc_time + timedelta(hours=7)
    #             date_part = thai_time.date()
    #             date_time = thai_time.strftime("%H:%M")
    #             results.append({"date":str(date_part), "time":str(date_time),"field":record.get_field(),"value":record.get_value(),"topic":record.values.get("topic")})
       
    #     grouped_data = defaultdict(list)
    #     for entry in results:
    #         field = entry["field"]
    #         grouped_data[field].append({
    #             "date": entry["date"],
    #             "time": entry["time"],
    #             "value": entry["value"],
    #             "topic": entry["topic"]
    #         })
    #     return grouped_data
        
    # def read_point2_latest(self,start="-60m"):
    #     query = f'from(bucket: "devices")'  
    #     query += f'|> range(start: {start})'
    #     query += f'|> filter(fn:(r) => r["_measurement"] == "mqtt_consumer")'
    #     query=query+f'|> filter(fn: (r) => r["topic"] == "devices/hatyai/2")'
    #     query += f'|> filter(fn: (r) => r._field == "DO_value" or r._field == "pH_value" or r._field == "temp")'
    #     result = self.query_api.query(org=self.org, query=query)
    #     results = []
    #     for table in result:
    #         for record in table.records:
    #             utc_time = record.get_time()
    #             thai_time = utc_time + timedelta(hours=7)
    #             date_part = thai_time.date()
    #             date_time = thai_time.strftime("%H:%M")
    #             results.append({"date":str(date_part), "time":str(date_time),"field":record.get_field(),"value":record.get_value(),"topic":record.values.get("topic")})
    #     grouped_data = defaultdict(list)
    #     for entry in results:
    #         field = entry["field"]
    #         grouped_data[field].append({
    #             "date": entry["date"],
    #             "time": entry["time"],
    #             "value": entry["value"],
    #             "topic": entry["topic"]
    #         })
    #     return grouped_data
        
    # def read_point3_latest(self,start="-60m"):
    #     query = f'from(bucket: "devices")'  
    #     query += f'|> range(start: {start})'
    #     query += f'|> filter(fn:(r) => r["_measurement"] == "mqtt_consumer")'
    #     query=query+f'|> filter(fn: (r) => r["topic"] == "devices/hatyai/3")'
    #     query += f'|> filter(fn: (r) => r._field == "DO_value" or r._field == "pH_value" or r._field == "temp")'
    #     result = self.query_api.query(org=self.org, query=query)
    #     results = []
    #     for table in result:
    #         for record in table.records:
    #             utc_time = record.get_time()
    #             thai_time = utc_time + timedelta(hours=7)
    #             date_part = thai_time.date()
    #             date_time = thai_time.strftime("%H:%M")
    #             results.append({"date":str(date_part), "time":str(date_time),"field":record.get_field(),"value":record.get_value(),"topic":record.values.get("topic")})
    #     grouped_data = defaultdict(list)
    #     for entry in results:
    #         field = entry["field"]
    #         grouped_data[field].append({
    #             "date": entry["date"],
    #             "time": entry["time"],
    #             "value": entry["value"],
    #             "topic": entry["topic"]
    #         })
    #     return grouped_data
        
    # def read_point4_latest(self,start="-60m"):
    #     query = f'from(bucket: "devices")'  
    #     query += f'|> range(start: {start})'
    #     query += f'|> filter(fn:(r) => r["_measurement"] == "mqtt_consumer")'
    #     query=query+f'|> filter(fn: (r) => r["topic"] == "devices/hatyai/4")'
    #     query += f'|> filter(fn: (r) => r._field == "chlorine_value")'

    #     result = self.query_api.query(org=self.org, query=query)
    #     results = []
    #     for table in result:
    #         for record in table.records:
    #             utc_time = record.get_time()
    #             thai_time = utc_time + timedelta(hours=7)
    #             date_part = thai_time.date()
    #             date_time = thai_time.strftime("%H:%M")
    #             results.append({"date":str(date_part), "time":str(date_time),"field":record.get_field(),"value":record.get_value(),"topic":record.values.get("topic")})
    #     grouped_data = defaultdict(list)
    #     for entry in results:
    #         field = entry["field"]
    #         grouped_data[field].append({
    #             "date": entry["date"],
    #             "time": entry["time"],
    #             "value": entry["value"],
    #             "topic": entry["topic"]
    #         })
    #     return grouped_data
    
    # def read_point5_latest(self,start="-60m"):
    #     query = f'from(bucket: "devices")'  
    #     query += f'|> range(start: {start})'
    #     query += f'|> filter(fn:(r) => r["_measurement"] == "mqtt_consumer")'
    #     query=query+f'|> filter(fn: (r) => r["topic"] == "devices/hatyai/5")'
    #     query += f'|> filter(fn: (r) => r._field == "DO_value" or r._field == "pH_value" or r._field == "temp")'

    #     result = self.query_api.query(org=self.org, query=query)
    #     results = []
    #     for table in result:
    #         for record in table.records:
    #             utc_time = record.get_time()
    #             thai_time = utc_time + timedelta(hours=7)
    #             date_part = thai_time.date()
    #             date_time = thai_time.strftime("%H:%M")
    #             results.append({"date":str(date_part), "time":str(date_time),"field":record.get_field(),"value":record.get_value(),"topic":record.values.get("topic")})
    #     grouped_data = defaultdict(list)
    #     for entry in results:
    #         field = entry["field"]
    #         grouped_data[field].append({
    #             "date": entry["date"],
    #             "time": entry["time"],
    #             "value": entry["value"],
    #             "topic": entry["topic"]
    #         })
    #     return grouped_data
        
        
