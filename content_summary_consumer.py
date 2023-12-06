import requests
import json
import asyncio

from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
import torch
import nltk
nltk.download('punkt')

from pydantic import BaseModel

from kafka import KafkaConsumer
from kafka.structs import TopicPartition

import pymongo

# mongodb 연결
client = pymongo.MongoClient('mongodb://sam9:sam9mo!!@59.3.28.12:27017/')
db = client["sam9mo"]
collection = db["news"]

class DataInput(BaseModel):
    inputText: str
class SummaryOut(BaseModel):
    summary: str

model_dir = "gogamza/kobart-summarization"
tokenizer = AutoTokenizer.from_pretrained(model_dir)
model = AutoModelForSeq2SeqLM.from_pretrained(model_dir)

def summary_content_process(Text_data):
    response = requests.post("http://221.156.60.18:8010/summary", data=json.dumps(Text_data))
    return response.json()

def summary_content_process_local(Text_data: DataInput):
    returnSummary: SummaryOut = None
    try:
        print("Text_data\n", Text_data["inputText"])
        text: str =  Text_data["inputText"]

        raw_input_ids = tokenizer.encode(text)
        input_ids = [tokenizer.bos_token_id] + raw_input_ids + [tokenizer.eos_token_id]

        summary_ids = model.generate(torch.tensor([input_ids]),  num_beams=4,  max_length=4000,  eos_token_id=1)
        predicted_title = tokenizer.decode(summary_ids.squeeze().tolist(), skip_special_tokens=True)
        
        returnSummary: SummaryOut = predicted_title
        print("returnSummary", returnSummary)
    except Exception as e:
        returnSummary: SummaryOut = str(e)
        print(f'Local Summary EXCEPTION => {e}')
        
    return returnSummary

def consume_messages():
    # Create a Consumer instance
    CARD_TOPIC = 'send_news_content'

    brokers = ['59.3.28.12:9092', '59.3.28.12:9093', '59.3.28.12:9094']
    consumer = KafkaConsumer(
        bootstrap_servers=brokers,
        value_deserializer=lambda m:json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest')

    # partitions = consumer.partitions_for_topic(CARD_TOPIC)
    
    while True:
        # offsetValue_save.txt에서 마지막 실행 offset변수 불러오기
        p = 2
        # for p in partitions:
        with open(f'offsetValue{p}_save.txt', 'r') as file:
            offset_save = int(file.read())
            
        topic_partition = TopicPartition(CARD_TOPIC, p)
        # Seek offset 0
        consumer.assign([topic_partition])
        consumer.seek(partition=topic_partition, offset=offset_save+1)
        
        for message in consumer:
            
            print("partition_number ", p)
            print("offset_number ", message.offset)
            print("message\n", message)
            
            find_id_query = {"_id" : message.value['_id']}
            
            if collection.find_one(find_id_query) == None:
                print("_id 중복됨으로 다음 사이클로 넘어감")
                continue
            
            Text_data = {"inputText" : message.value['news_content']}
        
            content_summary = summary_content_process_local(Text_data)
            content_summary = ' '.join(content_summary.split())
            
            try:
                collection.update_one(find_id_query, {"$set" : {"content_summary" : content_summary}}, upsert=False)

            except Exception as ex:
                print(ex)
            
            # offset을 offsetValue_save.txt에 저장
            with open(f'offsetValue{p}_save.txt', 'w') as file:
                file.write(str(message.offset))            
    
async def consumer_process():
    
    consumer = KafkaConsumer(
        "send_news_content",
        bootstrap_servers=['59.3.28.12:9092', '59.3.28.12:9093', '59.3.28.12:9094'],
        value_deserializer=lambda m:json.loads(m.decode('utf-8'))
    )
    
    try:
        await consumer.start()
    except Exception as e:
        print(e)
        
    while True:
        try:
            for message in consumer:
                
                find_id_query = {"_id" : message.value['_id']}
                if collection.find_one(find_id_query) == None:
                    continue
                
                Text_data = {"inputText" : message.value['news_content']}
               
                # content_summary = summary_content_process(Text_data)
                content_summary = summary_content_process(Text_data)
                content_summary = ' '.join(content_summary.split())
                
                try:
                    collection.update_one(find_id_query, {"$set" : {"content_summary" : content_summary}}, upsert=False)
                except Exception as ex:
                    print(ex)
        finally:
            await consumer.stop()

# 축적된 topic consume
consume_messages()

# producer가 실행될 때 topic consume
# asyncio.run(consumer_process())
