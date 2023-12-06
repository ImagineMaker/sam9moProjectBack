import json

from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
import torch
import nltk
nltk.download('punkt')

from kafka import KafkaConsumer, KafkaProducer

from pydantic import BaseModel

import pymongo

CARD_TOPIC = 'send_news_content'
brokers = ['59.3.28.12:9092', '59.3.28.12:9093', '59.3.28.12:9094']

consumer = KafkaConsumer(
    CARD_TOPIC,
    bootstrap_servers=brokers,
    value_deserializer=lambda m:json.loads(m.decode('utf-8'))
)

# kafa세팅
class MessageProducer:
    def __init__(self, broker, topic):
        self.broker = broker
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=self.broker,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            acks=0,
            api_version=(2, 5, 0),
            retries=3,
        )

    def send_message(self, msg, auto_close=False):
        try:
            future = self.producer.send(self.topic, msg)
            self.producer.flush()  # 비우는 작업
            if auto_close:
                self.producer.close()
            future.get(timeout=2)
            return {"status_code": 200, "error": None}
        except Exception as exc:
            raise exc

pr_func_status = MessageProducer(brokers, topic="send_function_status")

def send_function_status(*arg):
    if arg[2] == "Success":
        send_message = {
            "function_category" : arg[0],
            "function_name" :  arg[1],
            "function_status" : arg[2]
        }
    else:
        send_message = {
            "function_category" : arg[0],
            "function_name" :  arg[1],
            "function_status" : arg[2],
            "status_message" : arg[3],
            "problem_id" : arg[4],            
        }
    pr_func_status.send_message(send_message, False)
    
model_dir = "gogamza/kobart-summarization"
tokenizer = AutoTokenizer.from_pretrained(model_dir)
model = AutoModelForSeq2SeqLM.from_pretrained(model_dir)

class DataInput(BaseModel):
    inputText: str
class SummaryOut(BaseModel):
    summary: str

# mongodb 연결
client = pymongo.MongoClient('mongodb://sam9:sam9mo!!@59.3.28.12:27017/')
db = client["sam9mo"]
collection = db["news"]

def summary_content_process(Text_data: DataInput):
    returnSummary: SummaryOut = None
    try:
        # print("Text_data\n", Text_data)
        text: str =  Text_data["inputText"]

        raw_input_ids = tokenizer.encode(text)
        input_ids = [tokenizer.bos_token_id] + raw_input_ids + [tokenizer.eos_token_id]

        summary_ids = model.generate(torch.tensor([input_ids]),  num_beams=4,  max_length=4000,  eos_token_id=1)
        predicted_title = tokenizer.decode(summary_ids.squeeze().tolist(), skip_special_tokens=True)
        
        returnSummary: SummaryOut = predicted_title
    except Exception as e:
        returnSummary: SummaryOut = str(e)
        send_function_status(
            "news", 
            "content_summary", 
            "Fail", 
            f'but process continue.\n Local Summary EXCEPTION => {e}', 
            Text_data["_id"]
        )
        
        print(f'Local Summary EXCEPTION => {e}')
        
    return returnSummary

# offsetValue_save.txt에서 마지막 실행 offset변수 불러오기
def local_content_summary():

    print("content_summary_consumer start....")
    for message in consumer:
        print("partition_number : ", message.partition, "offset_number : ", message.offset)
        send_function_status(
            "news", 
            "content_summary", 
            "Success"
        )
        
        find_id_query = {
            "_id" : message.value['_id']
        }
        
        insert_news_content = {
            "news_content" : message.value['news_content']
        }
        update_news_content_query = {
            "$set": insert_news_content
        }
        
        try:
            collection.update_one(find_id_query, update_news_content_query)
        except Exception as ex:
            send_function_status(
                "news", 
                "content_summary", 
                "Fail", 
                f'but process continue.\n {ex}', 
                message.value['_id']
            )
            print("뉴스 전체 내용 삽입하는데 문제", "id", message.value['_id'])

        Text_data = {
            "inputText" : message.value['news_content'], 
            "news_id" : message.value['_id'] 
        }

        content_summary = summary_content_process(Text_data)
        content_summary = ' '.join(content_summary.split())
        
        update_content_summary_query = {
            "$set" : {
                "content_summary" : content_summary
            }
        }
        
        try:
            collection.update_one(find_id_query, update_content_summary_query, upsert=False)
        except Exception as ex:
            send_function_status(
                "news",
                "content_summary", 
                "Fail", 
                f'but process continue.\n {ex}', 
                message.value['_id']
            )
            print("뉴스 기사 요약 내용 삽입하는데 문제", "id", message.value['_id'])
        
local_content_summary()
send_function_status(
    "news", 
    "content_summary", 
    "Fail", 
    "process stop."
)
