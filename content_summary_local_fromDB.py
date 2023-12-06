import json
import requests

from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
import torch
import nltk
nltk.download('punkt')

from kafka import KafkaConsumer

from pydantic import BaseModel

import pymongo

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

def summary_content_process_local(Text_data: DataInput):
    returnSummary: SummaryOut = None
    try:
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

def summary_content_process(Text_data):
    response = requests.post("http://221.156.60.18:8010/summary", data=json.dumps(Text_data))
    return response.json()

CARD_TOPIC = 'send_news_content'

brokers = ['59.3.28.12:9092', '59.3.28.12:9093', '59.3.28.12:9094']
consumer = KafkaConsumer(
    CARD_TOPIC,
    bootstrap_servers=brokers,
    value_deserializer=lambda m:json.loads(m.decode('utf-8'))
)

# offsetValue_save.txt에서 마지막 실행 offset변수 불러오기
def local_content_summary():

    find_content_summary_query = {"_id" : "202311300004968303"}
    find_doc_list = list(collection.find(find_content_summary_query))
    
    for doc in find_doc_list:
        
        find_id_query = {"_id" : doc['_id']}
        
        doc_news_content = doc['news_content']
        
        if len(doc_news_content) >= 2000:
            doc_news_content = doc_news_content[:1900]
        
        Text_data = {"inputText" : doc_news_content}

        content_summary = summary_content_process_local(Text_data)
        # content_summary = summary_content_process(Text_data)
        content_summary = ' '.join(content_summary.split())
        
        try:
            collection.update_one(find_id_query, {"$set" : {"content_summary" : content_summary}}, upsert=False)
            print("뉴스 기사 요약 내용 저장 완료")
        except Exception as ex:
            print("뉴스 기사 요약 내용 삽입하는데 문제")
        
local_content_summary()