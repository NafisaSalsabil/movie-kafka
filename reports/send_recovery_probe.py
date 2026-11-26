"""Container-only, explicitly labelled old-event traffic for recovery experiments."""
import csv,json,os,time
from kafka import KafkaProducer
row=next(csv.DictReader(open('ratings.csv')))
row={k:float(v) if k=='rating' else int(v) for k,v in row.items()}
# Separate old timestamps make the two probe records distinguishable.
path='reports/recovery_probes.json'
records=json.load(open(path)) if os.path.exists(path) else []
row['timestamp']-=86400+len(records)
producer=KafkaProducer(bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS','kafka:29092'),value_serializer=lambda v:json.dumps(v).encode(),acks='all')
metadata=producer.send('ratings',value=row).get(timeout=60)
producer.flush();producer.close()
records.append({'probe':len(records)+1,'sent_at_epoch':time.time(),'purpose':'Beyond-watermark event after forced consumer restart; expected dropped, source counts unchanged','row':row,'partition':metadata.partition,'offset':metadata.offset})
json.dump(records,open(path,'w'),indent=2)
print(json.dumps(records[-1]))
