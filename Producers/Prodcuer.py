import boto3
import json
import time
import uuid
import random
from datetime import datetime
kinesis = boto3.client('kinesis', region_name='us-east-1')
STREAM_NAME = 'supplychain-events'
EVENT_TYPES = ['telemetry','order','shipment']
def gen_event():
    event = {
    'event_id': str(uuid.uuid4()),
    'event_type': random.choices(EVENT_TYPES, weights=[70,20,10])[0],
    'device_id': f"device-{random.randint(1,20)}",
    'value': random.random()*100,
    'ingest_ts': datetime.utcnow().isoformat()
    }
    return event
if __name__ == '__main__':
    while True:
        records = []
        for _ in range(100):
            e = gen_event()
            records.append({'Data': json.dumps(e), 'PartitionKey': e['device_id']})
        resp = kinesis.put_records(Records=records, StreamName=STREAM_NAME)
        print('Wrote batch, failed:', resp['FailedRecordCount'])
        time.sleep(1)
