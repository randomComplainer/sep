import json
import re
import sys
import datetime
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional


@dataclass
class DataEventIdentifier:
    conn_id: str
    msg: str
    size: int

def parse_parent_conn_id(obj: dict) -> Optional[str]:
    spans = obj.get('spans', [])
    for span in spans:
        if span.get('name') == 'conn lifetime':
            conn_id = span.get('conn_id')
            assert conn_id is not None
            return conn_id

    return None

def parse_conn_id(obj: dict) -> Optional[str]:
    spand = obj.get('span', {})
    return spand.get('conn_id')

def parse_data_event_identifier_sent(obj: dict):
    conn_id = parse_parent_conn_id(obj)
    assert conn_id is not None

    span_msg = obj.get('span', {}).get('msg', '')
    assert span_msg is not None

    size = 0
    match = re.search(r'data: (\d+)', span_msg)
    if match:
        size = int(match.group(1))

    return DataEventIdentifier(conn_id, span_msg, size)

def parse_data_event_identifier_recv(obj: dict) -> Optional[DataEventIdentifier]:
    try:
        conn_id = parse_parent_conn_id(obj)
        assert conn_id is not None

        msg = obj.get('fields', {}).get('msg', '')
        assert msg is not None

        size = 0
        match = re.search(r'data: (\d+)', msg)
        if match:
            size = int(match.group(1))

        return DataEventIdentifier(conn_id, msg, size)
    except:
        return None

@dataclass
class SentEvent:
    timestamp: datetime.datetime
    id: DataEventIdentifier

def parse_sent_event(line) -> Optional[SentEvent]: 
    try:
        obj = json.loads(line)
        spans = obj.get('spans', [])
        fields_msg = obj.get('fields', {}).get('message', '')
        span_name = obj.get('span', {}).get('name', '')
        span_msg = obj.get('span').get('msg')
        
        # Check if this is a sent event
        is_sent = (fields_msg == 'close' and 
                  'Data' in span_msg and
                  span_name == 'send msg' and
                  'conn lifetime' in [sp.get('name') for sp in spans] and 
                  'send loop' in [sp.get('name') for sp in spans])

        if not is_sent:
            return None

        id = parse_data_event_identifier_sent(obj)
        if id is None:
            return None

        timestamp = datetime.datetime.fromisoformat(obj['timestamp'])
        
        return SentEvent(timestamp, id)
    except:
        return None

@dataclass
class ReceivedEvent:
    timestamp: datetime.datetime
    id: DataEventIdentifier

def parse_received_event(line) -> ReceivedEvent | None:
    try:
        obj = json.loads(line)
        spans = obj.get('spans', [])
        fields_msg = obj.get('fields', {}).get('message', '')
        span_name = obj.get('span', {}).get('name', '')
        msg = obj.get('fields', {}).get('msg', '')
        
        # Check if this is a received event
        is_received = (fields_msg == 'message from stream' and
                      'Data' in msg and
                      span_name == 'recv loop' and
                      'conn lifetime' in [span.get('name') for span in spans] and 
                      'recv loop' in [span.get('name') for span in spans]);
        
        if not is_received:
            return None
        
        id = parse_data_event_identifier_recv(obj)
        if id is None:
            return None

        timestamp = datetime.datetime.fromisoformat(obj['timestamp'])
        
        return ReceivedEvent(timestamp, id)   
    except:
        return None

@dataclass
class DisconnectedEvent:
    conn_id: str

def parse_disconnected_event(obj) -> DisconnectedEvent | None:
    try:
        conn_id = parse_conn_id(obj)
        if conn_id is None:
            return None

        return DisconnectedEvent(conn_id)
    except:
        return None

def main(source_path, dest_path) -> List[Tuple[DataEventIdentifier, datetime.timedelta]] :
    result: List[Tuple[DataEventIdentifier, datetime.timedelta]] = []

    # [conn_id][msg] = SentEvent
    sent_events:Dict[str, Dict[str,SentEvent]] = {}

    # received_cache:Dict[str, Dict[DataEventIdentifier, ReceivedEvent]] = {}

    with open(source_path, 'r') as src_file:
        for line in src_file:
            sent_event = parse_sent_event(line)
            if not sent_event:
                continue

            if sent_event.id.conn_id not in sent_events:
                sent_events[sent_event.id.conn_id] = {}

            sent_events[sent_event.id.conn_id][sent_event.id.msg] = sent_event
            

    # Process destination file (received events)
    with open(dest_path, 'r') as dest_file:
        for line in dest_file:
            recv_event = parse_received_event(line)
            if recv_event:
                sent_event = sent_events.get(recv_event.id.conn_id, {}).get(recv_event.id.msg)
                assert sent_event is not None

                latency = recv_event.timestamp - sent_event.timestamp
                result.append((recv_event.id, latency))
            else:
                disconn_event = parse_disconnected_event(line)
                if disconn_event:
                    sent_events.pop(disconn_event.conn_id, None)

    return result

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print("Usage: python extract_latency.py <source_file> <dest_file>")
        sys.exit(1)
    
    for [msg_id, delta] in main(sys.argv[1], sys.argv[2]):
        print(delta)
        # print(delta, msg_id.msg, msg_id.conn_id)
