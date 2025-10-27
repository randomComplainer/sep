# outgoing - client emits
jq < ./log/client.json | \
	jq 'select((.spans | type =="array") and any(.spans[]; .name=="session" and .session_id==46868))' | \
	jq 'select(.spans[].name=="proxyee to server" and .fields.message=="new" and .span.name=="emit event:")' | 
	jq .span.evt

# outgoing - server receives
jq < ./log/server.json | \
	jq 'select((.spans | type =="array") and any(.spans[]; .name=="session" and .session_id==46868))' | \
	jq 'select(.spans[].name=="client to target" and .fields.message=="command received")'  |\
	jq .fields.cmd

# outgoing - server emits
jq < ./log/server.json | \
	jq 'select((.spans | type =="array") and any(.spans[]; .name=="session" and .session_id==46868))' | \
	jq 'select(.spans[].name=="client to target" and .fields.message=="new" and .span.name=="emit event:")' | 
	jq .span.evt

# outgoing - client receives
jq < ./log/client.json | \
	jq 'select((.spans | type =="array") and any(.spans[]; .name=="session" and .session_id==46868))' | \
	jq 'select(.spans[].name=="proxyee to server" and .fields.message=="command received")' |\
	jq .fields.cmd


# incoming - server emits
jq < ./log/server.json | \
	jq 'select((.spans | type =="array") and any(.spans[]; .name=="session" and .session_id==46868))' | \
	jq 'select(.spans[].name=="target to client" and .fields.message=="new" and .span.name=="emit event:")' | 
	jq .span.evt

# incoming - client receives
jq < ./log/client.json | \
	jq 'select((.spans | type =="array") and any(.spans[]; .name=="session" and .session_id==46868))' | \
	jq 'select(.spans[].name=="server to proxyee" and .fields.message=="command received")' |\
	jq .fields.cmd

# incoming - client emits
jq < ./log/client.json | \
	jq 'select((.spans | type =="array") and any(.spans[]; .name=="session" and .session_id==46868))' | \
	jq 'select(.spans[].name=="server to proxyee" and .fields.message=="new" and .span.name=="emit event:")' | 
	jq .span.evt

# incoming - server receives
jq < ./log/server.json | \
	jq 'select((.spans | type =="array") and any(.spans[]; .name=="session" and .session_id==46868))' | \
	jq 'select(.spans[].name=="target to client" and .fields.message=="command received")' |\
	jq .fields.cmd





