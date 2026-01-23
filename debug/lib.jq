def with_fields(filter):
	select(.fields | filter);

def with_spans(filter):
	select((.spans | type == "array") and (.spans | any(filter)));

def error: "ERROR";
def warn: "WARN";
def debug: "DEBUG";
def info: "INFO";
def trace: "TRACE";
def levels:
	[error, warn, info, debug, trace];

def with_level(filter):
	select((.level | type == "string") and (.level | filter));

def with_target(filter):
	select((.target | type == "string") and (.target | filter));


# gte
def level($level_filter_str):
	with_level(. as $entry_level 
		| (levels | index($level_filter_str | ascii_upcase)) >= (levels | index($entry_level))
	);

def req_url(url): 
	with_fields(
		(.message=="request" and (.addr | contains(url)))
	);

def session(session_id):
	with_spans(.session_id==session_id and .name=="session");

def no_ping:
	.span.msg != "Ping" and .fields.message != "ping";

def target(target):
	with_target(. | contains(target));

def within_span(name):
	with_spans(.name | contains(name));

