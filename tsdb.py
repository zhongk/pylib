import socket, select, http.client, json, time

class Connection:
	def __init__(self, host='localhost', port=4242, timeout=2.0):
		self.address = (host, port)
		self.timeout = timeout
		self.sock = None
		self.connect()
	
	def connect(self):
		if not self.sock:
			self.sock = socket.socket()
		try:
			self.sock.settimeout(self.timeout)
			self.sock.connect(self.address)
		except:
			self.close()
			raise
	
	def close(self):
		if self.sock:
			self.sock.close()
			self.sock = None
	
	@property
	def closed(self):
		return (self.sock is None)
	
	def put(self, metric, timestamp, value, **kws):
		float(value)
		if not kws:
			raise ValueError('Missing tags')
		if self.closed:
			self.connect()
		
		if not timestamp: timestamp = time.time()
		tagvals = ' '.join(['{}={}'.format(k,v) for k,v in kws.items() if v not in (None,'')])
		line = str.format('put {} {} {} {}\n', metric, int(timestamp), value, tagvals)
		msg = line.encode('latin-1')
		try:
			while select.select([self.sock.fileno()], [], [], 0)[0]:
				if not self.sock.recv(2048):
					raise BrokenPipeError(socket.errno.EPIPE, 'Connection closed by peer')
			self.sock.send(msg)
		except:
			self.close()
			raise

class HTTPConnection:
	def __init__(self, host='localhost', port=4242, timeout=2.0):
		self.http = http.client.HTTPConnection(host, port, timeout=timeout)
		self.config = self.request('GET', '/api/config')
		self.auto_commit = False
		self.metrics, self.metrics_length = [], 0
	
	def close(self):
		self.http.close()
	
	def onRequest(self, request, response):
		res = None
		# callback function: self.cb_request(method, url, request, reply|except)
		if hasattr(self, 'cb_request') and callable(self.cb_request):
			if isinstance(response, http.client.HTTPException):
				exc = response
				res = self.cb_request(request['method'], request['url'], request['body'], exc)
			else:
				res = response['reply']
				self.cb_request(request['method'], request['url'], request['body'], res)
		return res
	
	def request(self, method, url, body=None):
		self.last_request, self.last_response = dict(method=method, url=url, body=body), None
		try:
			self.http.request(method, url, body, headers={"Content-Type":"application/json;charset=UTF-8"})
			response = self.http.getresponse()
		except http.client.HTTPException as exc:
			if self.onRequest(self.last_request, exc) is None:
				raise exc
			return None
		
		total_bytes = int(response.getheader('Content-Length'))
		data = bytearray(total_bytes)
		response.readinto(data)
		reply = json.loads(data.decode('utf-8'))
		self.last_response = dict(status=response.status, reason=response.reason, reply=reply)
		self.onRequest(self.last_request, self.last_response)
		return reply
	
	def put(self, metric, timestamp, value, **kws):
		max_chunk = int(self.config['tsd.http.request.max_chunk'])
		if not timestamp: timestamp = time.time()
		tags = {k:v for k,v in kws.items() if v not in (None,'')}
		datapoint = json.dumps(dict(metric=metric, timestamp=int(timestamp), value=value, tags=tags))
		if len(datapoint)+self.metrics_length+2 > max_chunk:
			if not self.auto_commit:
				raise http.client.LineTooLong('Too long to send metrics')
			self.commit()
		
		self.metrics.append(datapoint)
		self.metrics_length += len(datapoint) + 2
		return (max_chunk - self.metrics_length)
	
	def len(self):
		return len(self.metrics)
	
	def commit(self):
		if not self.metrics: return
		body = '[' + ', '.join(self.metrics) + ']'
		self.metrics, self.metrics_length = [], 0
		reply = self.request('POST', '/api/put?details', body)
		return reply
	
	def annotation(self, tsuid, start, end=0, description='', notes='', custom={}):
		req_data = {
			"startTime": start,
			"endTime": end,
			"tsuid": tsuid,
			"description": description,
			"notes": notes,
			"custom": custom
		}
		return self.request('POST', '/api/annotation', json.dumps(req_data))
		
	def query(self, metric, start, end=None, tags={}, aggregator='avg', downsample=None):
		query_data = {
			"start": start,
			"end": end,
			"noAnnotations": True,
			"queries": [
				{
					"aggregator": aggregator,
					"downsample": downsample,
					"metric": metric,
					"tags": tags
				}
			]
		}
		
		reply = self.request('POST', '/api/query', json.dumps(query_data))
		if not reply: return reply
		if self.last_response['status'] != 200:
			error = reply['error']
			return {'error':{'code':error['code'], 'message':error['message']}}
		dps = [(int(timestamp), value) for timestamp, value in reply[0]['dps'].items()]
		dps.sort()
		return dps
	
	def suggest(self, type, q=None, max=25):
		assert(type in {'metrics', 'tagk', 'tagv'})
		return self.request('POST', '/api/suggest', json.dumps({'type':type, 'q':q, 'max':max}))
	
	def getuid(self, metrics=[], tagk=[], tagv=[]):
		reply = self.request('POST', '/api/uid/assign', json.dumps({'metric':metrics, 'tagk':tagk, 'tagv':tagv}))
		uids = dict(metric=reply['metric'], tagk=reply['tagk'], tagv=reply['tagv'])
		metric_exists, tagk_exists, tagv_exists = reply.get('metric_errors',{}), reply.get('tagk_errors',{}), reply.get('tagv_errors',{})
		for k,v in metric_exists.items():
			if v.startswith('Name already exists with UID:'): uids['metric'][k] = v[-6:]
		for k,v in tagk_exists.items():
			if v.startswith('Name already exists with UID:'): uids['tagk'][k] = v[-6:]
		for k,v in tagv_exists.items():
			if v.startswith('Name already exists with UID:'): uids['tagv'][k] = v[-6:]
		return uids

###############################################################################################################################

if __name__ == "__main__":
	from datetime import datetime
	from pprint import pprint
	metric = 'python.tsd.test'
	start = datetime(2017, 6, 2, 12).timestamp()
	tsd = HTTPConnection('192.168.17.226')
#	tsd.put(metric, 0, 10, host='host1')
#	tsd.put(metric, 0, 11, host='host1', name='test.1')
#	tsd.put(metric, 0, 12, host='host1', name='test.2')
#	tsd.put(metric, 0, 20, host='host2')
#	tsd.put(metric, 0, 21, host='host2', name='test.1')
#	tsd.put(metric, 0, 22, host='host2', name='test.2')
#	print(tsd.commit())
	tsd.query(metric, '2d-ago', tags={'host':'host1'}, aggregator='avg')
	print(tsd.last_response['reply'])
	
#	pprint(tsd.annotation('00009A000004000049000005000048', int(time.time())-60, 0, 'Test for annotation 1'))
#	pprint(tsd.annotation('00009A000005000048000004000049', int(time.time()), 0, 'Test for annotation 2'))
#	req = {'start':'5m-ago', 'end':'now', 'showTSUIDs':True, 'queries':[{'aggregator':'sum', 'metric':metric, 'tags':{'host':'host1', 'name':'test.1'}}]}
	#pprint(tsd.request('POST', '/api/query', json.dumps(req)))
