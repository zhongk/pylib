import socket, select, http.client
import time, os, sys, queue, json


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
		if not tags:
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
	def __init__(self, host='localhost', port=4242, timeout=2.0, **kws):
		self.http = http.client.HTTPConnection(host, port, timeout=timeout)
		self.auto_commit = kws.get('auto_commit', False)
		self.metrics, self.metrics_length = [], 0
	
	@property
	def config(self):
		if not hasattr(self, '__config'):
			self.__config = self.request('GET', '/api/config')
		return self.__config
	
	def close(self):
		self.http.close()
	
	def onRequest(self, request, response):
		res = None
		# callback function: self.cb_request(method, url, request, reply|exc_info)
		if hasattr(self, 'cb_request') and callable(self.cb_request):
			if isinstance(response, tuple):
				exc_info = response
				res = self.cb_request(request['method'], request['url'], request['body'], exc_info)
			else:
				res = response['reply']
				self.cb_request(request['method'], request['url'], request['body'], res)
		return res
	
	def _request(self, method, url, body=None):
		self.last_request, self.last_response = dict(method=method, url=url, body=body), None
		self.http.request(method, url, body, headers={"Content-Type":"application/json;charset=UTF-8"})
	
	def get_response(self):
		response = self.http.getresponse()
		total_bytes = int(response.getheader('Content-Length'))
		data = bytearray(total_bytes)
		response.readinto(data)
		reply = json.loads(data.decode('utf-8'))
		self.last_response = dict(status=response.status, reason=response.reason, reply=reply)
		return reply
	
	def request(self, method, url, body=None):
		try:
			self._request(method, url, body)
			reply = self.get_response()
			self.onRequest(self.last_request, self.last_response)
			return reply
		except:
			if self.onRequest(self.last_request, sys.exc_info()) is None:
				raise
			return None
	
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


class TSDB:
	from multiprocessing.dummy import Process, Queue, Event
	
	def __init__(self, host='localhost', port=4242, timeout=2.0, **kws):
		self.host, self.port, self.timeout = host, port, timeout
		self.metrics, self.metrics_length = [], 0
		tsd = HTTPConnection(host, port, timeout)
		max_chunk = kws.get('max_chunk', 0)
		if max_chunk <= 0:
			self.max_chunk = int(tsd.config['tsd.http.request.max_chunk'])
		else:
			self.max_chunk = min(max_chunk, int(tsd.config['tsd.http.request.max_chunk']))
		tsd.close()
		
		self._processes = kws.get('processes', os.cpu_count())
		self._requests = self.Queue(kws.get('qsize', self._processes))
		self._terminated = self.Event()
		self._callback = kws.get('callback')
		self._error_callback = kws.get('error_callback')
		self._handlers = [self.__new_handler()]
	
	def __del__(self):
		if self._handlers:
			self.close()
	
	def close(self):
		self.commit()
		self._terminated.set()
		for handler in self._handlers:
			handler.join()
		self._handlers.clear()
		self._terminated.clear()
	
	def __len__(self):
		return len(self.metrics)
	
	def put(self, metric, timestamp, value, **kws):
		if not timestamp: timestamp = time.time()
		tags = {k:v for k,v in kws.items() if v not in (None,'')}
		datapoint = json.dumps(dict(metric=metric, timestamp=int(timestamp), value=value, tags=tags))
		if len(datapoint)+self.metrics_length+2 > self.max_chunk:
			self.commit()
		
		self.metrics.append(datapoint)
		self.metrics_length += len(datapoint) + 2
		return (self.max_chunk - self.metrics_length)
	
	def commit(self):
		if not self.metrics: return
		if self._requests.full() and len(self._handlers)<self._processes:
			self._handlers.append(self.__new_handler())
		self._requests.put('[' + ', '.join(self.metrics) + ']')
		self.metrics, self.metrics_length = [], 0
	
	def __new_handler(self):
		handler = self.Process(target=self.__handler, 
		                       args=(self.host, self.port, self.timeout,
		                             self._requests, self._terminated,
		                             self._callback, self._error_callback))
		handler.start()
		return handler
	
	@staticmethod
	def __handler(host, port, timeout, requests, terminated, callback, error_callback):
		tsd = HTTPConnection(host, port, timeout)
		while True:
			try:
				request_body = requests.get(timeout=0.1)
				reply = tsd.request('POST', '/api/put?details', request_body)
				if callable(callback):
					callback(request_body, reply)
			
			except queue.Empty:
				if terminated.is_set():
					tsd.close()
					break
			except:
				if callable(error_callback):
					try:
						error_callback(request_body, sys.exc_info())
					except:
						pass
