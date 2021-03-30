#!/usr/bin/env python3
import re
import sys
import os
import subprocess
import traceback
from itertools import chain
from datetime import datetime, timedelta
from pprint import pprint
from web import Application, Response, FileResponse, main, send_file, send_json, URLConverter
from typing import Any


def match(pattern, obj):
	"""See whether pattern is a subset of obj. Not-recursive."""
	for key, val in pattern.items():
		if key not in obj:
			return False
		if obj[key] != val:
			return False
	return True


class Job(dict):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		
		if 'JobName' in self:
			if match := re.match(r'^(shard|merge-shard|clean-shard|dedupe|split|translate|tokenise|align|clean)-([a-z]{2})-([a-z]+[a-z0-9\-]*)$', self['JobName']):
				self.step, self.language, self.collection = match[1], match[2], match[3]
			elif match := re.match(r'^(reduce-tmx|reduce-tmx-deferred|reduce-classified|reduce-filtered)-([a-z]{2})$', self['JobName']):
				self.step, self.language, self.collection = match[1], match[2], None
			elif match := re.match(r'^(warc2text)-([a-z]+[a-z0-9\-]*)$', self['JobName']):
				self.step, self.language, self.collection = match[1], None, match[2]


class Collection:
	def __init__(self, path):
		self.path = path

	@property
	def languages(self):
		if not os.path.isdir(self.path + '-shards/'):
			return frozenset()
		return frozenset(entry.name
			for entry in os.scandir(self.path + '-shards/')
			if entry.is_dir() and re.match(r'^[a-z]{1,2}(\-[a-zA-Z]+)?$', entry.name))


class Slurm:
	def scheduled_jobs(self, since=None):
		if since is None:
			since = datetime(1970, 1, 1)

		since_timestamp = since.strftime('%Y%m%d%H%M%S')
		
		with open('.schedule-log') as fh:
			lines = fh.readlines()
		for line in lines:
			timestamp, line_job_id, arguments = line.rstrip().split(' ', maxsplit=2)
			if not line_job_id.isnumeric():
				continue
			if timestamp.isnumeric() and timestamp >= since_timestamp:
				yield from self.jobs_from_cli_args({'JobId': line_job_id, 'SubmitTime': timestamp}, arguments.split(' '))

	def current_jobs(self):
		output = subprocess.check_output(['squeue',
			'--user', os.getenv('USER'),
			'--format', '%A|%i|%K|%F|%C|%b|%j|%P|%r|%u|%y|%T|%M|%b|%N'])
		lines = output.decode().splitlines()
		mapping = {
			'JOBID': 'JobId',
			'NAME': 'JobName',
			'STATE': 'State',
			'ARRAY_TASK_ID': 'ArrayTaskId',
			'ARRAY_JOB_ID': 'ArrayJobId',
			'TIME': 'Elapsed',
			'REASON': 'Reason',
		}
		headers = [mapping.get(header, header) for header in lines[0].strip().split('|')]
		for line in lines[1:]:
			job = dict(zip(headers, line.strip().split('|')))
			if 'ArrayTaskId' in job and '-' in job['ArrayTaskId']:
				for task_id in self.parse_job_arrays(job['ArrayTaskId']):
					yield Job({**job, 'JobId': '{}_{}'.format(job['ArrayJobId'], task_id), 'ArrayTaskId': task_id})
			elif 'ArrayTaskId' in job and job['ArrayTaskId'] != 'N/A':
				yield Job({**job, 'JobId': '{}_{}'.format(job['ArrayJobId'], job['ArrayTaskId'])})
			else:
				yield Job(job)

	def accounting_jobs(self, additional_args=[]):
		output = subprocess.check_output(['sacct',
			'--parsable2',
			'--user', os.getenv('USER'),
			'--format', 'ALL',
			*additional_args
		])
		lines = output.decode().splitlines()
		mapping = {
			'JobIDRaw': 'JobIDRaw',
			'JobState': 'State',
			'JobID': 'JobId'
		}
		headers = [mapping.get(header, header) for header in lines[0].strip().split('|')]
		for line in lines[1:]:
			job = dict(zip(headers, line.strip().split('|')))
			match = re.match(r'^(\d+)_(?:\d+|\[(\d+)(?:-(\d+))?\])$', job['JobId'])

			# job with suffix, like \d_\d.batch or .extern
			if not match:
				continue

			# It's a collapsed job array!
			if match[3]:
				for task_id in range(int(match[2]), int(match[3]) + 1):
					yield Job({**job,
						'JobId': '{}_{}'.format(match[1], task_id),
						'ArrayJobId': match[1],
						'ArrayTaskId': str(task_id)
					})
			elif match[2]:
				yield Job({
					**job,
					'JobId': '{}_{}'.format(match[1], int(match[2])),
					'ArrayJobId': match[1],
					'ArrayTaskId': match[2]
				})
			else:
				yield Job(job)

	def jobs(self, since=None, include_completed=True):
		scheduled = {job['JobId']: job for job in self.scheduled_jobs(since=since)}

		array_job_ids = set(job.get('ArrayJobId', job['JobId']) for job in scheduled.values())

		sources = []

		if include_completed:
			sources.append(self.accounting_jobs(['--jobs', ','.join(array_job_ids)]))

		sources.append(self.current_jobs())		

		for job in chain(*sources):
			try:
				if job['JobId'] in scheduled:
					scheduled[job['JobId']].update(job)
				else:
					scheduled[job['JobId']] = job
			except:
				pprint(job, stream=sys.stderr)
				raise
		
		return list(scheduled.values())

	def scheduled_job(self, job_id):
		if '_' in job_id:
			job_pattern = dict(zip(['ArrayJobId', 'ArrayTaskId'], job_id.split('_', maxsplit=1)))
			job_id = job_pattern['ArrayJobId']
		else:
			job_pattern = {'JobId': job_id}

		with open('.schedule-log') as fh:
			for line in fh:
				timestamp, line_job_id, arguments = line.rstrip().split(' ', maxsplit=2)
				if not line_job_id.isnumeric():
					continue
				if line_job_id != job_id:
					continue
				for job in self.jobs_from_cli_args({'JobId': line_job_id, 'SubmitTime': timestamp}, arguments.split(' ')):
					if match(job_pattern, job):
						return job

		return None

	def current_job(self, job_id):
		output = subprocess.check_output(['scontrol', '--details', 'show', 'job', job_id])
		job = dict()
		for line in output.decode().splitlines():
			for match in re.finditer(r'\b(?P<key>[A-Z][A-Za-z_\/:]+)=(?P<value>[^\s]+)', line):
				if match.group('key') in {'Command'}:
					job[match.group('key')] = line[match.end('key') + 1:]
					break
				else:
					job[match.group('key')] = match.group('value')
		return Job({
			**job,
			'State': job['JobState'],
			'JobId': '{}_{}'.format(job['ArrayJobId'], job['ArrayTaskId']) if 'ArrayJobId' in job else job['JobId']
		})

	def accounting_job(self, job_id):
		return next(iter(self.accounting_jobs(['--job', job_id])), None)

	def job(self, job_id):
		job = self.scheduled_job(job_id)
		if not job:
			return None

		try:
			job.update(self.accounting_job(job_id) or {})
		except subprocess.CalledProcessError:
			pass

		try:
			job.update(self.current_job(job_id))
		except subprocess.CalledProcessError:
			pass
		return job

	def parse_job_arrays(self, job_arrays):
		for job_array in job_arrays.split(','):
			if '-' in job_array:
				start, end = job_array.split('-', maxsplit=1)
				yield from range(int(start), int(end) + 1)
			else:
				yield int(job_array)

	def normalize_cli_args(sel, args):
		for arg in args:
			match = re.match(r'^(--.+?)=(.+?)$', arg)
			if match:
				yield match.group(1)
				yield match.group(2)
			else:
				yield arg

	def jobs_from_cli_args(self, job, args):
		it = iter(self.normalize_cli_args(args))
		job_array = None
		for arg in it:
			if arg in {'--parsable', '--verbose', '--exclusive'}: # options we ignore
				pass
			elif arg in {'--nice', '--mem-per-cpu', '--export'}: # options with arguments we ignore
				next(it)
			elif arg in {'--dependency', '-d'}:
				job['Dependency'] = next(it)
			elif arg in {'--ntasks', '-n'}:
				job['NumTasks'] = next(it)
			elif arg in {'--nodes', '-N'}:
				job['NumNodes'] = next(it)
			elif arg in {'--account', '-A'}:
				job['Account'] = next(it)
			elif arg in {'--partition', '-p'}:
				job['Partition'] = next(it)
			elif arg == '-J':
				job['JobName'] = next(it)
			elif arg in {'-a', '--array'}:
				job_array = next(it)
			elif arg == '--time':
				job['TimeLimit'] = next(it)
			elif arg == '--cpus-per-task':
				job['NumCPUs'] = next(it)
			elif arg == '-e':
				job['StdErr'] = next(it)
			elif arg == '-o':
				job['StdOut'] = next(it)
			elif arg[0] != '-':
				job['Command'] = list(it)
			else:
				raise ValueError("Cannot parse arg '{}'".format(arg))

		if not job_array:
			yield Job({
				**job,
				'StdOut': job.get('StdOut', '').replace('%A', job['JobId']),
				'StdErr': job.get('StdOut', '').replace('%A', job['JobId']),
			})
		else:
			for array_task_id in self.parse_job_arrays(job_array):
				yield Job({
					**job,
					'JobId': '{}_{}'.format(job['JobId'], array_task_id),
					'ArrayJobId': job['JobId'],
					'ArrayTaskId': str(array_task_id),
					'StdOut': job.get('StdOut', '').replace('%A', job['JobId']).replace('%a', str(array_task_id)),
					'StdErr': job.get('StdErr', '').replace('%A', job['JobId']).replace('%a', str(array_task_id)),
				})


def read_collections():
	output = subprocess.check_output(['bash',
		'--init-file', 'env/init.sh',
		'-c', 'source config.sh; for k in "${!COLLECTIONS[@]}"; do printf "%s\t%s\n" $k ${COLLECTIONS[$k]}; done'])
	lines = output.decode().splitlines()
	collections = {}
	for line in lines:
		collection, path = line.split('\t', maxsplit=1)
		collections[collection] = Collection(path)
	return collections

slurm = Slurm()

collections = read_collections()

app = Application()

@app.url_type('job')
class JobConverter(URLConverter):
	def to_pattern(self) -> str:
		return r'\d+(?:_\d+)?'

	def to_python(self, val: str) -> int:
		return slurm.job(val)

	def to_str(self, val: Any) -> str:
		if 'ArrayJobId' in val:
			return '{:d}_{:d}'.format(int(val['ArrayJobId']), int(val['ArrayTaskId']))
		else:
			return '{:d}'.format(int(val['JobId']))


@app.route('/')
def index(request):
	return send_file(os.path.join(os.path.dirname(__file__), 'dashboard.html'))


@app.route('/<str:filename>.html')
def index(request, filename):
	path = os.path.join(os.path.dirname(__file__), '{}.html'.format(filename))
	if not os.path.exists(path):
		return Response("File not found: {}".format(path), status_code=404)
	return send_file(path)


@app.route('/collections/')
def list_collections(request):
	return send_json([
		{
			'name': name,
			'languages': collection.languages
		} for name, collection in collections.items()
	])


@app.route('/jobs/')
@app.route('/jobs/delta/<str:timestamp>', name='list_jobs_delta')
def list_jobs(request, timestamp=None):
	now = datetime.now()
	if timestamp:
		since = datetime.fromisoformat(timestamp)
	else:
		since = datetime.now() - timedelta(days=7)
	return send_json({
		'timestamp': now.isoformat(),
		'jobs': [
			{
				'id': job['JobId'],
				'step': job.step,
				'language': job.language,
				'collection': job.collection,
				'slurm': job, # the dict data
				'stdout': app.url_for('show_stream', job=job, stream='stdout'),
				'stderr': app.url_for('show_stream', job=job, stream='stderr'),
				'link': app.url_for('show_job', job=job),
			}
			for job in slurm.jobs(since=since, include_completed=timestamp is None)
			if hasattr(job, 'language') and hasattr(job, 'collection')
		]
	})


def tail(filename):
	if os.path.exists(filename):
		with open(filename, 'r') as fh:
			return fh.read()


@app.route('/jobs/<job:job>/')
def show_job(request, job):
	return send_json({
		'id': job['JobId'],
		'slurm': job,
		'stdout': app.url_for('show_stream', job=job, stream='stdout'),
		'stderr': app.url_for('show_stream', job=job, stream='stderr')
	})


class Tailer:
	def __init__(self, *paths):
		self.proc = subprocess.Popen(['tail', '-n', '+0', '-f', *paths], stdout=subprocess.PIPE)

	def fileno(self):
		return self.proc.stdout.fileno()

	def read(self, *args, **kwargs):
		return self.proc.stdout.read(*args, **kwargs)

	def close(self):
		self.proc.stdout.close()
		self.proc.terminate() # closing otherwise tail won't notice
		self.proc.wait() # wait to prevent zombie


@app.route('/jobs/<job:job>/<any(stdout,stderr):stream>')
def show_stream(request, stream, job):
	mapping = {
		'stdout': 'StdOut',
		'stderr': 'StdErr'
	}

	return FileResponse(Tailer(job[mapping[stream]]))

if __name__ == "__main__":        
	main(app)
