#!/usr/bin/env python3
import re
import sys
import os
import subprocess
import json
from itertools import chain
from datetime import datetime, timedelta
from pprint import pprint
from web import Application, Response, FileResponse, main, send_file, send_json, URLConverter
from typing import Any, Callable, TypeVar, Optional, Dict, Tuple, List, Iterable, Iterator, Set, Union


T = TypeVar('T')

def none_throws(val: Optional[T]) -> T:
	if val is None:
		raise ValueError('val is None')
	return val


def match(pattern, obj):
	"""See whether pattern is a subset of obj. Not-recursive."""
	for key, val in pattern.items():
		if key not in obj:
			return False
		if obj[key] != val:
			return False
	return True


class Job(dict):
	__slots__ = (
		'step',
		'language',
		'collection'
	)

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		
		self.step, self.language, self.collection = None, None, None

		if 'JobName' in self:
			if match := re.match(r'^(shard|merge-shard|clean-shard|dedupe|split|translate|tokenise|align|fix|score|clean|)-([a-z]{2,3}(?:-[A-Z][a-z]+)?)-([a-z]+[a-z0-9_\-]*)$', self['JobName']):
				self.step, self.language, self.collection = match[1], match[2], match[3]
			elif match := re.match(r'^(reduce-tmx|reduce-tmx-deferred|reduce-classified|reduce-filtered)-([a-z]{2,3}(?:-[A-Z][a-z]+)?)$', self['JobName']):
				self.step, self.language, self.collection = match[1], match[2], None
			elif match := re.match(r'^(warc2text|pdf2warc)-([a-z]+[a-z0-9_\-]*)$', self['JobName']):
				self.step, self.language, self.collection = match[1], None, match[2]

		if 'ArrayTaskId' in self and self['ArrayTaskId'] == 'N/A':
			raise ValueError('Job {} as an ArrayTaskId of N/A'.format(self['JobId']))


class Collection:
	def __init__(self, path):
		self.path = path

	@property
	def languages(self):
		if not os.path.isdir(self.path + '-shards/'):
			return frozenset()
		return frozenset(entry.name
			for entry in os.scandir(self.path + '-shards/')
			if entry.is_dir() and re.match(r'^[a-z]{1,3}(?:\-[A-Z][a-z]+)?$', entry.name))


class Slurm:
	accounts: Set[str]

	def __init__(self, accounts:Iterable[str]):
		self.accounts = set(accounts)

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
				yield from self.jobs_from_cli_args({'JobId': line_job_id, 'SubmitTime': timestamp, 'State': 'PENDING'}, arguments.split(' '))

	def current_jobs(self, additional_args:List[str]=[]):
		output = subprocess.check_output(['squeue',
			'--account', ','.join(self.accounts),
			'--format', '%i|%K|%F|%C|%b|%j|%P|%r|%u|%y|%T|%M|%b|%N',
			*additional_args])
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

			if job['ArrayTaskId'] == 'N/A':
				yield Job({key: val for key, val in job.items() if key not in {'ArrayJobId', 'ArrayTaskId'}})
			elif 'ArrayTaskId' in job and job['ArrayTaskId'] != 'N/A':
				for task_id in self.parse_job_arrays(job['ArrayTaskId']):
					yield Job({**job, 'JobId': '{}_{}'.format(job['ArrayJobId'], task_id), 'ArrayTaskId': task_id})
			else:
				raise ValueError('Job interpretation error: {!r}'.format(job))

	def accounting_jobs(self, additional_args:List[str]=[]):
		output = subprocess.check_output(['sacct',
			'--parsable2',
			'--accounts', ','.join(self.accounts),
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
			match = re.match(r'^(?P<array_job_id>\d+)(?:_(?P<array_task_id>\d+)|\[(?P<array_task_start>\d+)(?:-(?P<array_task_end>\d+))?\])?$', job['JobId'])

			# job with suffix, like \d_\d.batch or .extern
			if not match:
				continue

			# It's a collapsed job array!
			if match['array_task_start'] and match['array_task_end']:
				for task_id in range(int(match['array_task_start']), int(match['array_task_end']) + 1):
					yield Job({**job,
						'JobId': '{}_{}'.format(match['array_job_id'], task_id),
						'ArrayJobId': match['array_job_id'],
						'ArrayTaskId': str(task_id)
					})
			elif match['array_task_id'] or match['array_task_start']:
				yield Job({
					**job,
					'JobId': '{}_{}'.format(match['array_job_id'], int(match['array_task_id'] or match['array_task_start'])),
					'ArrayJobId': match['array_job_id'],
					'ArrayTaskId': match['array_task_id'] or match['array_task_end']
				})
			else:
				yield Job(job)

	def jobs(self, since=None, include_completed=True):
		scheduled = {job['JobId']: job for job in self.scheduled_jobs(since=since)}

		array_job_ids = set(job.get('ArrayJobId', job['JobId']) for job in scheduled.values())

		sources = []

		if include_completed:
			sources.append(self.accounting_jobs(['--jobs', ','.join(array_job_ids)]))

		sources.append(self.current_jobs(['--jobs', ','.join(array_job_ids)]))

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
				for job in self.jobs_from_cli_args({'JobId': line_job_id, 'SubmitTime': timestamp, 'State': 'PENDING'}, arguments.split(' ')):
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
				if '%' in end:
					end = end[:end.find('%')] # strip concurrent task limit
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
				job['Command'] = tuple(it)
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
	# Allow us to use COLLECTIONS env variable to filter which collections we see
	selection = os.getenv('COLLECTIONS')
	if selection:
		subset = frozenset(selection.split(':'))
		collections = {
			name: collection
			for name, collection in collections.items()
			if name in subset
		}
	return collections


def read_config_var(varname):
	output = subprocess.check_output(['bash',
		'--init-file', 'env/init.sh',
		'-c', 'source config.sh; echo ${{{}}}'.format(varname)])
	return output.decode().strip()


def read_accounts():
	"""Get Slurm account names from environment or cirrus-scripts config"""
	return os.getenv('SBATCH_ACCOUNT', read_config_var('SBATCH_ACCOUNT')).split(',')


slurm = Slurm(read_accounts())

app = Application()

class JobList:
	jobs: Dict[str,Tuple[Job,datetime]]

	def __init__(self, jobs:Iterable[Tuple[Job, datetime]] = []):
		self.jobs = {job['JobId']: (job, timestamp) for job, timestamp in jobs}

	def insert(self, jobs:Iterable[Job], timestamp:datetime) -> None:
		for job in jobs:
			if job['JobId'] in self.jobs:
				current = self.jobs[job['JobId']][0]
				self.jobs[job['JobId']] = (Job({**current, **job}), timestamp)
			else:
				self.jobs[job['JobId']] = (job, timestamp)

	def update(self, joblist:'JobList') -> None:
		for job, update_timestamp in joblist.with_timestamp():
			if job['JobId'] in self.jobs:
				current, current_timestamp = self.jobs[job['JobId']]
				# If the entry is newer, prioritise its values
				if update_timestamp >= current_timestamp:
					self.jobs[job['JobId']] = (type(current)({**current, **job}), update_timestamp)
				# if it is older, but has more info, add the info but don't overwrite anything
				elif set(job.items()) - set(current.items()):
					self.jobs[job['JobId']] = (type(current)({**job, **current}), current_timestamp)
			else:
				self.jobs[job['JobId']] = (job, update_timestamp)

	def __iter__(self) -> Iterator[Job]:
		return iter(job for job, _ in self.jobs.values())

	def with_timestamp(self) -> Iterable[Tuple[Job,datetime]]:
		return self.jobs.values()

	def filter(self, op:Callable[[Job],bool]) -> 'JobList':
		return self.__class__(entry for entry in self.jobs.values() if op(entry[0]))

	def get(self, job_id:str, default:T=None) -> Union[Job,T]:
		return self.jobs[job_id][0] if job_id in self.jobs else default

	def job_ids(self) -> Iterable[str]:
		return self.jobs.keys()


def add_jobs_to_set(job_id_set, jobs):
	for job in jobs:
		job_id_set.add(job['JobId'])
		yield job


class State:
	STALE_STATES = {
		'COMPLETED',
		'CANCELLED',
		'FAILED',
		'TIMEOUT'
	}

	def __init__(self, since):
		self.last_update = datetime.now()
		self.jobs = JobList((job, self.last_update) for job in slurm.jobs(since=since, include_completed=True))

	def update(self):
		now = datetime.now()

		# Active jobs (that we need updates on)
		active_jobs = self.jobs.filter(lambda job: 'State' not in job or job['State'] not in self.STALE_STATES)

		# List of seen job ids in this update. Any job in active_jobs that's not also in seen_jobs is not active.
		seen_jobs = set()

		# Add any new scheduled jobs
		active_jobs.insert(add_jobs_to_set(seen_jobs, slurm.scheduled_jobs(since=self.last_update)), now)

		# Query latest status on these jobs
		active_jobs.insert(add_jobs_to_set(seen_jobs, slurm.accounting_jobs(['--jobs', ','.join(active_jobs.job_ids())])), now)

		# Query active jobs, but still limit to only jobs that appeared in our scheduling log.
		active_jobs.insert(add_jobs_to_set(seen_jobs, slurm.current_jobs(['--jobs', ','.join(active_jobs.job_ids())])), now)

		# Remove dead jobs
		active_jobs.insert([
			Job(JobId=job['JobId'], State='CANCELLED')
			for job in active_jobs
			if job['JobId'] not in seen_jobs
		], now)

		self.jobs.update(active_jobs)

		self.last_update = now

		return active_jobs

	def get_job(self, job_id):
		job = self.jobs.get(job_id, Job(JobId=job_id))
		update = slurm.job(job_id)
		if update: job.update(update)
		return job


state = State(since=datetime.now() - timedelta(days=365))


@app.url_type('job')
class JobConverter(URLConverter):
	def to_pattern(self) -> str:
		return r'\d+(?:_\d+)?'

	def to_python(self, val: str) -> Job:
		return state.get_job(val)

	def to_str(self, val: Any) -> str:
		if 'ArrayJobId' in val:
			return '{:d}_{:d}'.format(int(val['ArrayJobId']), int(val['ArrayTaskId']))
		else:
			return '{:d}'.format(int(val['JobId']))


@app.route('/')
def index(request):
	return send_file(os.path.join(os.path.dirname(__file__), 'dashboard.html'))


@app.route('/<str:filename>.html')
def show_static_file(request, filename):
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
		} for name, collection in read_collections().items()
	])


@app.route('/jobs/')
@app.route('/jobs/delta/<str:timestamp>', name='list_jobs_delta')
def list_jobs(request, timestamp=None):
	collections = read_collections()
	state.update()
	if timestamp:
		since = datetime.fromisoformat(timestamp)
	else:
		since = datetime.now() - timedelta(days=365)
	return send_json({
		'timestamp': state.last_update.isoformat(),
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
				'last_update': job_timestamp.isoformat()
			}
			for job, job_timestamp in state.jobs.with_timestamp()
			if job_timestamp > since \
			and job.collection is None or job.collection in collections
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
		return none_throws(self.proc.stdout).fileno()

	def read(self, *args, **kwargs):
		return none_throws(self.proc.stdout).read(*args, **kwargs)

	def close(self):
		none_throws(self.proc.stdout).close()
		self.proc.terminate() # closing otherwise tail won't notice
		self.proc.wait() # wait to prevent zombie


@app.route('/jobs/<job:job>/<any(stdout,stderr):stream>')
def show_stream(request, stream, job):
	mapping = {
		'stdout': 'StdOut',
		'stderr': 'StdErr'
	}

	path = job.get(mapping[stream], None)

	if path is None or not os.path.exists(path):
		return Response('File not found: {}'.format(path), 404)

	return FileResponse(Tailer(path))


def disk_quota():
	lines = subprocess.check_output("quota").decode().splitlines()

	columns = [
		'proj', 
		'size_usage', 'size_quota', 'size_limit', 'size_grace',
		'file_usage', 'file_quota', 'file_limit', 'file_grace',
		'account'
	]

	for line in lines[1:]:
		fields = re.split(r'\s+', line)
		if len(fields) == len(columns):
			yield {
				column: float(value) if re.match(r'^\d+(\.\d+)?$', value) else value
				for column, value in zip(columns, fields)
			}


def slurm_account_balance(account):
	return int(subprocess.check_output(['sbank', 'balance', 'statement', '-u', '-a', account]))


def slurm_balance():
	return [
		{
			'account': account_name,
			'balance': slurm_account_balance(account_name)
		} for account_name in slurm.accounts
	]


def lumi_balance():
	out = []
	for account in slurm.accounts:
		with open(f'/var/lib/project_info/users/{account}/{account}.json') as fh:
			billing = json.load(fh).get('billing', {})
			out += [
				{
					'account': f'{account}-{partition_name[:-len("_hours")]}',
					'balance': partition['alloc'] - partition['used']
				}
				for partition_name, partition in billing.items()
				if partition_name.endswith('_hours')
			]
	return out


def lumi_disk_quota():
	columns = [
		'size_usage', 'size_quota', 'size_limit', 'size_grace',
		'file_usage', 'file_quota', 'file_limit', 'file_grace'
	]
	offsets = {
	 	'users':    1000000000,
		'projappl': 2000000000,
		'scratch':  3000000000,
		'flash':    3000000000,
	}
	partition = 'scratch'
	for account in slurm.accounts:
		gid = none_throws(subprocess.check_output(['getent', 'group', account])).split(b':')[2] # 3rd field
		quota = subprocess.check_output(['lfs', 'quota', '-q', '-p', str(int(gid) + offsets[partition]), f'/{partition}/{account}']).split(b'\n')[1] # second line
		fields = {
			column: ((int(value) * 1024) / (1000 ** 3) if column.startswith('size_') else int(value)) if value != b'-' else None
			for column, value in zip(columns, quota.split())
		}
		yield {'proj': account, **fields}


@app.route('/quota/')
def list_quota(request):
	try:
		return send_json(list(lumi_disk_quota()))
	except:
		return send_json(list(disk_quota()))


@app.route('/balance/')
def list_balance(request):
	try:
		return send_json(lumi_balance())
	except:
		return send_json(slurm_balance())


if __name__ == "__main__":
	main(app)
