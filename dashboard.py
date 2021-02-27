#!/usr/bin/env python3
import re
import sys
import os
import subprocess
from web import Application, Response, main, send_file, send_json


class Job(dict):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		for name_prop in 'NAME', 'JobName':
			if name_prop in self and self[name_prop].count('-') >= 2:
				self.step, self.language, self.collection = self[name_prop].split('-', maxsplit=2)
				break


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
	def jobs(self):
		output = subprocess.check_output(['squeue',
			'--user', os.getenv('USER'),
			'--format', '%A|%i|%K|%F|%C|%b|%j|%P|%r|%u|%y|%T|%M|%b|%N'])
		lines = output.decode().splitlines()
		headers = lines[0].strip().split('|')
		return [Job(dict(zip(headers, line.strip().split('|')))) for line in lines[1:]]

	def job(self, job_id):
		output = subprocess.check_output(['scontrol', '--details', 'show', 'job', job_id])
		job = dict()
		for line in output.decode().splitlines():
			for match in re.finditer(r'\b(?P<key>[A-Z][A-Za-z_\/:]+)=(?P<value>[^\s]+)', line):
				if match.group('key') in {'Command'}:
					job[match.group('key')] = line[match.end('key') + 1:]
					break
				else:
					job[match.group('key')] = match.group('value')
		return Job(job)

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


@app.route('/')
def index(request):
	return send_file(os.path.join(os.path.dirname(__file__), 'dashboard.html'))


@app.route('/collections/')
def list_collections(request):
	return send_json([
		{
			'name': name,
			'languages': collection.languages
		} for name, collection in collections.items()
	])


@app.route('/jobs/')
def list_jobs(request):
	return send_json([
		{
			'id': job['JOBID'],
			'step': job.step,
			'language': job.language,
			'collection': job.collection,
			'slurm': job, # the dict data
			'link': app.url_for('show_job', array_job_id=int(job['ARRAY_JOB_ID']), array_task_id=int(job['ARRAY_TASK_ID']))
			        if 'ARRAY_TASK_ID' in job else None
		} for job in slurm.jobs() if hasattr(job, 'language') and hasattr(job, 'collection')
	])


def tail(filename):
	if os.path.exists(filename):
		with open(filename, 'r') as fh:
			return fh.read()


@app.route('/jobs/<int:array_job_id>/<int:array_task_id>')
def show_job(request, array_job_id, array_task_id):
	job = slurm.job('{:d}_{:d}'.format(array_job_id, array_task_id))
	return send_json({
		'slurm': job,
		'stdout': tail(job['StdOut']),
		'stderr': tail(job['StdErr'])
	})


if __name__ == "__main__":        
	main(app)
