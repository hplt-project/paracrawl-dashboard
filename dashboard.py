#!/usr/bin/env python3
import re
import sys
import os
import subprocess
from functools import partial
from pprint import pprint, pformat
from collections import defaultdict
from itertools import chain
from web import Application, Response, main
from htl import _ as h


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
	jobs = slurm.jobs()
	languages = set(chain.from_iterable(collection.languages for collection in collections.values()))
	grid = {
		language: {collection: [] for collection in collections.keys()}
		for language in sorted(languages)
	}
	
	for job in jobs:
		if hasattr(job, 'language') and hasattr(job, 'collection'):
			grid[job.language][job.collection].append(job)

	return Response(h.table(
		h.tr(
			h.td(),
			(h.th(collection) for collection in collections.keys())
		),
		(
			h.tr(
				h.th(language),
				(
					h.td(
						h.ul(
							(
								h.li(
									h.a(job['JOBID'], href="/jobs/{ARRAY_JOB_ID}/{ARRAY_TASK_ID}".format(**job))
								) for job in grid[language][collection]
							)
						)
					) for collection in collections.keys()
				)
			) for language in languages
		)
	))

	html = "<table><tr><th></th>"
	for collection in collections.keys():
		html += "<th>" + collection + "</th>"
	html += "</tr>"
	for language, lang_collections in grid.items():
		html += "<tr><th>" + language + "</th>"
		for collection in collections.keys():
			html += "<td><ul>"
			for job in lang_collections[collection]:
				html += "<li><a href=\"/jobs/{ARRAY_JOB_ID}/{ARRAY_TASK_ID}\">{JOBID}</li>".format(**job)
			html += "</ul></td>"
		html += "</tr>"
	html += "</table>"
	return Response(html)


def tail(filename):
	with open(filename, 'r') as fh:
		return fh.read()


@app.route('/jobs/<int:array_job_id>/<int:array_task_id>')
def task_log(request, array_job_id, array_task_id):
	job = slurm.job('{:d}_{:d}'.format(array_job_id, array_task_id))
	html = h.body(
		h.h2("StdOut"),
		h.pre(tail(job['StdOut'])),
		h.h2("StdErr"),
		h.pre(tail(job['StdErr'])),
		h.h2("Job"),
		h.table(
			(
				h.tr(h.th(key, align="left"), h.td(h.pre(value)))
				for key, value in job.items()
			)
		)
	)
	return Response(html, headers={"Content-Type": "text/html"})


if __name__ == "__main__":        
	main(app)
