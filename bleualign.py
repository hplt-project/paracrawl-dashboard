#!/usr/bin/env python3
import os
import gzip
from base64 import b64decode
from glob import glob
from collections import defaultdict
from web import Application, Response, send_file, send_json, main


class lazydict(dict):
	"""Like defaultdict, but passes the key to the default_factory in __missing__()."""
	def __init__(self, default_factory):
		super().__init__()
		self.factory = default_factory

	def __missing__(self, key):
		return self.factory(key)


def index_document(filename):
	offsets = []
	with gzip.open(filename, 'rb') as fh:
		read_offset = 0
		while True:
			chunk = fh.read(32768)
			if len(chunk) == 0:
				break
			pos = 0
			while pos != -1:
				pos = chunk.find(b'\n', pos) + 1
				if pos:
					offsets.append(read_offset + pos)
				else:
					break
			read_offset += len(chunk)

	# If there is a blank line at the end of the file, it's not a valid offset
	if offsets and offsets[-1] == read_offset:
		del offsets[-1]

	return offsets


def index_document_2(filename):
	offsets = []
	with gzip.open(filename, 'rb') as fh:
		pos = 0
		for line in fh:
			pos_src_url = line.find(b'\t')
			pos_trg_url = line.find(b'\t', pos_src_url + 1)
			if pos_src_url != -1 and pos_trg_url != -1:
				offsets.append((pos, line[:pos_src_url].decode(), line[pos_src_url+1:pos_trg_url].decode()))
			pos += len(line)
	return offsets


indexes = lazydict(index_document_2)

def get_aligned_filename(filename):
	pos = filename.find('-bleualign-input.tab.gz')
	if pos == -1:
		return None
	aligned_filename = os.path.join('..', 'aligned', filename[:pos] + '-aligned.gz')
	if not os.path.exists(aligned_filename):
		return None
	return aligned_filename


def index_aligned_document(filename):
	aligned_filename = get_aligned_filename(filename)
	if not aligned_filename:
		return {}
	index = indexes[aligned_filename]
	reverse_index = defaultdict(list)
	for offset, src_url, trg_url in index:
		reverse_index[(src_url, trg_url)].append(offset)
	return reverse_index


def get_aligned_sentences(filename, offsets):
	aligned_filename = get_aligned_filename(filename)
	if not aligned_filename or not offsets:
		return []
	with gzip.open(aligned_filename, 'rb') as fh:
		for offset in offsets:
			fh.seek(offset)
			yield tuple(col.decode().lstrip() for col in fh.readline().rstrip(b'\n').split(b'\t', maxsplit=4))


aligned_indexes = lazydict(index_aligned_document)


def ltrim(items):
	while items and items[0] == "":
		items = items[1:]
	return items


def get_document(filename, index):
	offsets = indexes[filename]
	assert index < len(offsets)
	with gzip.open(filename, 'rb') as fh:
		fh.seek(offsets[index][0])
		cols = fh.readline().split(b'\t')
		return {
			'url_src': cols[0].decode(),
			'url_trg': cols[1].decode(),
			'text_src':  ltrim(b64decode(cols[2]).decode().split('\n')),
			'text_trg':  ltrim(b64decode(cols[3]).decode().split('\n')),
			'align_src': ltrim(b64decode(cols[4]).decode().split('\n')),
			'align_trg': ltrim(b64decode(cols[5]).decode().split('\n'))
		}


def get_document_with_aligned(filename, index):
	doc = get_document(filename, index)
	aligned_offsets = aligned_indexes[filename].get((doc['url_src'], doc['url_trg']), [])
	rows = list(get_aligned_sentences(filename, aligned_offsets))
	if rows:
		_, _, doc['aligned_src'], doc['aligned_trg'], doc['aligned_scores'] = zip(*rows)
	else:
		doc['aligned_src'], doc['aligned_trg'], doc['aligned_scores'] = [], [], []
	return doc


def human_filesize(size):
	for suffix in ['B', 'K', 'M', 'G', 'T']:
		if size < 1000:
			return '{:.1f}{}'.format(size, suffix)
		else:
			size /= 1000


app = Application()


@app.route('/')
def index(request):
	return send_file(os.path.join(os.path.dirname(__file__), 'bleualign.html'))


@app.route('/files/')
def list_files(request):
	return send_json([
		{
			'name': '{} ({})'.format(filename, human_filesize(os.path.getsize(filename))),
			'link': app.url_for('list_documents', filename=filename, index=1)
		} for filename in sorted(glob('*-bleualign-input.tab.gz'))
	])


@app.route('/files/<str:filename>/')
def list_documents(request, filename):
	aligned_index = aligned_indexes[filename]

	return send_json([
		{
			'name': '{}: {} - {}{}'.format(n, src_url, trg_url, ' ({})'.format(len(aligned_index[(src_url,trg_url)])) if (src_url, trg_url) in aligned_index else ''),
			'link': app.url_for('show_document', filename=filename, index=n)
		} for n, (offset, src_url, trg_url) in enumerate(indexes[filename])
	])


@app.route('/files/<str:filename>/<int:index>')
def show_document(request, filename, index):
	return send_json(get_document_with_aligned(filename, index))


if __name__ == '__main__':
	main(app)
