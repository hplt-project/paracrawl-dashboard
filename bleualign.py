#!/usr/bin/env python3
import os
import gzip
from base64 import b64decode
from glob import glob
from web import Application, Response, send_file, send_json, main


indexes = {}

def index_document(filename):
	offsets = []
	with gzip.open(filename, 'rb') as fh:
		read_offset = 0
		while True:
			chunk = fh.read(8192)
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


def get_document_index(filename):
	if filename not in indexes:
		indexes[filename] = index_document(filename)
	return indexes[filename]

	
def get_document(filename, index):
	offsets = get_document_index(filename)
	assert index < len(offsets)
	with gzip.open(filename, 'rb') as fh:
		fh.seek(offsets[index])
		cols = fh.readline().split(b'\t')
		return {
			'url_src': cols[0].decode(),
			'url_trg': cols[1].decode(),
			'text_src': b64decode(cols[2]).decode(),
			'text_trg': b64decode(cols[3]).decode(),
			'align_src': b64decode(cols[4]).decode(),
			'align_trg': b64decode(cols[5]).decode()
		}


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
	return send_json([
		{
			'name': 'Document {} (at {})'.format(n, human_filesize(offset)),
			'link': app.url_for('show_document', filename=filename, index=n)
		} for n, offset in enumerate(get_document_index(filename))
	])


@app.route('/files/<str:filename>/<int:index>')
def show_document(request, filename, index):
	return send_json(get_document(filename, index))


if __name__ == '__main__':
	main(app)
