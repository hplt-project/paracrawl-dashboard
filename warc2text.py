#!/usr/bin/env python3
import html
import os
import gzip
from typing import Counter, Generic, NamedTuple, Iterable, Iterator, List, TypeVar
from urllib.parse import urlparse
from itertools import count, islice
from base64 import b64decode


from web import Application, Request, Response, main
from template import Template

ROOT = os.getcwd()

PAGE = 10

app = Application()

template_header = r"""
<meta charset="utf-8">
<style>
	:root { color-scheme: light dark; }
	@media (prefers-color-scheme: dark) {
		a { color: #88f; }
		a:visited { color: #ddf; }
	}
	.domain-list {
		list-style: none;
		padding: 0;
		font-size: 0.8em;
	}
</style>
"""

template_model_index = Template(template_header + r"""
<ul>
	{% for model in models %}
	<li><a href="{{ app.url_for('output_index', model=model) }}">{{ html.escape(model) }}</a></li>
	{% endfor %}
</ul>
""", html=html, app=app)

template_output_index = Template(template_header + r"""
<h1>{{ html.escape(model) }}</h1>
<ol class="language-list">
	{% for lang in langs %}
	<li>
		<a href="{{ app.url_for('language_index', model=model, lang=lang.name)}}">{{ lang.name }}</a> ({{ lang.size }} records; {{ '{:0.2f}%'.format(lang.size * 100 / total) }} of total)
		<ol class="domain-list">
			{% for domain, count in lang.domains.most_common(10) %}
			<li>{{ html.escape(domain) }}: {{ count }}</li>
			{% endfor %}
		</ol>
	</li>
	{% endfor %}
</ol>
""", html=html, app=app)

template_language_index = Template(template_header + r"""
<a href="{{ app.url_for('output_index', model=model) }}">Language list</a>
<h1>{{ html.escape(model) }} / {{ lang }}</h1>
<p>Showing {{ records.pages[page].start + 1 }} to {{ records.pages[page].end }} of {{ len(records.items) }} records.</p>
{% for record in records.pages[page] %}
<article style="border: 1px solid #ccc; margin: 1em; padding: 1em;">
	<h2><a href="{{ app.url_for('record', model=model, lang=lang, index=record.index) }}">{{ record.index + 1 }}: {{ html.escape(record.url) }}</a></h2>
	{% for paragraph in record.text %}
	<p>{{ html.escape(paragraph) }}</p>
	{% endfor %}
</article>
{% endfor %}

<ol>
{% for slice in records.pages %}
<li>
	<a href="{{ app.url_for('language_index', model=model, lang=lang, page=slice.page) }}" class="{% 'current' if slice.page == page else '' %}">
		{{ slice.start + 1 }}&nbsp;&ndash;&nbsp;{{slice.end}}
	</a>
</li>
{% endfor %}
""", html=html, app=app)

template_record = Template(template_header + r"""
<h1>{{ lang }}</h1>
<h2>{{ html.escape(record.url) }}</h2>
<article>
	{% for paragraph in record.text %}
	<p>{{ html.escape(paragraph) }}</p>
	{% endfor %}
</article>
""", html=html, app=app)

def render_template(template, **kwargs) -> Response:
	return Response(template.format(**kwargs), 200, headers={'Content-Type': 'text/html'})


T = TypeVar('T')

class Page(Generic[T]):
	def __init__(self, items: List[T], page:int):
		self.page = page
		self.start = page * PAGE
		self.end = min((page + 1) * PAGE, len(items))
		self.items = items[self.start:self.end]

	def __iter__(self) -> Iterator[T]:
		return iter(self.items)


class Pagination(Generic[T]):
	items: List[T]
	pages: List[Page[T]]
	def __init__(self, items: Iterable[T]):
		self.items = list(items)
		self.pages = [
			Page(self.items, index)
			for index in range(int(len(self.items) / PAGE))
		]


class Language:
	name: str
	domains: Counter
	size: int

	def __init__(self, name:str, domains:Counter):
		self.name = name
		self.domains = domains
		self.size = sum(domains.values())


class Record(NamedTuple):
	index: int
	text: List[str]
	url: str


def domain(url):
    try:
        return urlparse(url).netloc
    except:
        return url


def domains(urls):
    return frozenset(domain(url) for url in urls)


def count_domains(path:str) -> Counter:
	with gzip.open(path) as fh:
		return Counter(domain(line.decode().strip()) for line in fh)


def read_records(model:str, lang:str) -> Iterable[Record]:
	with gzip.open(os.path.join(ROOT, model, lang, 'text.gz')) as fh_text, gzip.open(os.path.join(ROOT, model, lang, 'url.gz')) as fh_url:
		for index, text, url in zip(count(), fh_text, fh_url):
			yield Record(index, b64decode(text.rstrip()).decode().split('\n'), url.rstrip().decode())


@app.route('/')
def model_index(request:Request) -> Response:
	models = [
		item.name
		for item in os.scandir(ROOT)
		if item.is_dir() and not item.name.startswith('.')
	]

	return render_template(template_model_index, models=models)

@app.route('/<str:model>/')
def output_index(request:Request, model:str) -> Response:
	langs = [
		Language(item.name, count_domains(os.path.join(item.path, 'url.gz')))
		for item in os.scandir(os.path.join(ROOT, model))
		if item.is_dir() and not item.name.startswith('.')
	]

	total = sum(lang.size for lang in langs)

	langs.sort(key=lambda lang: lang.size, reverse=True)

	return render_template(template_output_index, langs=langs, total=total, model=model)


@app.route('/<str:model>/<str:lang>/')
@app.route('/<str:model>/<str:lang>/<int:page>/')
def language_index(request:Request, model:str, lang:str, page:int=0) -> Response:
	records = read_records(model, lang)
	return render_template(template_language_index, model=model, lang=lang, page=page, records=Pagination(records))


@app.route('/<str:model>/<str:lang>/records/<int:index>/')
def record(request:Request, model:str, lang:str, index:int) -> Response:
	record = next(iter(islice(read_records(model, lang), index, index + 1)))
	return render_template(template_record, model=model, lang=lang, record=record)

if __name__ == '__main__':
	main(app)