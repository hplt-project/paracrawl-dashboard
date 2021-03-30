#!/usr/bin/env python3
from web import Response, Application, send_json, main
from template import Template

app = Application()


def send_html(template, **kwargs):
	return Response(template.format(**kwargs), headers={'Content-Type': 'text/html'})


@app.route('/test/<str:a>/<str:b>')
@app.route('/test/<str:a>')
def test_route_1(request, a, b=None):
	return send_json({'a': a, 'b': b})


@app.route('/test-int/<int:a>')
def test_int_route(request, a):
	return send_json({'a': a, 'type': type(a).__name__})


test_index_tpl = Template(r'''
<h1>Tests</h1>
<ul>
	<li><a href="{{ app.url_for("test_route_1", a=4) }}">test_route_1(a=4)</a></li>
	<li><a href="{{ app.url_for("test_route_1", a=4, b=None) }}">test_route_1(a=4, b=None)</a></li>
	<li><a href="{{ app.url_for("test_route_1", a=4, b=6) }}">test_route_1(a=4, b=6)</a></li>
	<li><a href="{{ app.url_for("test_route_1", a=4, b=6, c=8) }}">test_route_1(a=4, b=6, c=8)</a></li>
	<li><a href="{{ app.url_for("test_int_route", a=4) }}">test_int_route(a=4)</a></li>
	<li><a href="{{ app.url_for("test_int_route", a="4") }}">test_int_route(a="4")</a></li>
</ul>
''')

@app.route('/')
def test_index(request):
	return send_html(test_index_tpl, app=app)

main(app)
