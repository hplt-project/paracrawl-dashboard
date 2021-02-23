from html import escape as html_escape
from functools import partial


class HTMLElement:
	def __init__(self, element, *children, **attributes):
		self.element = element
		self.children = children
		self.attributes = attributes

	def _to_html(self, root):
		if isinstance(root, HTMLElement):
			yield from root
		elif isinstance(root, str):
			yield html_escape(root)
		elif hasattr(root, '__iter__'):
			for child in root:
				yield from self._to_html(child)
		else:
			yield str(root)

	def __iter__(self):
		yield "<{}".format(self.element)
		for name, value in self.attributes.items():
			yield " {}=\"{}\"".format(name, value)
		yield ">"
		for child in self.children:
			yield from self._to_html(child)
		yield "</{}>".format(self.element)

	def __str__(self):
		return "".join(self)


class HTMLWriter:
	def __getattr__(self, attr):
		return partial(HTMLElement, attr)

_ = HTMLWriter()
