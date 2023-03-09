#!/usr/bin/env python3
import re
import builtins
from pprint import pprint

VARIABLE_EXPRESSION = r'[a-zA-Z\_][a-zA-Z0-9\_\-]*'

VARIABLE_TUPLE_EXPRESSION = VARIABLE_EXPRESSION + r'(?:,\s*' + VARIABLE_EXPRESSION + ')*'

TEMPLATE_EXPRESSIONS = [
	r'{%\s*(?P<if_statement>if\s+(?P<if_condition>.+?))\s*%}',
	r'{%\s*(?P<elif_statement>elif\s+(?P<elif_condition>.+?))\s*%}',
	r'{%\s*(?P<else_statement>else)\s*%}',
	r'{%\s*(?P<endif_statement>endif)\s*%}',
	r'{%\s*(?P<for_statement>for\s+(?P<for_tuple>' + VARIABLE_TUPLE_EXPRESSION + r')\s+in\s+(?P<for_expression>.+?))\s*%}',
	r'{%\s*(?P<endfor_statement>endfor)\s*%}',
	r'{{\s*(?P<expression>.+?)\s*}}'
]

def indent(text):
	return '\n'.join('\t' + line for line in text.splitlines())

class Node:
	def __init__(self):
		self.children = []

	def __repr__(self):
		return '<{} [\n{}\n]>'.format(self.__class__.__name__, indent('\n'.join(repr(child) for child in self.children)))

	def __str__(self):
		return "".join(str(child) for child in self.children)

	def append(self, child):
		self.children.append(child)

	def format(self, **kwargs):
		return "".join([child.format(**kwargs) for child in self.children])


class Raw:
	def __init__(self, data):
		self.data = data

	def __repr__(self):
		return '<Raw [{}]>'.format(self.data.replace("\n", " "))

	def __str__(self):
		return str(self.data)

	def format(self, **kwargs):
		return str(self.data)


class Expression:
	def __init__(self, expr):
		self.code = expr
		self.expr = builtins.compile(expr, '<template>', 'eval')

	def __repr__(self):
		return '<Expression [{}]>'.format(self.code.replace("\n", " "))

	def __str__(self):
		return '{{{{ {} }}}}'.format(self.code)

	def format(self, **kwargs):
		return str(eval(self.expr, globals(), kwargs))


class IfNode(Node):
	def __init__(self):
		self.cases = []

	def __repr__(self):
		return '<{} [\n{}\n]>'.format(self.__class__.__name__, indent('\n'.join(
			'If {}: {!r}'.format(expr, node) for _, expr, node in self.cases
			)))

	def __str__(self):
		return r'{}{{% endif %}}'.format(''.join(
			r'{{% {op} {expr} %}}{node!s}'.format(
				op=('if' if n == 0 else
					'elif' if n != len(self.cases) -1 or condition_expr == 'True' else
					'elif'),
				expr=condition_expr,
				node=node) for n, (_, condition_expr, node) in enumerate(self.cases)
		))

	def add_case(self, condition_expr):
		condition = builtins.compile(condition_expr, '<template>', 'eval')
		self.cases.append((condition, condition_expr, Node()))

	def append(self, child):
		self.cases[-1][-1].append(child)

	def format(self, **kwargs):
		for condition, _, node in self.cases:
			if eval(condition, globals(), kwargs):
				return node.format(**kwargs)
		return ""


class ForNode(Node):
	def __init__(self, loop_expr, loop_variables):
		super().__init__()
		variable_expr = '{{ {} }}'.format(
			', '.join([
				"'{0}': {0}".format(variable.strip())
				for variable in loop_variables
			])
		)
		inline_loop_expr = '({} for {} in {})'.format(variable_expr, ','.join(loop_variables), loop_expr)
		self.expr = (loop_variables, loop_expr)
		print(inline_loop_expr)
		self.loop = builtins.compile(inline_loop_expr, '<template>', 'eval')

	def __repr__(self):
		return '<{} for {}: [\n{}\n]>'.format(self.__class__.__name__, self.expr, indent('\n'.join(repr(child) for child in self.children)))

	def __str__(self):
		return r'{{% for {vars} in {expr} %}}{node}{{% endfor %}}'.format(
			vars=','.join(self.expr[0]),
			expr=self.expr[1],
			node=super().__str__())

	def format(self, **kwargs):
		return "".join(
			super(ForNode, self).format(**{**kwargs, **local})
			for local in eval(self.loop, globals(), kwargs)
		)


def compile(root, text):
	stack = [root]
	pos = 0

	try:
		while True:
			match = re.search('|'.join(TEMPLATE_EXPRESSIONS), text[pos:])
			if not match:
				break

			stack[-1].append(Raw(text[pos:pos+match.start()]))
			pos = pos + match.end()

			if match.group('if_statement'):
				node = IfNode()
				node.add_case(match.group('if_condition'))
				stack.append(node)

			elif match.group('elif_statement'):
				assert isinstance(stack[-1], IfNode), 'elif without if'
				stack[-1].add_case(match.group('elif_condition'))

			elif match.group('else_statement'):
				assert isinstance(stack[-1], IfNode), 'else without if'
				stack[-1].add_case("True")

			elif match.group('endif_statement'):
				assert isinstance(stack[-1], IfNode), 'endif without if'
				node = stack.pop()
				stack[-1].append(node)

			elif match.group('for_statement'):
				node = ForNode(match.group('for_expression'), match.group('for_tuple').split(','))
				stack.append(node)

			elif match.group('endfor_statement'):
				assert isinstance(stack[-1], ForNode), 'endfor without for'
				node = stack.pop()
				stack[-1].append(node)

			elif match.group('expression'):
				stack[-1].append(Expression(match.group('expression')))

			else:
				assert False

		stack[-1].append(Raw(text[pos:]))
	except:
		pprint(stack)
		raise

	assert len(stack) == 1
	return stack[-1]


class Template(Node):
	def __init__(self, template, **kwargs):
		super().__init__()
		self.args = kwargs
		compile(self, template)

	def format(self, **kwargs) -> str:
		return super().format(**(self.args | kwargs))


if __name__ == '__main__':
	TEMPLATE=r'''
	Hello {{ beep }} exworld
	{% for key, value in lut.items() %}
		{{ key }}: 
		{% if len(value) % 2 == 0 %}
			{% for letter in value %}
				{{ letter }},
			{% endfor %}
		{% else %}
			Booh!
		{% endif %}
	{% endfor %}
	end world
	'''

	data = {
		'a': 'abc',
		'b': 'abcd',
		'c': 'abcde',
		'd': 'abcdef',
	}

	tpl = Template(TEMPLATE)
	print(repr(tpl))
	print(str(tpl))
	print(tpl.format(lut=data, beep='boop'))

