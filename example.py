import koala
from koala import *
import copy
import json

db_info = {
	'db_user': 'root', 
	'db_pass': 'root', 
	'db_name': 'test', 
	'db_host': 'localhost', 
	'db_port': '8889'
}

koala = Koala(**db_info)

class BaseModel(KoalaModel):
	_koala = koala
	_buffered = False
	_timestamps = True
	_soft_delete = True

	def __str__(self):
		str = ""
		for key, value in self.__dict__.items():
			if not key.startswith('_'):
				str += "%s: %s\n" % (key, value)
		return str

class User(BaseModel):
	_table = 'users'
	_schema = {
		'name': 'varchar(255)',
		'email': 'varchar(255)',
		'role_id': {
			'alias': 'role',
			'type': 'int(11)',
			'has_one': 'roles'
		}
	}

	def giveRole(self, role):
		self.role = Role.first_or_create(name=role)

class Role(BaseModel):
	_table = 'roles'
	_schema = {
		'name': 'varchar(20)'
	}
	_relations = {
		'users': 'has_many'
	}

admin = User.first_or_create(name='Tom', email='frank@example.com').giveRole('Admin')
vendor = User.first_or_create(name='Frank', email='frank@example.com').giveRole('Vendor')

print '\n', koala.info()