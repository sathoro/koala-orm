import sys
import logging
import mysql.connector
import time
import copy
import datetime
from mysql.connector import errorcode

class Koala(object):
	def __init__(self, db_user, db_pass, db_name, db_host=False, db_port=False, debug=False):
		# store which columns each table has (as a set)
		self.columns_cache = {}
		# keep track of the queries we execute
		self.queries = []
		# total query execution time
		self.total_time = 0
		# table name -> important model info
		self.table_to_model_map = {}

		self.debug = bool(debug)

		# preset datatypes for particular columns
		self.datatypes = {
			'created_at': 'TIMESTAMP NOT NULL DEFAULT 0',
			'updated_at': 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
			'deleted_at': 'TIMESTAMP NULL DEFAULT NULL'
		}

		conn_details = {
			"user":		db_user,
			"password":	db_pass,
			"host":		db_host or '127.0.0.1',
			"port": 	db_port or '3306'
		}

		try:
			self.conn = mysql.connector.connect(**conn_details)
			self.cursor = self.conn.cursor()
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
				return self.throw("Invalid credentials, verify your username and password then try again")
			else:
				return self.throw(err)

		try:
			self.conn.database = db_name
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_BAD_DB_ERROR:
				self.create_db(db_name)
				self.conn.database = db_name
			else:
				return self.throw(err)

	def info(self):
		str = ""
		for query in self.queries:
			str += "%s\n" % query['q']
		str += '===\nQuery Time: %.00006f' % self.total_time
		return str

	def create_db(self, db_name):
		try:
			query = "CREATE DATABASE `%s` DEFAULT CHARACTER SET 'utf8'" % db_name
			self.do_query(query)
		except mysql.connector.Error as err:
			return self.throw(err)

	def ensure_table_exists(self, table_name, primary_key):
		try:
			query = "CREATE TABLE `%s` (`%s` int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY (`%s`)) ENGINE=InnoDB" % (table_name, primary_key, primary_key)
			self.do_query(query)
		except mysql.connector.Error as err:
			if err.errno != errorcode.ER_TABLE_EXISTS_ERROR:
				return self.throw(err)

	def ensure_column_exists(self, table_name, column_name, datatype=None):
		if table_name in self.columns_cache:
			if column_name in self.columns_cache[table_name]:
				return True
			else:
				try:
					if datatype is None and column_name in self.datatypes:
						datatype = self.datatypes[column_name]
					datatype = datatype or 'VARCHAR(255)'
					query = "ALTER TABLE `%s` ADD `%s` %s" % (table_name, column_name, datatype)
					self.do_query(query)
					self.columns_cache[table_name].add(column_name)
				except mysql.connector.Error as err:
					return self.throw(err)
		else:
			'''
			The table exists so re-cache the columns and make sure
			the column exists as well.
			'''
			self.get_columns(table_name)
			return self.ensure_column_exists(table_name, column_name, datatype)

	def get_columns(self, table_name):
		if table_name in self.columns_cache:
			return self.columns_cache[table_name]
		try:
			query = "SELECT column_name FROM information_schema.COLUMNS WHERE TABLE_NAME = '%s'" % table_name
			self.do_query(query)

			columns = set([x[0] for x in self.cursor])
			self.columns_cache[table_name] = columns
			return columns
		except mysql.connector.Error as err:
			return self.throw(err)

	def insert(self, table_name, data=False, timestamp=False):
		data = data or {}

		if timestamp:
			# NULL on NOT NULL column makes mysql set it to current timestamp
			data['created_at'] = 'NULL'

		try:
			query = "INSERT INTO `%s` (%s) VALUES (%s)" % (table_name, ', '.join(data.keys()), ', '.join(data.values()))
			self.do_query(query)

			self.conn.commit()
			return self.cursor.lastrowid
		except mysql.connector.Error as err:
			return self.throw(err)

	def update(self, table_name, values, where):
		try:
			set_syntax = []
			for column, value in values.iteritems():
				set_syntax.append("`%s` = '%s'" % (column, value))
			set_syntax = ', '.join(set_syntax)

			where_syntax = []
			for column, value in where.iteritems():
				where_syntax.append("`%s` = '%s'" % (column, value))
			where_syntax = ', '.join(where_syntax)

			query = "UPDATE `%s` SET %s WHERE %s" % (table_name, set_syntax, where_syntax)
			self.do_query(query)

			self.conn.commit()
			return self.cursor.rowcount
		except mysql.connector.Error as err:
			return self.throw(err)

	def throw(self, err):
		if not self.debug:
			print err
			raise BaseException
		return False

	def do_query(self, query):
		start = time.clock()
		self.cursor.execute(query)
		elapsed = time.clock() - start
		self.log_query(query, elapsed)
		self.total_time += elapsed

	def log_query(self, query, time):
		self.queries.append({'t': time, 'q': query})

	def populate_list(self, key):
		list = []
		for row in self.cursor:
			if key in row:
				list.append(row[key])
		return list

	def get_time(self):
		return datetime.datetime.today()

class KoalaQuery(object):
	def __init__(self, model_class):
		self._select = set()
		self._set_values = []
		self._where = []
		self._limit = None
		self._offset = None
		self._model_class = model_class
		self._model = model_class()
		self._table = self._model._table
		self._koala = self._model._koala
		self._koala.ensure_table_exists(self._table, 'id' or self._model._primary_key)
		self._soft_delete = self._model._soft_delete
		self._columns = set()

	def select(self, *items):
		self._select.update(items)
		return self

	def format_select(self):
		if len(self._select) > 0:
			return self.comma_seperate(self._select)
		return '*'

	def where(self, *conditions, **kwargs):
		if len(conditions) > 0:
			if isinstance(conditions[0], dict):
				conditions = zip(conditions[0].keys(), conditions[0].values())
			elif not isinstance(conditions[0], list):
				conditions = [conditions]
		else:
			conditions = []

		for column, value in kwargs.items():
			conditions.append([column, value])

		for condition in conditions:
			self._columns.add(self._model.resolve_name(condition[0]))
			if len(condition) == 2:
				key = condition[0]
				value = condition[1]
				if isinstance(condition[1], KoalaModel):
					key = self._model._aliases[key]
					value = condition[1]._pk
				self._where.append("`%s` = '%s'" % (key, value))
			elif len(condition) == 3:
				key = condition[0]
				operator = condition[1]
				value = condition[2]
				if isinstance(condition[2], KoalaModel):
					key = self._model._aliases[key]
					value = condition[2]._pk
				self._where.append("`%s` %s %s" % (key, operator, value))
			else:
				self._koala.throw('Conditions must be either two or three elements!')
		return self

	def format_where(self):
		return ' AND '.join(self._where)

	def comma_seperate(self, list):
		return ', '.join(list)

	def limit(self, limit):
		if not isinstance(limit, int):
			self._koala.throw('Limit must be an integer!')
		self._limit = limit
		return self

	def offset(self, offset):
		if not isinstance(offset, int):
			self._koala.throw('Offset must be an integer!')
		self._offset = offset
		return self

	def get(self):
		if self._soft_delete:
			self = self.where('deleted_at', 'IS', 'NULL')

		for column in self._columns:
			self._koala.ensure_column_exists(self._table, column)

		sql = []
		sql.append("SELECT %s FROM %s" % (self.format_select(), self._table))
		if len(self._where) > 0:
			sql.append("WHERE %s" % self.format_where())
		if self._limit is not None:
			sql.append("LIMIT %s" % self._limit)
			if self._offset is not None:
				sql.append("OFFSET %s" % self._offset)
		return self._populate_models(sql)

	def delete(self, force=False):
		if self._soft_delete:
			if not force:
				return 0

		sql = []
		sql.append("DELETE FROM `%s`" % (self._table))
		if len(self._where) > 0:
			sql.append("WHERE %s" % self.format_where())
		if self._limit is not None:
			sql.append("LIMIT %s" % self._limit)
		self._koala.do_query(' '.join(sql))
		self._koala.conn.commit()
		return self._koala.cursor.rowcount

	def _populate_models(self, sql):
		self._koala.do_query(' '.join(sql))
		results = self._koala.cursor.fetchall()
		columns = self._koala.cursor.column_names
		if len(results) > 1:
			objects = []
			for result in results:
				objects.append(self._make_model(columns, result))
			return objects
		elif len(results) == 1:
			return self._make_model(columns, results[0])
		else:
			return False

	def _make_model(self, columns, results):
		return self._model_class(**dict(zip(columns, results)))

class KoalaModel(object):
	def __init__(self, *args, **kwargs):
		self._koala.ensure_table_exists(self._table, 'id' or self._primary_key)
		self._buffer = {}
		self._pk_name = 'id' or self._primary_key

		'''
		Set the primary key to -1 if this model
		isn't yet in the database. (we can use __init__
		for instantiating objects that may or may not 
		already be saved)
		'''
		if self._pk_name in kwargs.keys():
			self._pk = kwargs[self._pk_name]
		else:
			self._pk = -1

		self._columns = None
		self._aliases = {}

		if self._schema is not None:
			'''
			Maps an alias to the actual column name.
			Example: user -> user_id
			'''
			for key, value in self._schema.items():
				if isinstance(value, dict) and 'alias' in value:
					self._aliases[value['alias']] = key

		'''
		Most importantly, we are saving a reference to this 
		model's class so we can use its class methods
		from anywhere in the script as long as we have the
		table name.
		'''
		self._koala.table_to_model_map[self._table] = {
			'pk': self._pk_name,
			'class': self.__class__,
			'aliases': self._aliases
		}

		'''
		Lock the model (prevent database writes) until
		all the attributes have been set.
		'''
		self._lock = True
		self._skip_buffer = (self._pk != -1)
		for k, v in kwargs.items():
			setattr(self, k, v)

		'''
		Unlock and save model if not buffered. (prevents multiple rapid
		UPDATE statements upon creation)
		'''
		self._lock = self._skip_buffer = False
		if not self._buffered and self._pk == -1:
			self.save()

	def __setattr__(self, name, value):
		object.__setattr__(self, name, value)

		'''
		If the attribute isn't used internally then
		write it to the database or store in buffer.
		Don't allow directly writing the primary key.
		'''
		if not name.startswith('_') and name != self._pk_name:
			# If it is an alias, get the actualy column name first...
			name = self._aliases[name] if name in self._aliases else name
			if self._buffered or self._lock:
				if not self._skip_buffer:
					self._buffer[name] = value
			else:
				self.store({name: value})

	def __getattr__(self, name):
		foreign_field = None
		if name in self._schema:
			foreign_field = name
		elif name in self._aliases:
			foreign_field = self._aliases[name]
		schema = self._schema[foreign_field]

		if foreign_field is not None:
			if 'has_one' in schema:
				related_model = self._koala.table_to_model_map[schema['has_one']]
				return related_model['class'].get(getattr(self, foreign_field))
			elif 'has_many' in schema:
				return True
			elif 'belongs_to' in schema:
				return True
		else:
			if name in self._relations.keys():
				if name in self._koala.table_to_model_map:
					referenced_model = self._koala.table_to_model_map[name]
					_class = self.__class__.__name__.lower()
					foreign_field = _class
					if _class in referenced_model['aliases']:
						foreign_field = referenced_model['aliases'][_class]
					results = referenced_model['class'].where(foreign_field, self.id).get()
				else:
					self._koala.throw("Model with table '%s' not found!" % name)

				if self._relations[name] == 'has_many':
					if isinstance(results, list):
						return results
					else:
						return [results]
				elif self._relations[name] == 'has_one':
					if isinstance(results, list):
						return results[0]
					else:
						return results
				else:
					self._koala.throw("Invalid relation, must be 'has_many' or 'has_one'.")

		self._koala.throw("Property '%s' not found for model %s" % (name, self.__class__.__name__))

	def resolve_name(self, name):
		return self._aliases[name] if name in self._aliases else name

	def get_datatype(self, column):
		if column in self._schema:
			if isinstance(self._schema[column], dict):
				if 'type' in self._schema[column]:
					datatype = self._schema[column]['type']
			else:
				datatype = self._schema[column]

	def save(self):
		# Flush buffer and save everything to the database
		saved = False
		if len(self._buffer) > 0:
			saved = (self.store(self._buffer) > 0)
			self._buffer = {}
		return saved

	def store(self, data):
		if self._columns is None:
			self._columns = self._koala.get_columns(self._table)

		if self._timestamps:
			self.make_column_if_not_exists('created_at')
			self.make_column_if_not_exists('updated_at')
			if self._soft_delete:
				self.make_column_if_not_exists('deleted_at')

		if self._pk == -1:
			self._pk = self._koala.insert(self._table, timestamp=self._timestamps)

		setattr(self, self._pk_name, self._pk)

		for column, value in data.iteritems():
			self.make_column_if_not_exists(column)
			if isinstance(value, KoalaModel):
				data[column] = value._pk

		return self._koala.update(self._table, data, {self._pk_name: self._pk})

	def make_column_if_not_exists(self, column, datatype=None):
		if column not in self._columns:
			datatype = self.get_datatype(column)
			self._koala.ensure_column_exists(self._table, column, datatype)
			self._columns.add(column)
			self._koala.columns_cache[self._table].add(column)

	def delete(self, force=False):
		if self._soft_delete or not force:
			self._buffer['deleted_at'] = datetime.datetime.today()
			return self.save()
		else:
			return KoalaQuery(cls).where('id', pk_value).limit(1).delete(force=force)

	@classmethod
	def first_or_create(cls, **kwargs):
		first = KoalaQuery(cls).where(kwargs).limit(1).get()
		if not first:
			return cls(**kwargs)
		return first

	@classmethod
	def select(cls, *args):
		return KoalaQuery(cls).select(*args)

	@classmethod
	def where(cls, *args, **kwargs):
		return KoalaQuery(cls).where(*args, **kwargs)

	@classmethod
	def get(cls, pk_value):
		return KoalaQuery(cls).where('id', pk_value).limit(1).get()