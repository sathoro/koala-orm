import sys
import logging
import mysql.connector
import time
from mysql.connector import errorcode

class Koala(object):
	def __init__(self, db_user, db_pass, db_name, db_host=False, db_port=False):
		conn_details = {
			"user": 	db_user,
			"password": db_pass,
			"host": 	db_host or '127.0.0.1',
			"port": 	db_port or '3306'
		}

		try:
			self.conn = mysql.connector.connect(**conn_details)
			self.cursor = self.conn.cursor()
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
				throw("Invalid credentials, verify your username and password then try again")
			else:
				throw(err)

		try:
			self.conn.database = db_name
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_BAD_DB_ERROR:
				self.create_db(db_name)
				self.conn.database = db_name
			else:
				throw(err)

		self.queries = []
		self.total_time = 0

	def __call__(self):
		return self

	def __str__(self):
		return '===\nTime Elapsed: %.00006f' % self.total_time

	def create_db(self, db_name):
		try:
			query = "CREATE DATABASE %s DEFAULT CHARACTER SET 'utf8'" % db_name
			self.do_query(query)
		except mysql.connector.Error as err:
			throw(err)

	def ensure_table_exists(self, table_name, primary_key):
		try:
			query = "CREATE TABLE %s (%s int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY (`%s`)) ENGINE=InnoDB" % (table_name, primary_key, primary_key)
			self.do_query(query)
		except mysql.connector.Error as err:
			if err.errno != errorcode.ER_TABLE_EXISTS_ERROR:
				throw(err)

	def ensure_column_exists(self, table_name, column_name):
		try:
			query = "SELECT * FROM information_schema.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s'" % (table_name, column_name)
			self.do_query(query)

			if len(self.cursor.fetchall()) == 0:
				query = "ALTER TABLE %s ADD %s VARCHAR(255) NOT NULL" % (table_name, column_name)
				self.do_query(query)
		except mysql.connector.Error as err:
			throw(err)

	def get_columns(self, table_name):
		try:
			query = "SELECT column_name FROM information_schema.COLUMNS WHERE TABLE_NAME = '%s'" % table_name
			self.do_query(query)

			return [x[0] for x in self.cursor]
		except mysql.connector.Error as err:
			throw(err)

	def insert(self, table_name, data=None):
		if data is None:
			data = {}

		try:
			query = "INSERT INTO %s (%s) VALUES (%s)" % (table_name, ', '.join(data.keys()), ', '.join(data.values()))
			self.do_query(query)

			self.conn.commit()
			return self.cursor.lastrowid
		except mysql.connector.Error as err:
			throw(err)

	def update(self, table_name, values, where):
		try:
			set_syntax = []
			for column, value in values.iteritems():
				set_syntax.append("%s = '%s'" % (column, value))
			set_syntax = ', '.join(set_syntax)

			where_syntax = []
			for column, value in where.iteritems():
				where_syntax.append("%s = '%s'" % (column, value))
			where_syntax = ', '.join(where_syntax)

			query = "UPDATE %s SET %s WHERE %s" % (table_name, set_syntax, where_syntax)
			self.do_query(query)

			self.conn.commit()
			return self.cursor.lastrowid
		except mysql.connector.Error as err:
			throw(err)

	def throw(err):
		print err
		raise

	def do_query(self, query):
		start = time.clock()
		self.cursor.execute(query)
		elapsed = time.clock() - start
		self.log_query(query, elapsed)
		self.total_time += elapsed

	def log_query(self, query, time):
		print "Query: %s in %.06f seconds" % (query, time)
		self.queries.append({'t': time, 'q': query})

	def populate_list(self, key):
		list = []
		for row in self.cursor:
			print row
			if key in row:
				list.append(row[key])
		return list

class KoalaModel(object):
	def __init__(self, koala, table, primary_key='id', buffered=False):
		self._lock = False
		self._koala = koala
		self._koala.ensure_table_exists(table, primary_key)
		self._buffered = buffered
		self._pk = -1
		self._table = table
		self._pk_name = primary_key
		self._columns = self._koala.get_columns(table)

	def __setattr__(self, name, value):
		object.__setattr__(self, name, value)

		if name not in ['_lock', '_koala'] and not self._lock and not self._buffered and name[0] != '_':
			if name not in self._columns:
				self._koala.ensure_column_exists(self._table, name)
				self._columns.append(name)
			self.store(name, value)

	def store(self, column, value):
		if self._pk == -1:
			self._pk = self._koala.insert(self._table)
		if isinstance(value, KoalaModel):
			value = value._pk
		self._koala.update(self._table, {column: value}, {self._pk_name: self._pk})