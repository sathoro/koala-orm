import koala
from koala import *

db_info = {
	'db_user': 'root', 
	'db_pass': 'root', 
	'db_name': 'test', 
	'db_host': 'localhost', 
	'db_port': '8889'
}

koala = Koala(**db_info)

class User(KoalaModel):
	def __init__(self, name, email):
		KoalaModel.__init__(self, koala, table='users')
		self.name = name
		self.email = email

class Post(KoalaModel):
	def __init__(self, user, content):
		KoalaModel.__init__(self, koala, table='posts')
		self.user = user
		self.content = content

user = User('Bob', 'bob@example.com')
post = Post(user, 'This is a comment by Bob')

print koala