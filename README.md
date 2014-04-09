## How is Koala different from existing ORMs?

With only one extra line of code in each of your class declarations, Koala will automatically map each of your objects to your database. You never have to create a table, add columns, or manage relationships. Based on the type of data you are assigning to your objects, Koala will automatically create new columns as necessary with the correct types. For optimization purposes you have the option of including a `koala.json` to specify column data types. Since columns are created dynamically by Koala you can enjoy practically schemaless MySQL.

## Getting Started

Koala is very young but there are a lot of powerful features on their way. Please view the examples below to get started with Koala.

The only dependency Koala has is the `mysql-connector-python` package available through pip:

    sudo pip install mysql-connector-python --allow-external mysql-connector-python
    
To get started just create a dictionary with your MySQL connection details and pass it to Koala:

    db_info = {
      'db_user': 'root', 
      'db_pass': 'root', 
      'db_name': 'test', 
      'db_host': 'localhost', 
      'db_port': '8889'
    }

    koala = Koala(**db_info)
    
## Models

Models are just classes that have their own table in your database and each object you create will have a row in that table.
  
Creating models is simple, just make a class:

    class User(KoalaModel):
    	def __init__(self, name, email):
    		KoalaModel.__init__(self, koala, table='users')
    		self.name = name
    		self.email = email

Now when you call something like `user = User('Bob', 'bob@example')`, Koala will automatically create a `users` table if it doesn't already exist, create the appropriate `name` and `email` columns, and insert the data you just passed the model.

Later if you do `user.name = 'Frank'` the database will update the row associated with that object. If you suddenly need a new attribute, such as `user.gender = 'm'` then that column will be created for you.

## Relations

You can also associate models with other models, known as a relation. Let's create a Post model and make Bob the author:

    class Post(KoalaModel):
    	def __init__(self, user, content):
    		KoalaModel.__init__(self, koala, table='posts')
    		self.user = user
    		self.content = content
    
    user = User('Bob', 'bob@example.com')
    post = Post(user, 'This is a comment by Bob')

Again, Koala will automatically create the `posts` table, add the necessary columns, and populate a row with Bob's user id, and the post content. To specify a foreign key relationship and constraints you can do that very easily through the `koala.json` file.
