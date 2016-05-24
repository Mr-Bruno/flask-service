- The service is a flask based application. To set it up and run it, follow these steps:

pip install virtualenv
mkdir test
cd test
virtualenv flask
flask/bin/pip install flask
flask/bin/pip install sqlalchemy

python service.py

- Can be tested with the following command lines:

# This will upload the information within the file
curl -i -F name=test -F filedata=@name_of_the_file http://localhost:5000/uploads

# This will request a url to retrieve the information about
curl -iXPOST -H "Content-Type:text/plain" -X POST -d "url_to_check" http://localhost:5000/request




