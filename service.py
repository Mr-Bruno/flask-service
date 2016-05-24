#!flask/bin/python
import threading
import json
import cStringIO
import zipfile
import gzip
import multiprocessing
import time
from db import transaction, Domain, Information, Referer

from flask import Flask
from flask import request
from flask import jsonify

app = Flask(__name__)

json_data_queue = multiprocessing.Queue()


def find_key(key, json_var):
    """
    :param key: key to search in the json_var variable
    :param json_var: variable with json format
    :return: list with the values for the key found
    """
    my_list = []
    for k, v in json_var.iteritems():
        if k == key:
            my_list.append(v)
        elif isinstance(v, dict):
            find_key(key, v)
            for result in find_key(key, v):
                my_list.append(result)

    return my_list


def value_matches(key, value, my_json):
    """
    :param key: key to search in the json_var variable
    :param value: value to search in the json_var variable
    :param my_json: variable with json format where it searches
    :return: list with they key
    """
    my_list = []
    for k, v in my_json.iteritems():
        if k == key and my_json[key] == value:
            my_list.append(key)
        elif isinstance(v, dict):
            for result in value_matches(key, value, v):
                my_list.append(result)

    return my_list


def file_type(filename):
    """
    :param filename: name of the file to be identified
    :return: string with the type of the file "gz","bz2",etc.
    """

    magic_dict = {
        "\x1f\x8b\x08": "gz",
        "\x42\x5a\x68": "bz2",
        "\x50\x4b\x03\x04": "zip"
    }

    max_len = max(len(x) for x in magic_dict)

    with open(filename) as f:
        file_start = f.read(max_len)
    for magic, filetype in magic_dict.items():
        if file_start.startswith(magic):
            return filetype
    return "no match"

@app.route('/uploads', methods=['POST'])
def uploaded_file():

    # Detecting the file type sent to the service
    f_name = request.files['filedata'].filename
    f_type = file_type(f_name)

    # Decompress it in memory if it is gzip
    if f_type == "gz":
        print "gz"
        gfile = gzip.open(f_name, 'r')
        buf_file = cStringIO.StringIO(gfile.read())

    # Decompress it in memory if it is zip
    elif f_type == "zip":
        zfile = zipfile.ZipFile(f_name, 'r')
        names = zfile.namelist()

        # Pick the first one (in case it creates other side files
        buf_file = cStringIO.StringIO(zfile.open(names[0]).read())

    else:
        buf_file = request.files['filedata']

    json_data_queue.put((buf_file.read(), f_name))

    return "Information processed correctly"

@app.route('/request', methods=['POST'])
def request_info():

    # It should print the web requested
    print request.data

    # history of each referer domain
    with transaction() as session:
        information = session.query(Information).filter(Information.domain_url == request.data).all()

    return jsonify({'entrances': [{'creative_size': entrance.creative_size, 'referer_url': entrance.referer_url} for entrance in information]})


class JsonQueueReader(threading.Thread):
    """
    This thread is responsible of picking the Json Encoded information of one request and introduce it into the db.
    """
    def __init__(self, buf_file, f_name):
        super(JsonQueueReader, self).__init__(name='JsonQueueReader')
        self.buf_file = buf_file
        self.f_name = f_name

    def run(self):
        """
        Main function for thread that will
        """

        # It might be good to create a process rather than a thread per file, and to create a multithreading environment
        # to process the list rather than a single thread for all of it (test under big file environment).

        # One json per line
        my_list = self.buf_file.splitlines()

        for line in my_list:
            # Ignoring if there is an empty entrance in the list
            if line:

                # load json
                json_line = json.loads(line)

                # we are going to look for creative_size, if it does not exist we will get the information from ad_width
                # and ad_height
                creative_size = find_key("creative_size", json_line)
                if not creative_size:
                    value_width = find_key("ad_width", json_line)
                    value_height = find_key("ad_height", json_line)
                    if value_width and value_height:
                        creative_size = [value_width[0] + "x" + value_height[0]]

                # We are going to look for the keys page_url and Referer
                referer = find_key("Referer", json_line)
                url = find_key("page_url", json_line)

                # If the three elements were found, introduce them in the DB.
                if creative_size and referer and url:
                    with transaction() as session:
                        added = False
                        # Check the existence of the entrance before introducing a repetitive one
                        if not session.query(Domain).filter(Domain.url==url[0]).first():
                            session.add(Domain(url[0]))
                            added = True
                        if not session.query(Referer).filter(Referer.url==referer[0]).first():
                            session.add(Referer(referer[0]))
                            added = True

                        session.flush()

                        # If one of the previous tables has a new entry. No need to check of existence in here.
                        if added:
                            session.add(Information(domain_url=url[0], referer_url=referer[0], creative_size=creative_size[0]))

                        elif not session.query(Information).filter(Information.domain_url==url[0])\
                                .filter(Information.referer_url==referer[0])\
                                .filter(Information.creative_size==creative_size[0]).first():
                            session.add(Information(domain_url=url[0], referer_url=referer[0], creative_size=creative_size[0]))

        print "Database updated with information from file: %s" % self.f_name


def worker(json_queue):
    """
    :param json_queue: queue used as a communication that will contain all the strings to process
    :return:
    """

    print "Starting worker process"

    # The worker will never die. It will keep reading from the queue forever so it can process data in the background
    while 1:
        str_io, f_name = json_queue.get()

        result_queue_consumer = JsonQueueReader(str_io, f_name)
        print "kicking thread for %s" % f_name
        result_queue_consumer.start()

        time.sleep(0.1)

if __name__ == '__main__':

    p = multiprocessing.Process(target=worker, args=(json_data_queue,))
    p.start()

    # Starting flask
    app.run()
