from mysql.connector import connect, Error
import os
from bs4 import BeautifulSoup
import bs4
import re
import datetime
import time
import sys


class FilterDB:
    def __init__(self):
        pass

    def runFilter(self):
        try:
            cnx = connect(user=os.environ['MYSQL_USER'], password=os.environ['MYSQL_PASSWORD'],
                          host=os.environ['MYSQL_HOST'],
                          database=os.environ['MYSQL_DATABASE'], port=os.environ['MYSQL_PORT'])
            fetchCursor = cnx.cursor()
            print('Now connecting to....: %s' % cnx.server_host)
            fetchCursor.execute('select _id,body  from annonce')
            print('Successfully connected to: %s' % cnx.server_host)
            startTimer = time.time()
            print('{%s} Filter started' % datetime.datetime.now().strftime('%H:%M:%S'))
            for _id, body in fetchCursor:
                convertToSting = str(body)
                removespaces = re.compile(r'\s+')
                advertBody = re.sub(removespaces, ' ', convertToSting)
                soup = BeautifulSoup(advertBody, 'html.parser')
                searchable_body = self.walker(soup)
                self.insertToDB(searchable_body=searchable_body, condition=_id)
                sys.stdout.flush()
                #print(searchable_body )
            elapsed = time.time() - startTimer
            duration = time.strftime('%H:%M:%S', time.gmtime(elapsed))
            print('Took: %s' % duration)
        except Error as e:
            print(e.args)
        finally:
            fetchCursor.close()
            cnx.close()

    def walker(self, node):
        result = ''
        if node.name is not None and node.name not in ['script', 'iframe', 'img', 'style', 'a', 'input', 'textarea',
                                                       'button', 'selecr', 'option', 'optiongroup', 'fieldset',
                                                       'label']:
            for child in node.children:
                if type(child) is bs4.element.NavigableString:
                    cookieText = 'Websitet anvender cookies til at huske dine indstillinger, statistik og'
                    if cookieText not in child.string:
                        result += child.string
                else:
                    result += self.walker(child)

        return result

    def insertToDB(self, searchable_body, condition):
        try:
            connection = connect(user=os.environ['MYSQL_USER'], password=os.environ['MYSQL_PASSWORD'],
                                 host=os.environ['MYSQL_HOST'],
                                 database=os.environ['MYSQL_DATABASE'], port=os.environ['MYSQL_PORT'])
            cursor = connection.cursor()
            print('{%s}: Inserting searchable_body with id: %d ' % (datetime.datetime.now().strftime('%H:%M:%S'), condition))
            searchable_body = searchable_body.replace("\\n", " ").replace('\\t', ' ').replace("'", " ")
            cursor.execute("UPDATE annonce SET searchable_body = '%s' WHERE _id = %d" % (searchable_body, condition))
            connection.commit()
        except Error as e:
            # If there is any case of error - Rollback
            print(e.args)
            connection.rollback()
        finally:
            cursor.close()
            connection.close()
