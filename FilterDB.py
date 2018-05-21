from mysql.connector import connect, Error
from bs4 import BeautifulSoup
import os
import bs4
import re
import datetime
import time
import sys


class FilterDB:

    def runFilter(self):
        try:
            cnx = connect(user=os.environ['MYSQL_USER'], password=os.environ['MYSQL_PASSWORD'],
                          host=os.environ['MYSQL_HOST'],
                          database=os.environ['MYSQL_DATABASE'], port=os.environ['MYSQL_PORT'])
            fetchCursor = cnx.cursor()
            print('Now connecting to....: %s' % cnx.server_host)
            fetchCursor.execute('SELECT _id,body FROM annonce where searchable_body IS NULL')
            print('Successfully connected to: %s' % cnx.server_host)
            startTimer = time.time()
            print('{%s} Filter started' % datetime.datetime.now().strftime('%H:%M:%S'))
            for _id, body in fetchCursor:
                #print("_id: %s" % _id)
                #print("fetchCursor: %s" % fetchCursor)
                convertToSting = str(body)
                removespaces = re.compile(r'\s+')
                advertBody = re.sub(removespaces, ' ', convertToSting)
                soup = BeautifulSoup(advertBody, 'html.parser')
                searchable_body = self.walker(soup)
                self.insertToDB(searchable_body=searchable_body, condition=_id)
                sys.stdout.flush()
                # print(searchable_body )
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
        if node.name is None or node.name in ['script', 'iframe', 'img', 'style', 'a', 'input', 'textarea',
                                              'button', 'select', 'option', 'optiongroup', 'fieldset',
                                              'label']:
            return result
        if node.name == 'div' and node.get('id') == 'heart_job_offers':
                return result
        for child in node.children:
            filterTexts = ['Websitet anvender cookies til at huske dine indstillinger, statistik og',
                           'Aktiver JavaScript for at',
                           'need a browser with JavaScript support',
                           'Enable JavaScript in your browser',
                           'JavaScript is currently disabled',
                           'JavaScript is turned',
                           'JavaScript enabled',
                           'bruger cookies',
                           'brug af cookies',
                           'brugen af cookies',
                           'anvendelse af cookies',
                           'accepterer cookies',
                           'anvender cookies',
                           'use cookies',
                           'cookies fra',
                           'uses cookies',
                           'brug af cookies',
                           'JavaScript enabled',
                           'use of cookies']
            if type(child) is bs4.element.NavigableString:
                found = False
                for filterText in filterTexts:
                    if filterText in child.string:
                        found = True
                        break
                if not found:
                    result += child.string
            else:
                #print("child: %s" % child)
                result += self.walker(child)

        return result

    def insertToDB(self, searchable_body, condition):
        try:
            connection = connect(user=os.environ['MYSQL_USER'], password=os.environ['MYSQL_PASSWORD'],
                                 host=os.environ['MYSQL_HOST'],
                                 database=os.environ['MYSQL_DATABASE'], port=os.environ['MYSQL_PORT'])
            cursor = connection.cursor()
            print('{%s}: Inserting searchable_body with id: %d ' % (
                datetime.datetime.now().strftime('%H:%M:%S'), condition))
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
