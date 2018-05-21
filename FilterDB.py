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
            if type(child) is bs4.element.NavigableString:
                searchTexts = ['(dine?|us{es?|ing]|several|anv.{0,8}}nder|ikke|disse|bort|(b|B)rowser(.){0,50}|set|websted| en|til| de|(S|s)ome|will|synkronisere|slette(r|s)?( du)?|lger?|rence|nvend(er|es|te)?|bru(g|k)er?|(u|U)ses?|a(f|v)|vi|(A|a)ccept.{0,10}?|tter|bout|(o|O)m|to|vores|bruges?|elle|of) (c|C)ookies?',
                               '(c|C)ookies? (p|enable|(b|B)rowser(.){0,50}|on|help|hj|are|er|in|bruge|that|contain|anvende|fra|(p|P)(olicy|olitik)|will|.{0,10}(p|P)riva(t|cy))',
                               '(j|J)ava(s|S)cript (enable|support|in|is|)',
                               '(ktiver|with|nable) (j|J)ava(s|S)cript']
                matchTexts = ['[ \t\n]*(c|C)ookies?[ \.\?\n]*']
                found = False
                for filterExp in searchTexts:
                    found = found or (re.search(filterExp, child.string) != None)
                for filterExp in matchTexts:
                    found = found or (re.match(filterExp, child.string) != None)
                if not found:
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
