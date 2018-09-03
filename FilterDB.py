from mysql.connector import connect, Error
from bs4 import BeautifulSoup
import os
import bs4
import re
import datetime
import time

class FilterDB:

    def runFilter(self):
        global fetchCursor
        global cnx
        try:
            cnx = connect(user=os.environ['MYSQL_USER'], password=os.environ['MYSQL_PASSWORD'],
                          host=os.environ['MYSQL_HOST'],
                          database=os.environ['MYSQL_DATABASE'], port=os.environ['MYSQL_PORT'])
            fetchCursor = cnx.cursor()
            print('Now connecting to....: %s' % cnx.server_host, flush=True)
            #fetchCursor.execute('SELECT _id,body FROM annonce where _id=290')
            fetchCursor.execute('SELECT _id,body FROM annonce where searchable_body IS NULL OR lastSearchableBody IS NULL OR lastUpdated < lastSearchableBody')
            print('Successfully connected to: %s' % cnx.server_host, flush=True)
            startTimer = time.time()
            print('{%s} Filter started' % datetime.datetime.now().strftime('%H:%M:%S'), flush=True)
            for _id, body in fetchCursor:
                print("Inserting searchable_body for id: %s" % _id, flush=True)
                #print("fetchCursor: %s" % fetchCursor, flush=True)
                convertToSting = str(body.decode())
                # print(convertToSting, flush=True)
                removespaces = re.compile(r'\s+')
                advertBody = re.sub(removespaces, ' ', convertToSting)
                soup = BeautifulSoup(advertBody, 'html.parser')
                searchable_body = self.walker(soup)
                self.insertToDB(searchable_body=searchable_body, condition=_id)
                # print(searchable_body , flush=True)
            elapsed = time.time() - startTimer
            duration = time.strftime('%H:%M:%S', time.gmtime(elapsed))
            print('Took: %s' % duration, flush=True)
        except Error as e:
            print(e.args, flush=True)
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
                searchTexts = ['(c|C)ookies?',
                               '(j|J)ava(s|S)cript (enable|support|in|is|)',
                               '(ktiver|with|nable) (j|J)ava(s|S)cript']
                replaceTexts = [("kort(.{1,80})C","kort\1Ckort"),
                                ("C(.{1,80})kort","Ckort\1kort")]

                found = False
                for filterExp in searchTexts:
                    found = found or (re.search(filterExp, child.string) != None)
                if not found:
                    childString = child.string
                    for replaceTuple in replaceTexts:
                        childString = re.sub(replaceTuple[0],replaceTuple[1],childString);
                    result += childString
            else:
                result += self.walker(child)

        return result

    def insertToDB(self, searchable_body, condition):
        global connection
        global cursor
        try:
            connection = connect(user=os.environ['MYSQL_USER'], password=os.environ['MYSQL_PASSWORD'],
                                 host=os.environ['MYSQL_HOST'],
                                 database=os.environ['MYSQL_DATABASE'], port=os.environ['MYSQL_PORT'])
            cursor = connection.cursor()
            searchable_body = searchable_body.replace("\\n", " ").replace('\\t', ' ').replace("'", " ")
            cursor.execute("UPDATE annonce SET searchable_body = '%s', lastSearchableBody = CURRENT_TIMESTAMP() WHERE _id = %d" % (searchable_body, condition))
            connection.commit()
        except Error as e:
            # If there is any case of error - Rollback
            print(e.args, flush=True)
            connection.rollback()
        finally:
            cursor.close()
            connection.close()
