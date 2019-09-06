from mysql.connector import connect, Error
from bs4 import BeautifulSoup
import requests
from requests.auth import HTTPDigestAuth
import os
import bs4
import re
import datetime
import time
import json

class FilterDB:

    def runFilter(self):
        global fetchCursor
        global cnx
        try:
            cnx = connect(user=os.environ['MYSQL_USER'], password=os.environ['MYSQL_PASSWORD'],
                          host=os.environ['MYSQL_HOST'],
                          database=os.environ['MYSQL_DATABASE'], port=os.environ['MYSQL_PORT'])
            fetchCursor = cnx.cursor(buffered=True)

            # Enforce UTF-8 for the connection.
            print('Now connecting to....: %s' % cnx.server_host, flush=True)
            fetchCursor.execute('SET NAMES utf8mb4')
            fetchCursor.execute("SET CHARACTER SET utf8mb4")
            fetchCursor.execute("SET character_set_connection=utf8mb4")
            print('Successfully connected to: %s' % cnx.server_host, flush=True)

            #fetchCursor.execute('SELECT _id,body FROM annonce where _id = 762147')

            limit = 1000
            offset = 0
            while True:
                fetchCursor.execute('SELECT _id,body FROM annonce where searchable_body IS NULL OR lastSearchableBody IS NULL OR lastUpdated < lastSearchableBody LIMIT {0},{1}'.format(offset, limit))
                offset = offset + limit
                fetchCount = 0

                startTimer = time.time()
                print('{%s} Filter started' % datetime.datetime.now().strftime('%H:%M:%S'), flush=True)
                for _id, body in fetchCursor:
                    fetchCount = fetchCount + 1
                    print("Inserting searchable_body for id: %s" % _id, flush=True)
                    #print("fetchCursor: %s" % fetchCursor, flush=True)
                    convertToSting = str(body.encode().decode())
                    # print(convertToSting, flush=True)
                    removespaces = re.compile(r'\s+')
                    advertBody = re.sub(removespaces, ' ', convertToSting)
                    soup = BeautifulSoup(advertBody, 'html.parser')
                    searchable_body = self.walker(soup)
                    #print("searchable_body:\n%s", searchable_body)
                    #cvr_reg = re.compile("(cvr.{0,10})(?!21367087)(([0-9] ?){8})")
                    cvr_reg = re.compile("(?i)((cvr|vat).{0,10})(([0-9] ?){8})")
                    cvr_list = cvr_reg.findall(searchable_body)
                    #print("cvr_list:\n%s" % cvr_list, flush=True)
                    cvr = None
                    if cvr_list:
                        cvr = cvr_list[0][2].replace(" ", "")
                    if cvr is not None and cvr != "21367087":
                        # API CALL
                        url = "http://distribution.virk.dk/cvr-permanent/_search"
                        data = {"query": {"term": {"Vrvirksomhed.cvrNummer": cvr}}}
                        data = json.dumps(data)
                        headers = {'Content-type': 'application/json; charset=utf-8', 'user-agent': 'EAAA'}

                        response = requests.post(url=url, auth=(os.environ['API_USERNAME'], os.environ['API_PASSWORD']), data=data, headers=headers)

                        if response.status_code is 200:
                            print("Inserting cvr %s" % cvr)
                            self.insertGenericToDB(key="cvr", value=cvr, condition=_id)

                            company = response.text
                            print("Inserting company json")
                            #print(company)
                            self.insertGenericToDB(key="json", value=company, condition=_id)
                            self.insertToDB(searchable_body=searchable_body, condition=_id)
                    else:
                        self.insertToDB(searchable_body=searchable_body, condition=_id)
                        # print(searchable_body , flush=True)
                elapsed = time.time() - startTimer
                duration = time.strftime('%H:%M:%S', time.gmtime(elapsed))
                print('Took: %s' % duration, flush=True)
                if fetchCount < limit:
                    break;
        except Error as e:
            print(e.args, flush=True)
        finally:
            fetchCursor.close()
            cnx.close()

    def walker(self, node):
        result = ''
        if node.name is None or node.name in ['script', 'noscript', 'iframe', 'img', 'style', 'a', 'input', 'textarea',
                                              'button', 'select', 'option', 'optiongroup', 'fieldset',
                                              'label']:
            return result
        if node.name == 'div' and node.get('id') == 'heart_job_offers':
                return result
        pattern = re.compile("ookie")
        classValue = node.get('class')
        if classValue != None and classValue and pattern.search(classValue[0]):
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

            # Enforce UTF-8 for the connection.
            cursor.execute('SET NAMES utf8mb4')
            cursor.execute("SET CHARACTER SET utf8mb4")
            cursor.execute("SET character_set_connection=utf8mb4")

            searchable_body = searchable_body.replace("\\n", " ").replace('\\t', ' ').replace("'"," ")
            cursor.execute("UPDATE annonce SET searchable_body = '%s', lastSearchableBody = CURRENT_TIMESTAMP() WHERE _id = %d" % (searchable_body, condition))
            connection.commit()
        except Error as e:
            # If there is any case of error - Rollback
            print(e.args, flush=True)
            connection.rollback()
        finally:
            cursor.close()
            connection.close()

    def insertGenericToDB(self, key, value, condition):
        global connection
        global cursor
        try:
            connection = connect(user=os.environ['MYSQL_USER'], password=os.environ['MYSQL_PASSWORD'],
                                 host=os.environ['MYSQL_HOST'],
                                 database=os.environ['MYSQL_DATABASE'], port=os.environ['MYSQL_PORT'])
            cursor = connection.cursor()

            # Enforce UTF-8 for the connection.
            cursor.execute('SET NAMES utf8mb4')
            cursor.execute("SET CHARACTER SET utf8mb4")
            cursor.execute("SET character_set_connection=utf8mb4")
            cursor.execute("UPDATE annonce SET {key} = '{value}' WHERE _id = {condition}".format(key=key, value=value, condition=condition))
            connection.commit()
        except Error as e:
            # If there is any case of error - Rollback
            print(e.args, flush=True)
            connection.rollback()
        finally:
            cursor.close()
            connection.close()
