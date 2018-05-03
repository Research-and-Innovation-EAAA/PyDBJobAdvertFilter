from mysql.connector import connect, ProgrammingError, Error
import os
from bs4 import BeautifulSoup
import bs4
import regex


class FilterDB:
    def __init__(self):
        pass

    def run(self):
        cnx = connect(user=os.environ['MYSQL_USER'], password=os.environ['MYSQL_PASSWORD'],
                      host=os.environ['MYSQL_HOST'],
                      database=os.environ['MYSQL_DATABASE'], port=os.environ['MYSQL_PORT'])
        fetchCursor = cnx.cursor()
        print('works fine')
        sqlSelectFile = open('sql/SelectBody.sql').read()
        fetchCursor.execute(sqlSelectFile)
        for bodies in fetchCursor:
            convert = str(bodies)
            removespaces = regex.compile(r'\s+')
            advertBody = regex.sub(removespaces, ' ', convert)
            soup = BeautifulSoup(convert, 'lxml')
            # removeScript = soup.script.decompose()
            # soup.script.decompose()
            # soup.p.decompose()
            searchable_body = self.walker(soup)
            print(searchable_body)
            # print(soup)

    def walker(self, node):
        result = ''
        if node.name is not None and node.name not in ['script', 'iframe', 'img', 'style', 'a']:
            for child in node.children:
                if type(child) is bs4.element.NavigableString:
                    cookieText = 'Websitet anvender cookies til at huske dine indstillinger, statistik og'
                    if cookieText not in child.string:

                        result += child.string
                else:
                    # print(child.name)
                     result += self.walker(child)

        return result
