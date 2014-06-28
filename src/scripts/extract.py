#!/usr/bin/env/ python2

import logging

ID = ["WARC-TREC-ID", "WARC-Record-ID"]
URI = ["WARC-Target-URI"]

def LoadDocuments(fileNames):
    ID_Store = []
    URI_Store = []
    HTML_Store = []
    for fileName in fileNames:
        try:
            lis = fileName.rfind('/') + 1
            path, name = '', fileName
            if lis > 0:
                path, name = fileName[:lis], fileName[lis:]
            k = 0
            with open(path+name) as f:
                for line in f.readlines():
                    line = line.strip()
                    for ids in ID:
                        flag = ids in line
                        if flag is True:
                            ID_Store.append(line)
                    for uris in URI:
                        flag = uris in line
                        if flag is True:
                            URI_Store.append(line)
                    """
                    html = ""
                    for i in range(len(line)):
                        if line[i] == '<':
                            k = 1
                        else:
                            if k == 0:
                                html += line[i]
                            else:
                                if line[i] == '>':
                                    k = 0
                    HTML_Store.append(html)
                    """

        except Exception, _:
            logging.error("Impossible to parse file %s" % fileName)
    return ID_Store, URI_Store, HTML_Store

def LoadDocumentsFromFile(fileName):
    with open(fileName) as f:
        lines = [line.strip() for line in f.readlines()]
        return LoadDocuments(lines)

if __name__ == '__main__':
    ids, uri, html = LoadDocumentsFromFile('data_full.txt')
    with open('id.txt', 'w') as f:
        for I in ids:
            f.write(I+'\n')
    with open('uri.txt', 'w') as f:
        for U in uri:
            f.write(U+'\n')
    with open('html.txt', 'w') as f:
        for H in html:
            f.write(H+'\n')

