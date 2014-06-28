#!/usr/bin/env/ python2

import logging, os

ID = ["WARC-TREC-ID", "WARC-Record-ID"]
URI = ["WARC-Target-URI"]
address =  "/mnt/cw12/cw-data"

def LoadDocuments(fileNames):
    ID_Store = []
    URI_Store = []
    HTML_Store = []
    for fileName in fileNames:
        print "Parsing file", fileName
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

def LoadDocumentsFromFile():
    filename = []
    for folder in os.listdir(address):
        for subfolder in os.listdir(address+"/"+folder):
            for file in os.listdir(address+"/"+folder+"/"+subfolder):
                filename.append(address+"/"+folder+"/"+subfolder+"/"+file)
    return LoadDocuments(filename)

if __name__ == '__main__':
    ids, uri, html = LoadDocumentsFromFile()
    with open('id.txt', 'w') as f:
        for I in ids:
            f.write(I+'\n')
    with open('uri.txt', 'w') as f:
        for U in uri:
            f.write(U+'\n')
    with open('html.txt', 'w') as f:
        for H in html:
            f.write(H+'\n')

