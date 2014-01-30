#!/usr/bin/env python
__author__ = 'monk-ee'

"""This module .
probably need to stream io this
"""
from pyes import *
import csv
import logging
import yaml



#print conn.refresh()
class Sump:
    headers_nest = {}
    conn = ""
    config = ""

    def __init__(self):
        try:
            configStr = open('config.yml', 'r')
            self.config = yaml.load(configStr)
        except Exception as error:
            #we are done
            print "Unexpected error:" + str(error)
            exit("Failed Configuration")
        logging.basicConfig(filename=self.config['general']['logfile'],level=logging.INFO)
        try:
            self.conn = ES(self.config['servers'], timeout=self.config['es']['timeout'])
        except:
            #done again
            exit("Failed to connect to ElasticSearch")

    def search(self):
        q = MatchAllQuery()
        result = self.conn.search(query=q, indices=[self.config['es']['index']])
        for r in result:
            #print r this will probably need to be a buffer
            self.parseRow(r)

    def parseRow(self,row):
        #step 1 flatten the json to a flat dict
        flatdict = self.flatten(row)
        #step 2 grab all the keys for the header row
        csvrow = []
        header = []
        #build a nest of available header types - hashed for obvious reasons
        for key in flatdict.keys():
            header.append(key)
        #hash it
        header_hash = hash(frozenset(header))
        #now we will have to check if that header exists
        try:
            y = self.headers_nest[header_hash]
        except:
            self.headers_nest.update({header_hash: header})
            #write to the appropriate file here
            self.csvme(header_hash,header)
            print header_hash
        #output values into the appropriate nest
        for value in flatdict.values():
            try:
                #this situation: UnicodeEncodeError: 'ascii' codec can't encode characters in position 0-11: ordinal not in range(128)
                value.encode('ascii', 'ignore')
            except:
                pass
            csvrow.append(value)
        self.csvme(header_hash,csvrow)

    def csvme(self,header_hash,row_list):
        writer = csv.writer(open('csv/'+str(header_hash)+'.csv', 'a') , delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        try:
            writer.writerow(row_list)
        except:
            #too hard throw it away
            pass
    def flatten(self,d):
        def items():
            for key, value in d.items():
                if isinstance(value, dict):
                    for subkey, subvalue in self.flatten(value).items():
                        #flatten these into the same key value as we are sure they are dupes
                        yield subkey, subvalue
                else:
                    yield key, value
        return dict(items())

if __name__ == "__main__":
    elasticsump = Sump()
    elasticsump.search()
