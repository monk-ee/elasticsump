#!/usr/bin/env python
__author__ = 'monk-ee'

"""This module hits an elasticsearch index - flattens the data and puts up on s3 ready for redshiftiness.
"""
from pyes import *
import csv
import logging
import yaml
import boto
import time



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
        #output values into the appropriate nest
        # need to convert timestamps to sql friendly format
        for key,value in flatdict.iteritems():
            try:
                #this situation: UnicodeEncodeError: 'ascii' codec can't encode characters in position 0-11: ordinal not in range(128)
                value.encode('ascii', 'ignore')
            except:
                pass
            if key == self.config['es']['date_column']:
                value = time.strftime(self.config['es']['date_format'], time.localtime(value / 1e3))
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

    def compresscsv(self):
        #compress all files in csv folder
        pass


    def sendtos3(self):
        #pop the files into s3
        c = boto.connect_s3(aws_access_key_id=self.config['s3']['aws_access_key_id'],
                            aws_secret_access_key=self.config['s3']['aws_access_key_id'])
        b = c.get_bucket(self.config['s3']['s3_bucket'])
        if os.path.isdir('csv/'):
            for root, dirs, files in os.walk('csv/'):
                for ignore in ignore_dirs:
                    if ignore in dirs:
                        dirs.remove(ignore)
                for file in files:
                    fullpath = os.path.join(root, file)
                    key_name = get_key_name(fullpath, prefix)
                    if not quiet:
                        print 'Copying %s to %s/%s' % (file, self.config['s3']['s3_bucket'] key_name)
                    if not no_op:
                        k = b.new_key(key_name)
                        k.set_contents_from_filename(fullpath, cb=cb, num_cb=num_cb)
                    total += 1
        elif os.path.isfile(path):
            k = b.new_key(os.path.split('csv/')[1])
            k.set_contents_from_filename('csv/')


if __name__ == "__main__":
    elasticsump = Sump()
    elasticsump.search()
