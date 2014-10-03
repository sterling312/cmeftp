#!/usr/bin/env python
import argparse
from multiprocessing import Pool, cpu_count
import os
import datetime
import ftplib

parser = argparse.ArgumentParser()
parser.add_argument('-f','--filename')
parser.add_argument('-p','--path',default='settle')
parser.add_argument('-d','--date',default=datetime.datetime.now().strftime('%Y%m%d'))

class Connection(ftplib.FTP):
    url = 'ftp.cmegroup.com'
    path = 'settle'
    def __init__(self,filename=None,path='settle'):
        ftplib.FTP.__init__(self,self.url)
        self.path = path
        self.login()
        self.cwd('settle')
        if filename:
            self.download(filename)
        else:
            print(id(self))
            self.filenames = self.nlst()

    def current_file(self,doi=datetime.datetime.today().strftime('%Y%m%d'),filetype='s.xml'):
        xml = (i for i in self.filenames if i.endswith(filetype))
        return set(i for i in xml if doi in i)

    def download(self,filename):
        with open(os.path.join(self.path,filename),'wb') as fh:
            self.retrbinary('RETR {}'.format(filename),fh.write)

def main():
    ftp = Connection(path=args.path)
    if not os.path.isdir(ftp.path):
        os.makedirs(ftp.path)
    filenames = ftp.current_file(args.date)
    print(filenames)
    for f in filenames:
        Connection(f)

if __name__=='__main__':
    args = parser.parse_args()
    if args.filename:
        Connection(args.filename,args.path)
    else:
        main()
