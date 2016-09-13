import fileinput
import sys
import os
import os.path
import datetime
##for line in fileinput.input(files('D:\\home\\logs\\batch.log')):
##   print(line)
##fileinput.close()
##design the data model
## yyyymmdd|S|daystimestamp
## yyyymmdd|E|dayetimmstamp|dayduration
## yyyymmdd|node|S|nodestimestamp[|err_code|err_msg]
## yyyymmdd|node|E|nodeetimestamp|nodeduration
nodeinfo=[]
dayinfo=[]
errinfo=[]
totaldur=0
with open('D:\\home\\batch.log','wt') as fb:
    logdir="D:\\home\\logs\\"
    for rt,dirs,files in os.walk(logdir):
        for f in files:
            with open(logdir+f) as fl:
                fb.write(fl.read())
with open('D:\\home\\batch.log') as fp:
    batdate=0
    timestamp=''
    stateflag='I'
    dateflag='I'
    err_code=0
    err_msg=''
    daystimestamp=''
    dayetimmstamp=''
    nodestimestamp=''
    nodeetimestamp=''
    nodeduration=0
    dayduration=0
    for line in iter(fp.readline, ''):
        if '跑批开始' in line:
            err_code=0
            err_msg=''
            daystimestamp=line.split(',')[0]
            batdate=(line.split('[')[1]).split(']')[0]
            dateflag='S'
            dayduration=0
            print(batdate,'|',dateflag,'|',daystimestamp)
            dayinfo.append(batdate+'|'+dateflag+'|'+daystimestamp+'\n')
        if '跑批节点' in line:
            timestamp=line.split(',')[0]
            node=(line.split('[')[1]).split(']')[0]
            if '开始' in line:
                stateflag='S'
                nodestimestamp=line.split(',')[0]
                nodeduration=0
            elif '结束' in line:
                stateflag='E'
                nodeetimestamp=line.split(',')[0]
                nodeduration=(datetime.datetime.strptime(nodeetimestamp,'%Y-%m-%d %H:%M:%S')-datetime.datetime.strptime(nodestimestamp,'%Y-%m-%d %H:%M:%S')).seconds
            else :
                stateflag='N'
            print(batdate,'|',node,'|',stateflag,'|',timestamp,'|',nodeduration)
            nodeinfo.append(batdate+'|'+node+'|'+stateflag+'|'+timestamp+'|'+'{0:d}'.format(nodeduration)+'\n')
        if 'ErrCode' in line and '0000' not in line:
            err_code=(line.split('=')[1]).split('|')[0]
            err_msg=line.split('=')[2]
            timestamp=line.split(',')[0]
            print('err:',batdate,'|',node,'|',stateflag,'|',timestamp,'|',err_code,'|',err_msg)
            errinfo.append('err:'+batdate+'|'+node+'|'+stateflag+'|'+timestamp+'|'+err_code+'|'+err_msg+'\n')
        if '跑批结束' in line:
            dayetimestamp=line.split(',')[0]
            dateflag='E'
            batdate=(line.split('[')[1]).split(']')[0]
            dayduration=(datetime.datetime.strptime(dayetimestamp,'%Y-%m-%d %H:%M:%S')-datetime.datetime.strptime(daystimestamp,'%Y-%m-%d %H:%M:%S')).seconds
            print(batdate,'|',dateflag,'|',dayetimestamp,'|',dayduration)
            totaldur+=dayduration
            dayinfo.append(batdate+'|'+dateflag+'|'+dayetimestamp+'|'+'{0:d}'.format(dayduration)+'\n')
print('totaldur(h):',totaldur/3600)
with open('D:\\home\\logs\\batday.dat','wt') as fd:
    for value in dayinfo:
        fd.write(value)
with open('D:\\home\\logs\\baterr.dat','wt') as fe:
    for value in errinfo:
        fe.write(value)
with open('D:\\home\\logs\\batnode.dat','wt') as fn:
    for value in nodeinfo:
        fn.write(value)