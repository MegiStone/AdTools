import os
import os.path
##use vars to match log's file type
manstdft='managestdout.log'
batstdft='batchstdout.log'
calstdft='calonlinestdout.log'
logdir='D:\\home\\wasadmin\\log'
##manlogdir='D:\\home\\wasadmin\\log\\apmanage'
##batlogdir='D:\\home\\wasadmin\\log\\apbatch'
##calstdft='D:\\home\\wasadmin\\log\\apcalonline'
sdict={}
for rt,dirs,files in os.walk(logdir):
    for f in files:
        fname=os.path.splitext(f)
        if(manstdft in fname or batstdft in fname or calstdft in fname):
            print(os.path.join(rt,f))
            with open(os.path.join(rt,f),'r') as fo:
                for line in iter(fo.readline, ''):
                    if 'SessionID' in line and 'SQL:' in line:
                        lss=line.split(' ')
                        timestamp=lss[3]+' '+lss[4]
                        sid=lss[7]
                        sql=line.split('SQL:')[1]
                        if sid not in sdict:
                            conts=[]
                            conts.append(timestamp+'|'+sql)
                            sdict[sid]=conts
                        else:
                            sdict[sid].append(timestamp+'|'+sql)

with open('D:\\home\\anaBus.dat','wt') as fa:
    for k,v in sdict.items():
        fa.write(k+'\n')
        v1=sorted(v)
        for value in v1:
            fa.write(value)