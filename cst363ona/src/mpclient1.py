#mpclient1.py
#Bodey Provansal
#CST363 Online Spring 2019A
#Project 2 - Part 1

import socket, random

USERID=''
PASSWORD=''
hosts=[]
ports=[]
DEBUG = 1

def readConfig():
    global USERID, PASSWORD, hosts, ports, DEBUG 
    f = open("config.txt")
    for line in f:
       tokens=line.split()
       if tokens[0]=='userid':
           USERID = tokens[1]
       elif tokens[0]=='password':
           PASSWORD = tokens[1]
       elif tokens[0]=='worker':
           hosts.append(tokens[1])
           ports.append(int(tokens[2]))
       elif tokens[0]=='debug':
           DEBUG = int(tokens[1])
       else:
           print("configuration file error", line)
    f.close()

class Coordinator:
    def __init__(self):
       self.sockets = []     
       # connect to all workers
       for hostname, port in zip(hosts, ports):
           sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
           sock.connect((hostname, port))
           self.sockets.append(sock)
           if DEBUG >=2:
                print("Connected to", hostname, port)
       
    
    def close(self):
        for sock in self.sockets:
           sock.close();
        self.sockets = []
        if DEBUG >= 2:
             print("All worker connections closed")
    
    def sendToAll(self, stmt):
        for sock,port in zip(self.sockets,ports):
           self.send(sock, stmt)
           if DEBUG >= 1:
               print("to port=", port, "msg=", stmt)
               
        for sock,port in zip(self.sockets,ports):
           status_msg = self.recv(sock)
           if DEBUG >= 1:
               print("from port=",port,"msg=",status_msg)
               
           # check if message received is a set of rows
           # if yes, then print one row per line
           status_msg = str(status_msg).strip()
           if status_msg.startswith("("):
               index = 0
               while index < len(status_msg) and status_msg[index]=='(':
                   index_next = status_msg.find(')',index)
                   print(status_msg[index+1:index_next])
                   index = index_next+1
   
    def recv(self,sock):
        buffer = b''
        while True:
            chunk = sock.recv(2048)
            if len(chunk)==0:
                print('connection error', sock)
                return -1
            buffer = buffer+chunk
            if buffer[-1]==0:
                return buffer[0:-1].decode('utf-8')
            
        
    def send(self, sock, msg):
        buffer = bytes(msg,'utf-8')+b'\x00'
        buflen = len(buffer)
        start = 0
        while start < buflen: 
            sentbytes = sock.send(buffer[start:])
            if sentbytes==0:
                print("connection error", sock)
                return -1
            start=start+sentbytes
        if DEBUG >= 1:
             print("send msg=",msg)
        return 0
    
    def loadTable(self, tableName, filename):
        # read file of data values which must be comma separated and in the same column order as the schema columns
        # first column is the key (which must be integer) and is hashed to distributed the data across
        # worker nodes
        f = open(filename, 'r')
        for line in f:
            line = line.strip()
            sql='insert into '+tableName+' values('+line+')'
            intkey= line[0: line.index(',')]
            index = hash((int(intkey)))%len(ports)
            # index is which server to get the data
            self.send(self.sockets[index], sql)
            rc = self.recv(self.sockets[index])
            if DEBUG >= 1:
                 print("sent",sql,"received",rc)
        f.close()
          
    
    def getRowByKey(self, sql, key):
        index = hash(key)%len(ports)
        self.send(self.sockets[index], sql)
        rc = self.recv(self.sockets[index])
        print("getRowByKey data=", rc)

 
#  main 

readConfig()
#since ids are created randomly, need storage to find ids w/o 
#looking at the db after creation
TESTID = []
#create test data and write to student.data file
f = open("student.data", "w")
for iter in range(1,100):
    id = random.randint(12345, 99999);
    TESTID.append(id)
    gpa = round(random.uniform(2.5, 4.0), 1)
    name = 'Imogen Student'+str(id)
    majors = ['Biology', 'Business', 'CS', 'Statistics']
    major = random.choice(majors)
    line = str(id)+', "'+name+'", "'+major+'", '+str(gpa)+'\n'
    f.write(line)
f.close()

c = Coordinator()
#create tables, drop if already ran
c.sendToAll("drop table if exists student")
c.sendToAll("create table student(id int primary key, name char(20), major char(10), gpa double)") 
c.loadTable("student", "student.data")

#values that should not be included in the database (ids are too small)
print("The following ids will not be printed: 1249, 1000")
c.sendToAll("select * from student where id=1249")
c.sendToAll("select * from student where id=1000")
#Biology Majors
print("Students who are majoring in Biology")
c.sendToAll("select * from student where major = 'Biology'")
#GPA >=3.2
print("Students who have a gpa above 3.1")
c.sendToAll("select * from student where gpa >= 3.2")

#directions were unclear on what this program was expected to return
#so affter the professors email on 2/27 I added the following
print("The first 5 students created. Found by getRowByKey()")
for id in range(0, 5):
  c.getRowByKey("select * from student where id="+str(TESTID[id]), TESTID[id])

c.close()
