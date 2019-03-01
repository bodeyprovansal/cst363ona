#HRreport1.py
#Bodey Provansal
#CST363 Online Spring 2019A
#Project 2 - Part 3

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
s1 = ("map select d.dept, e.name from employee e "
    " join department d on e.dept=d.dept "
    " where e.empid=d.mgr_id ")

s2 = ("reduce select d.dept, m.name, avg(d.salary), min(d.salary), "
      " max(d.salary), count(*) from depttemp d "
      " join mgrtemp m on d.dept=m.dept "
      " group by dept order by dept")
c = Coordinator()
#create 2 temporary tables for department and managers
c.sendToAll("drop table if exists depttemp ")
c.sendToAll("drop table if exists mgrtemp ")
c.sendToAll("create table depttemp (dept int, salary double)")
c.sendToAll("create table mgrtemp (dept int, name char(20))")

#map for dept table, will provide deptid and salary info
c.sendToAll("map select dept, salary from employee ")
#each map stages info to be inserted into a table, so depttemp 
#shuffle should be called before the mgrtemp map call
c.sendToAll("shuffle insert into depttemp  values {}")
#s1 joins employee and dept table to match mgr_iid to manager name
c.sendToAll(s1)
c.sendToAll("shuffle insert into mgrtemp values {}")

#both temp tables are joined in the final reduce select statement
print("DeptID | Manager | Av Salary | Min Salary | Max Salary | Emp Count")
c.sendToAll(s2)

c.sendToAll("drop table if exists depttemp ")
c.sendToAll("drop table if exists mgrtemp")

c.close()
