#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 15:26:04 2020

@author: jamesgriffin
"""
"""
TCP/IP Version
"""
from multiprocessing import Process
from collections import OrderedDict
from threading import Thread


import time
import numpy as np
import scipy.io as sp
import matplotlib.pyplot as plt
from enum import Enum
import uuid
import datetime
import sys
import logging
import random
import socket
import select
import pickle
import sys
import networkx as nx

global nodeRun
global nodeStop
global nodePortOpen


class myClass():
    def __init__(self, a, b, q, name):
        self.a = a
        self.b = b      
        self.q = q
        self.name = name
        
class MsgType(Enum):
    """ Class MsgType - Enum
    GenAck = 1
    Delta = 2
    Value = 3
    """
    GenAck = 1
    Delta = 2
    Value = 3
    DisablePath = 4
    EnablePath = 5
    
class Queue:
    """ Class queue - message queues

    Attributes:
        queue: list
            list containing each item in the queue
        head : int
            index of first active item in the queue
        tail: int
            index of last active item in the queue
    
    Methods:
        enqueue()
            add an item to the queue.  Updates head and tail
        dequeue()
            return item at the head of the queue.  Updates head
        delEntry(uuid)
            deletes entry whose uuid matches.  Updates head and/or tail
            returns false if entry not found
        size()
            returns number of active entries in queue
        resetQueue()
            returns new queue, sets head and tail to zero
        trimQueue()
            deletes all entries before head and after tail, i.e., all inactive entries
        returnAll()
            returns a list of all active entries
                                  
    """
    #Constructor
    def __init__(self):
        self.queue = list()
        self.head = 0
        self.tail = 0
        self.entries = 0

    #Queueing elements
    def enqueue(self,message):
        #Checking if the queue is full

        self.queue.append(message)
        self.tail += 1
        self.entries += 1
        # message.printMessage(output=log_out)
        return True     
    
    # Deleting a specific element (message)
    def delEntry(self, itemID):
        if self.size() <= 0:
            self.resetQueue()
            return False

        for i in range (self.entries):
            message = self.queue[i] 
            if message.id == itemID:
                del self.queue[i]
                if i == self.head:
                    self.head += 1
                else:
                    self.tail -= 1 
                self.entries -= 1
                return True
        return False
                   
    #Dequeuing elements 
    def dequeue(self):
        #Checking if the queue is empty
        if self.size() <= 0:
            self.resetQueue()
            return False
        message = self.queue[self.head]
        self.head+=1
        self.entries -= 1
        return message
                
    #Calculate size
    def size(self):
        return self.tail - self.head
            
    #Reset queue
    def resetQueue(self):
        self.tail = 0
        self.head = 0
        self.queue = list()
        self.entries = 0
    
    #Trim queue - remove dequeued elements    
    def trimQueue(self):
        qlen = self.size()
        del self.queue[:self.head]
        del self.queue[self.tail:]
        self.head = 0
        self.tail = qlen
    
    # returnAll - return all active elements in queue
    def returnAll(self):
        data = []
        # for message in self.queue[self.head : self.tail+1]:
        for message in self.queue:
            data.append(message)
        return data
    
    def empty(self):
        if self.size() == 0:
            return True
        else :
            return False
    
    #end class Queue
    

class nodeMessage:
    """ Class nodeMessage - structure of message from one node to another
    
    Calling:
        obj = nodeMessage (type, fromNoe, toNode, path, value, unAckCt)
    
    Attributes:
        msgType : enum MsgType
            defines the type of the message content
        fromNode : Node
            sending node
        toNode : Node
            receiving node
        value : float
            value to send
        id : uuid
            automatically allocated uuid
        density: int
            density of sending node
        delay: datetime
            receive delay
        sequence: int
            message sequence
        
    Methods:
        printMessage()
            print message to stdout
        msgStr()
            returns a string with the content of the message
        logMessage(log_out)
            log the message to log_out
    
    """
    def __init__(self, msgType, fromNode, toNode, value = 0,
                 delta = 0, density = 0, delay = 0, sequence = 0):
        
        self.msgType = msgType     
        self.fromNode = fromNode
        self.toNode = toNode
        self.value = value
        self.delta = delta
        self.id = uuid.uuid1()
        self.density = density
        self.delay = delay
        self.sequence = sequence

     
    # Method to print the contents of a message    
    def printMessage (self,output=sys.stdout):
        print (self.msgStr,file=output,flush=True)

    def msgStr (self):
        aString = (f'Message type {self.msgType} from: {self.fromNode} to: ' +
               f'{self.toNode}' +
               f' value: {self.value} delta: {self.delta} density: {self.density}' +
               f' sequence: {self.sequence}, time stamp: {date_time(self.id)}')    
        return aString
    
    def logMessage (self):
        logging.info(self.msgStr())
    

    #end class nodeMessage   
        
class cmdMessage():
    def __init__(self, msgType, toNode, var1 = 0, var2 = 0):
        
        self.msgType = msgType
        self.toNode = toNode
        self.var1 = var1
        self.var2 = var2
        
    def msgStr (self):
        aString = (f'Command message type {self.msgType} to: {self.toNode} ' +
                   f'var1: {self.var1} var2: {self.var2}')
        return aString
        
    def printMessage(self):
        print (self.msgStr)
        
    def logMessage (self):
        logging.info(self.msgStr())
        
class path:
    """ class path - path object
    
    Attributes:
        fromNode : Node
            Node object of one end of path.  Lower numbered node of the path.
        toNode : Node
            Node object of the other end of the path.  Higher numberd node of the path.
        status: bool
            Status of the path (is communication available?)
    
    Methods:
        setStatus (bool)
            Sets status of the path
        getStatus ()
            Returns status of the path
        getPathTuple()
            Returns list of the nodes at the end of the path
                      
    """
    
    
    def __init__(self, fromNode, toNode, status=True):
        self.fromNode = fromNode
        self.toNode = toNode            
        self.status = status
        
    def setStatus (self,status):
        self.status = status
        
    def getStatus (self):
        return self.status
    
    def getPathTuple (self):
        return (self.fromNode, self.toNode)
        
    def pathStr (self):
        aString = (f'From: {self.fromNode}  To: {self.toNode} ' +
               f'  Status: {self.status}')
        return aString
    
    def logPath (self):
        logging.info(self.pathStr())
    #end class path

# Note that in this version, the class, which is instantianted, only contains
# nodal data.  All methods are at the root level.  The run method is instantiated
# in parallel.

# each previous class method now contain a reference to the node class,

class Node():
    
    def __init__(self, nodeID, value, neighborNodes, peerSocketNos, 
                 hostSockNum,verbose):
        
        self.z = value
        self.zlast = value
        self.x = value
        self.rQueue = Queue()
        self.sQueue = Queue()
        self.cQueue = Queue()
        self.recvQueue = Queue()
        self.sentQueue = Queue()
        self.sentDict = OrderedDict()
        self.nodeID = nodeID
        self.neighborNodes = neighborNodes
        self.neighborDict = {}
        self.neighborDensity = []
        self.neighborPaths = []
        self.pathDict = {}
        self.sendBlock = False
        self.recvBlock = False
        self.status = True
        self.pathCount = 0
        self.density = 0
        self.delta = None
        self.deltaij = None
        self.weight = None
        self.weightedDeltaij = None
        self.nNeighbors = 0
        self.deltaUpdateBlock = None
        self.flipFlop = True
        self.sequence = None
        self.neighborStatus = None
        self.firstSendErr = False
        self.firstRecvErr = False
        self.peerSocketNos = peerSocketNos
        self.message = None
        self.hostSockNum = hostSockNum
        self.hostConnection = None
        self.peerConnections = None
        self.peerHandler = None
        self.peerHandlerThread = None
        self.inSocks = []
        self.outSocks = []
        self.listenSocks = []
        self.nodeSocksDict = {}
        self.verbose = verbose
                
        i = 0
        for node in self.neighborNodes:
            self.neighborDict.update({node: i})
            i += 1
        
        # i = 0
        # for path in self.neighborPaths:
        #     self.pathDict.update({path: i})
        #     i += 1
            
        self.nNeighbors = len(self.neighborNodes)
        # print (f'Node: {self.nodeID} Neighbors: {self.neighborNodes}')
        
        self.delta = np.zeros((self.nNeighbors,),dtype=float)
        self.deltaij = np.zeros((self.nNeighbors,),dtype=float)
        self.weightedDeltaij = np.zeros((self.nNeighbors,),dtype=float)
        self.deltaUpdateBlock = np.zeros((self.nNeighbors,1),dtype=int)
        self.density = self.nNeighbors
        self.sequence = np.zeros((self.nNeighbors,),dtype=int)
        self.neighborDensity = np.zeros((self.nNeighbors,),dtype=int)
        self.neighborStatus = np.zeros((self.nNeighbors,),dtype=int)
        self.weight = np.zeros((self.nNeighbors,),dtype=float)


        for node in self.neighborNodes:
            if node > self.nodeID:
                self.sequence[self.neighborDict.get(node)] = 1
            else:
                self.sequence[self.neighborDict.get(node)] = 0
            self.neighborStatus[self.neighborDict.get(node)] = True
            self.neighborPaths.append (path(self.nodeID, node))
        
        # connect to host
        host_address = ('localhost', self.hostSockNum)
        self.hostConnection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.hostConnection.setsockopt(socket.SOL_SOCKET,
                                                socket.SO_REUSEPORT,1)
        self.hostConnection.connect(host_address)
        self.hostConnection.setblocking(0)
        message = nodeMessage(MsgType.Value,self.nodeID,-1,self.z)
        self.hostConnection.send(pickle.dumps(message))
        self.peerConnections = [-1]*self.nNeighbors
        
        # create listening sockets where this node is < neighbor node number
                
        for node in self.neighborNodes:
            if self.nodeID < node:
                nodeIdx = self.neighborDict[node]
                address = ('localhost', self.peerSocketNos[nodeIdx])
                self.peerConnections[nodeIdx] = socket.socket(socket.AF_INET,
                    socket.SOCK_STREAM)
                self.peerConnections[nodeIdx].setsockopt(socket.SOL_SOCKET,
                                                socket.SO_REUSEPORT,1)
                self.peerConnections[nodeIdx].setblocking(0)
                self.peerConnections[nodeIdx].bind(address)
                self.peerConnections[nodeIdx].listen(5)
                if self.verbose:
                    print (f'Node: {self.nodeID} peer socket listening on address: '+
                       f'{address} for node {node}')
                self.listenSocks.append(self.peerConnections[nodeIdx])
                self.nodeSocksDict[self.peerConnections[nodeIdx]] = node
                
                
        # start node socket handler thread
        self.inSocks = self.listenSocks.copy()
        
        self.peerHandler = handleSocks(self.nodeID, self.verbose)
        try:
            self.peerHandlerThread = Thread(target = self.peerHandler.run,
                                            args=(self.inSocks, self.outSocks,
                                                  self.listenSocks,
                                                  self.nodeSocksDict,
                                                  self.peerConnections,
                                                  self.neighborDict,
                                                  self.rQueue))
                                           

        except:
            print ('error establishing socket manager')
            for sock in self.inSocks:
                sock.close()
            exit
        if self.verbose: print ('Socket manager instantiated as thread')
        try:
            self.peerHandlerThread.start()
        except:
            print ('error starting socket manager')
            for sock in self.inSocks:
                sock.close()
                exit
        if self.verbose: print ('Socket manager running')
        
def connectNode (node):
    for nodeNo in node.neighborNodes:
        if node.nodeID > nodeNo:
            nodeIdx = node.neighborDict[nodeNo]
            address = ('localhost', node.peerSocketNos[nodeIdx])
            connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connection.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEPORT,1)
            connection.connect(address)
            connection.setblocking(0)
            res = [key for key, value in node.nodeSocksDict.items() if value == nodeNo]
            if res:
                del node.nodeSocksDict[res]
                if node.verbose: print (f'removed {res} from node.nodeSocksDict')
            node.peerConnections[nodeIdx] = connection
            node.peerHandler.addInSock(connection, nodeNo)
            
def processCQueue(node):
    while not node.cQueue.empty():
        cMessage = node.cQueue.dequeue()
        if cMessage.toNode == node.nodeID:
            if cMessage.msgType == MsgType.DisablePath:
                node.setPathStatus(cMessage.var1,cMessage.var2, False)
            elif cMessage.msgType == MsgType.EnablePath:
                node.setPathStatus(cMessage.var1,cMessage.var2, True)

def processRQueue(node):
    # count = 0
    
    # acquire lock; pull my rQueue messages into self.rQueue

    while not node.rQueue.empty():
        node.message = node.rQueue.dequeue()
        node.recvQueue.enqueue(node.message)
        
    
        if node.message.fromNode not in node.neighborNodes:
            logMsg (node, node.message, queue = 'rQueue',level='warning')
        else:
            logMsg (node, node.message, queue = 'rQueue')
  
        if node.message.msgType == MsgType.GenAck:
            processAckMsg(node, node.message)               
        elif node.message.msgType == MsgType.Delta:
            sendAck(node, node.message)
            recvDelta(node, node.message)
        elif node.message.msgType == MsgType.Value:
            sendAck(node, node.message)
            recvValue(node, node.message)
            
def logMsg (node,message,queue='sQueue',level='info'):
    # print (f'Node: {self.nodeID}: Queue: ', queue,message.msgStr(),
    #        file=log_out,flush=True)
    if level == 'info':
        logging.info(f'Node: {node.nodeID}: Queue: {queue}: {message.msgStr()}')
    elif level == 'warning':
        logging.warning(f'Node: {node.nodeID}: Queue: {queue}: {message.msgStr()}')

def processAckMsg (node, message):
#        self.sentQueue.delEntry(message.id)
    if message.id in node.sentDict:
        del node.sentDict[message.id]

def findPath (node,fromNode,toNode):
    # breakpoint()
    for path in node.neighborPaths:
        if fromNode == path.fromNode and toNode == path.toNode:
            return path
        elif toNode == path.fromNode and fromNode == path.toNode:
            return path
    return False

def sendAck (node,message):
    messageAck = nodeMessage(MsgType.GenAck,node.nodeID,message.fromNode)
    messageAck.id = message.id
    node.sQueue.enqueue(messageAck)
    if messageAck.toNode not in node.neighborNodes:
        logMsg(node,messageAck, level='warning')
    else:
        logMsg(node,messageAck)

def sendData (node):
    # breakpoint()
    for aNodeNo in node.neighborNodes:
        nodeIdx = node.neighborDict.get(aNodeNo)
        if aNodeNo > node.nodeID:
            messageSend = nodeMessage(MsgType.Value,node.nodeID,aNodeNo)
            messageSend.value  =  node.x
        else:
            messageSend = nodeMessage(MsgType.Delta,node.nodeID,aNodeNo)
            messageSend.delta = node.delta[nodeIdx]
        messageSend.sequence = node.sequence[nodeIdx]
        messageSend.density = node.density
        node.sQueue.enqueue(messageSend)
        if messageSend.toNode not in node.neighborNodes:
            logMsg(node, messageSend,level='warning')
        else:
            logMsg(node, messageSend)

def calcX (node):
    node.weightedDeltaij = np.multiply(node.weight, node.deltaij)
    node.x = node.z + (node.z - node.zlast) + np.sum(node.weightedDeltaij)
    node.zlast = node.z

def recvValue (node,message):
# value is state of fromNode
    for aNodeNo in node.neighborNodes:
        if aNodeNo == message.fromNode:
            nodeIdx = node.neighborDict.get(aNodeNo)
            if node.sequence[nodeIdx] == (message.sequence -1):    

                node.delta[nodeIdx] = message.value - node.x      
                node.deltaij[nodeIdx] += node.delta[nodeIdx]
                node.weight[nodeIdx] = 2./(node.density + message.density +2.)
                calcX(node)
                node.sequence[nodeIdx] += 1
                node.neighborDensity[nodeIdx] = message.density

def recvDelta (node,message):       
    # value is delta of fromNode
    for aNodeNo in node.neighborNodes:
        if aNodeNo == message.fromNode:
            nodeIdx = node.neighborDict.get(aNodeNo)
            if message.sequence == node.sequence[nodeIdx]:
                node.delta[nodeIdx] =  -message.delta
                node.deltaij[nodeIdx] += node.delta[nodeIdx]
                node.weight[nodeIdx] = 2./(node.density + message.density + 2.)
                calcX(node)
                node.sequence[nodeIdx] += 1
                node.neighborDensity[nodeIdx] = message.density
                        
def checkSent(node):
  if np.all(node.neighborDensity):
      keys = node.sentDict.keys()
      for key in keys:
          message, count = node.sentDict[key]
          # if count > 3*np.sum(self.neighborDensity):
          #     # print (f'Count: {count} {message.msgStr()}')
          #     node = message.toNode
          #     nodeIdx = self.neighborDict.get(node)
          #     self.neighborStatus[nodeIdx] = False
          #     self.deltaij[nodeIdx] = 0;
          #     self.calcX()
          count += 1
          node.sentDict.update({message.id: (message, count)})  

""" Note that sendMessage directly manipulates the to node's rQueue.  
    It does so through the global nodeList.  In a parallel scheme, the global
    nodeList won't be available and sendMessage will have to use TCP/IP comms
"""               
def sendMessage(node, nodeMessage):
#        print (f'Node: {node.nodeID} would send to node: {nodeMessage.toNode}')
    # pass
    if not node.sendBlock:
        toNode = nodeMessage.toNode
        nodeIdx = node.neighborDict.get(toNode)
        try:
            connection = node.peerConnections[nodeIdx]
            connection.send(pickle.dumps(nodeMessage))
        except:
            print (f'Node: {node.nodeID} error sending message to {nodeMessage.toNode}',
                   flush=True)
            print (node.peerConnections[nodeIdx],flush=True)
            sys.exit()

def recvMessage(node, nodeMessage):
    if not node.recvBlock:
        pass
                           
def processSQueue(node):
    if not node.sendBlock:
        while not node.sQueue.empty():
            message = node.sQueue.dequeue()
            if message.msgType !=MsgType.GenAck:
                node.sentQueue.enqueue(message)
                node.sentDict.update({message.id: (message, 0)})
            sendMessage(node, message)            

def setPathStatus (node,fromNode,toNode,state):
    path = node.findPath (fromNode, toNode)
    if path:
        path.setStatus(state)
        message = path.pathStr()
        if node.verbose: print (f'Node: {node.nodeID} {message}')

def blockComms (node):
    node.sendBlock = True
    node.recvBlock = True
    
def unBlockComms (node):
    node.sendBlock = False
    node.recvBlock = False    

def setValue(node, value):
    node.z = value
    
def getValue (node):
    return node.zno

def setX (node, value):
    node.x = value
    
def getX (node):
    return node.x
            
def setStatus(node, status):
    node.status = status
    
def getStatus (node):
    return node.status

def run(node):
    # while True:
        processCQueue(node)
        if nodeRun[node.nodeID]:
            if node.status == True:   
                processRQueue(node)
                checkSent(node)
                sendData(node)
                processSQueue(node)
        # elif nodeStop[node.nodeID]:
        #     break
        # time.sleep(0.0005)        

def nodeTerminate(node):

    node.peerHandler.terminate()
    node.peerHandlerThread.join()
    if node.verbose: print (f'Node {node.nodeID} nodeSockMgr terminated')
    
 
def buildConnection (mpc):
    """
    buildConnection (mpc)
    
    
    Parameters
    ----------
    mpc : structure
        MatPower data structure
        
    Returns
    -------
    C : numPy float array
        Connection (or association) array
    
    """

# build the connectivity matrix

    nNode = mpc.bus.shape[0]
    from_bus = mpc.branch[:,0].astype(int)
    to_bus = mpc.branch[:,1].astype(int)
    C = np.zeros((nNode,nNode),dtype=int)
    C[from_bus, to_bus] = 1
    C = C + C.transpose()
    return C
#end def buildConnection    
    
def buildSocket (mpc):
    
    baseSocket = 59152
    nNode = mpc.bus.shape[0]
    S = np.zeros((nNode,nNode),dtype=int)
    hostPortNos = []
    for i in range(nNode):
        for j in range(nNode):
            S[i,j] = -1
    nBranch = mpc.branch.shape[0]
    for i in range(nBranch):
        from_bus=mpc.branch[i,0].astype(int)
        to_bus = mpc.branch[i,1].astype(int)
        S[from_bus,to_bus] = baseSocket + i
        S[to_bus,from_bus] = baseSocket + i
    baseSocket += nBranch
    for i in range(nNode):
        hostPortNos.append(baseSocket + i)
    return S, hostPortNos


def buildGraph (busDict, lineList):
    """
    buildGraph (busDict,LineList)
    builds networkx graph

    Parameters
    ----------
    busDict : dictionary
        Bus Dictionary, defining vertices
    lineList : list
        List of lines, defining edges.

    Returns
    -------
    G : Graph
        Network graph 

    """
    # create network graph
    plt.figure()
    G = nx.Graph()
    G.add_nodes_from(busDict.values())
    G.add_edges_from(lineList)
    nx.draw(G,with_labels=True)
    return G
    # end def buildGraph
    
# function date_time returns datetime from a uuid.  It is based on how
# python implements uuid.
def date_time (uuid_in):   
    """
    

    Parameters
    ----------
    uuid_in : uuid
        Type 1 uuid.

    Returns
    -------
    datetime
        datetime from uuid timestamp

    """
    return datetime.datetime.fromtimestamp((uuid_in.time - 
                                            0x01b21dd213814000)*100/1e9)
     #end date_time

def findNode (nodeID):
    for nodeCell in nodeList:
        if nodeCell.nodeID == nodeID:
            return nodeCell
    return False

def openSockets(hostSockNos, verbose):
    hostConnections = []
    hostSocksDict = {}
    for i in range(len(hostSockNos)):
        address = ('localhost', hostSockNos[i])
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEPORT,1)
        sock.setblocking(0)
        sock.bind(address)
        sock.listen(5)        
        hostConnections.append(sock)
        hostSocksDict[sock] = i
        if verbose: print (f'Node: {i} host socket listening on address: {address}')
    return hostConnections, hostSocksDict
        

class handleSocks:
    def __init__(self, nodeID, verbose):
        self.running = True
        self.nodeID = nodeID
        self.inSocks = None
        self.outSocks = None
        self.listenSocks = None
        self.socksDict = None
        self.connections = None
        self.nodeDict = None
        self.queue = None
        self.verbose = verbose
        
    def terminate (self):
        self.running = False
        
    def addInSock(self, inSock, nodeNo):
        self.inSocks.append(inSock)
        if inSock in self.socksDict:
            self.socksDict.pop(inSock)
        self.socksDict[inSock] = nodeNo
        
    
    def run (self, inSocks, outSocks, listenSocks, socksDict, connections, 
             nodeDict, queue):
        self.inSocks = inSocks
        self.outSocks = outSocks
        self.listenSocks = listenSocks
        self.socksDict = socksDict
        self.connections = connections
        self.nodeDict = nodeDict
        self.queue = queue
        
        while self.running:
            
            
            try:
                readable, _, exceptional = select.select(self.inSocks, self.outSocks,
                                                         self.inSocks,0.00001)
            except:
                print (f'select except. inSocks: \n\t{self.inSocks}\noutSocks:\n\t' + \
                       f'{self.outSocks}',flush=True)
                sys.exit()
            
            for s in readable:
                if s in listenSocks:
                    # received connect
                    try:
                        connection, client_addr = s.accept()
                    except:
                        print ('error accepting')
                        sys.exit()
                    if self.verbose: print (f'Node {socksDict[s]} connected')
                    if self.verbose: print (f'Connection: {connection}')
                    connection.setblocking(0)
                    self.inSocks.append(connection)
                    nodeNo = self.socksDict.get(s)
                    self.inSocks.remove(s)
                    del self.socksDict[s]
                  
                    self.socksDict[connection] = nodeNo
                    nodeIdx = nodeDict[nodeNo]
                    self.connections[nodeIdx] = connection
                    
                else:
                    data = s.recv(1024)
                    if data:
                        # received data
                        try:
                            message = pickle.loads(data)
                        except:
                            print (f'Pickle load error, node {self.nodeID}')
                        if s not in self.outSocks:
                            self.outSocks.append(s)
                        self.queue.enqueue(message)

                    else:
                        # socket closed
                        nodeNo = self.socksDict.get(s)
                        if self.verbose: print (f'node {nodeNo} {s.getpeername()} closed')
                        if s in self.outSocks:
                            outSocks.remove(s)
                        self.inSocks.remove(s)
                        self.socksDict.pop(s)
                        s.close()
            
            for s in exceptional:
                print (f'socket exception for node {self.socksDict[s]}')
                self.inSocks.remove(s)                        
                if s in self.outSocks:
                    self.outSocks.remove(s)
                self.socksDict.pop(s)

                s.close()
        
        if self.nodeID == -1:
            if self.verbose: print (f'Host socket manager terminating')
        else:
            if self.verbose: print (f'Node: {self.nodeID} socket manager terminating')     
            
        for s in inSocks:
            s.close()
"""
    Main Entry Point
"""    
if __name__ == '__main__':
    verbose = True
    
    # startup manager process to manage node instances
    
    logging.basicConfig(filename='DistAvg10.log', filemode='w', level=logging.INFO, 
                        force=True)
#    log_out = open('output.txt','w')

    
    print ("Start time:", date_time(uuid.uuid1()))
    # Read case 14 matlab data file
    mat = sp.loadmat('mpc_case14',struct_as_record=False,squeeze_me=True)
    mpc = mat['mpc']
    
    # remove offset from bus number

    nNode = mpc.bus.shape[0]
    nGen = mpc.gen.shape[0]
    nBranch = mpc.branch.shape[0]
                          
    # build a dictionary of buses.  The key is the original bus number
    # the value is the enumeration of the original bus number.
    
    busDict = {v.astype(int):k for k,v in enumerate(mpc.bus[:,0])}
    
    # replace original (key value) bus number with it's enumeration
                                    
    # replace bus number in mpc.bus with locally increasing bus numbers
    for i in range(nNode):
        mpc.bus[i,0] = busDict.get(mpc.bus[i,0])
    
    # replace branch buses with corresponding local buses
    lineList = []
    dLineList = []
    for i in range(nBranch):
        mpc.branch[i,0] = busDict.get(mpc.branch[i,0])
        mpc.branch[i,1] = busDict.get(mpc.branch[i,1])
        fromNode = mpc.branch[i,0].astype(int)
        toNode = mpc.branch[i,1].astype(int)
        lineList.append ((fromNode,toNode)) 
        dLineList.append ((max(fromNode,toNode),min(fromNode,toNode)))
        
    # replace generator bus with local bus number
    for i in range(nGen):
        mpc.gen[i,0] = busDict.get(mpc.gen[i,0])
    
    # build connection matrix
    pathList = []
    C = buildConnection(mpc)
    
    # build socket matrix
    S, hostSockNos = buildSocket(mpc)
    
    # create network graph
    G = buildGraph(busDict, lineList)
    
    # build list of node names
    nodeName = ['Node'+str(i) for i in range(nNode)]
    nodeDict = {i:i for i in range(nNode)}
    

    # Two sets of queues are established here.  Each list has an entry for each
    # node
    # The first, rQueue, is the receive queue used to communicate between nodes.
    # It is shared at the global level because in this scheme, a node "sends" a message
    # by queueing it in the receiver node's rQueue
    # The second, cQueue, is a command queue.  It is how the main thread can 
    # send a command to the nodes
    
    rQueue = []
    cQueue = []
    for i in range(nNode):
        rQueue.append(Queue())
        cQueue.append(Queue())
    
    # open the host sockets
    
    hostConnections, hostSocksDict = openSockets(hostSockNos, verbose)
    if verbose:
        print ('host sockets open')
        
    # start host socket thread manager
    
    inSocks = hostConnections.copy()
    listenSocks = hostConnections.copy()
    outSocks = []
    hostSockMgr = handleSocks(-1, verbose)           # node -1 means host
    host_rQueue = Queue()
    try:
        th = Thread(target = hostSockMgr.run, args=(inSocks, outSocks, listenSocks, 
                                                hostSocksDict, hostConnections,
                                                nodeDict, host_rQueue))
    except:
        print ('error establishing socket manager')
        for sock in inSocks:
            sock.close()
        exit
    if verbose: print ('Socket manager instantiated as thread')
    try:
        th.start()
    except:
        print ('error starting socket manager')
        for sock in inSocks:
            sock.close()
            exit
    if verbose: print ('Socket manager running')
    nodeList = []

    nodeCount = []

    for i in range(nNode):
        # nodeCell = Node(i,random.random())
        neighborList = [j for j in range(nNode) if C[i,j]]
        socketNums = [S[i,j] for j in range(nNode) if S[i,j] > 0]
        nodeCount.append(0)

        # try:
        #     th = Node(i, float(i+1), neighborList, socketNums)
        # except:
        #     print ('Error instantiating thread')
        #     exit()
        # threadList.append(th)
        
        nodeList.append(Node(i, float(i+1), neighborList, socketNums, 
                             hostSockNos[i],verbose))
    # create the pathList
    pathList = []
    for (f,t) in lineList:
        fromNode = findNode(f)
        toNode = findNode(t)
        pathList.append(path(fromNode, toNode))
    
    for node in nodeList:
        connectNode(node)
        
        
    # seed averages array
    nodeRun = np.zeros((nNode,1),dtype=int)
#    nodeCount = np.zeros((nNode,1),dtype=int)
    max_iter = 27000
    averages = np.zeros((max_iter+1)*nNode).reshape(max_iter+1,nNode)
    x = np.zeros((nNode,),dtype=float)
    sumAvg = 0
    loop_iter = 1

    """ 
    Find consensus
    """    
    counts = np.zeros((nNode,),dtype=int)
    values = np.zeros((max_iter+1)*nNode).reshape(max_iter+1,nNode)
    curValue = np.zeros((nNode,),dtype=float)
    nodeVals = np.zeros((nNode,),dtype=float)
    nodeRun = np.zeros((nNode,),dtype=int)
    nodeStop = np.zeros((nNode,),dtype=int)
    consensus = False
    randomFlag = False
    numSamples = 0
    seqNode = 0
    procList = []
    
    for i in range(nNode):
        nodeRun[i] = 1
        

    for i in range(nNode):
        curValue[i] = random.random()
#        nodeRun[i] = True
        # try:
        #     threadList[i].start()
        # except:
        #     print (f'Error starting node {i}')
        #     exit()

    for i in range(nNode):
        # rQueue[i].resetQueue()
        nodeRun[i] = 1
    
 
    for m in range (100):
        
        for i in range (nNode):
            node = nodeList[i]
            run(node)
            nodeVals[i] = getX(node)
    
        
    # while abs(np.max(curValue) - np.min(curValue)) > 1e-4: 
        
        # if numSamples == 300:
        #     msg1 = cmdMessage (msgType=MsgType.DisablePath,toNode=4,var1=4,var2=5)
        #     msg2 = cmdMessage (msgType=MsgType.DisablePath,toNode=5,var1=4,var2=5)
        #     for aqueue in cQueue:
        #         aqueue.put(msg1)
        #         aqueue.put(msg2)
        
        # if loop_iter == 75:
        #       nodeList[8].setStatus(False)
        #       for node in nodeList:
        #           node.setPathStatus (4, 5, False)
            
        # if loop_iter == 200:
        #     nodeList[8].setStatus(True)
        #     for node in nodeList:
        #           node.setPathStatus (4, 5, True)
        # # if np.average(counts) > 250
        #     k = 1.0
        #     for j in range (nNode,0,-1):
        #         nodeList[j-1].setValue(k)
        #         k += 1.0

        averages[numSamples,0:nNode] = nodeVals
        numSamples += 1
        
                
        loop_iter += 1
        if  np.max(counts) > max_iter - 1:
            break
 
    for connection in hostConnections:
        connection.close()
    if verbose: print (f'(Reached end of iterations')   
    for i in range(nNode):
        nodeRun[i] = False
        nodeStop[i] = True
    
    hostSockMgr.terminate()
    th.join()
    
    for node in nodeList:
        nodeTerminate(node)
            
    print (f'Consensus reached in {np.average(counts)} iterations')    
    for nodeCell in nodeList:
        print (f'Node {nodeCell.nodeID} has consensus value ' +
              f'{nodeCell.x}')
        
        
    plt.figure()
    # for i in range (nNode):
    #     plt.plot(averages[0:counts[i],i])
    plt.plot(averages[0:numSamples])
    plt.title("Average Convergence")
    plt.xlabel("Node Iterations")
    plt.ylabel("Average")
    plt.savefig("Consensus.pdf")