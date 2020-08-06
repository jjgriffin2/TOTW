#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 10:16:19 2020

@author: jamesgriffin
"""

""" THREADED VERSION """

import numpy as np
import scipy.io as sp
import networkx as nx
import uuid
import datetime
import time
import random
import matplotlib.pyplot as plt
from enum import Enum
import sys
from collections import OrderedDict
import logging
from threading import Thread, Lock
import queue
from multiprocessing import Process
# from multiprocessing.managers import BaseManager, BaseProxy

# # define class for queing/dequeing.  The queue is positional
# class msgQueue:
#     """ Class queue - message queues

#     Attributes:
#         queue: list
#             list containing each item in the queue
#         head : int
#             index of first active item in the queue
#         tail: int
#             index of last active item in the queue
    
#     Methods:
#         enqueue()
#             add an item to the queue.  Updates head and tail
#         dequeue()
#             return item at the head of the queue.  Updates head
#         delEntry(uuid)
#             deletes entry whose uuid matches.  Updates head and/or tail
#             returns false if entry not found
#         size()
#             returns number of active entries in queue
#         resetQueue()
#             returns new queue, sets head and tail to zero
#         trimQueue()
#             deletes all entries before head and after tail, i.e., all inactive entries
#         returnAll()
#             returns a list of all active entries
                                  
#     """
#     #Constructor
#     def __init__(self):
#         self.queue = list()
#         self.head = 0
#         self.tail = 0
#         self.entries = 0

#     #Queueing elements
#     def enqueue(self,message):
#         #Checking if the queue is full

#         self.queue.append(message)
#         self.tail += 1
#         self.entries += 1
#         return True     
    
#     # Deleting a specific element (message)
#     def delEntry(self, itemID):
#         if self.size() <= 0:
#             self.resetQueue()
#             return False

#         for i in range (self.entries):
#             message = self.queue[i] 
#             if message.id == itemID:
#                 del self.queue[i]
#                 if i == self.head:
#                     self.head += 1
#                 else:
#                     self.tail -= 1 
#                 self.entries -= 1
#                 return True
#         return False
                   
#     #Dequeuing elements 
#     def dequeue(self):
#         #Checking if the queue is empty
#         if self.size() <= 0:
#             self.resetQueue()
#             return False
#         message = self.queue[self.head]
#         self.head+=1
#         self.entries -= 1
#         return message
                
#     #Calculate size
#     def size(self):
#         return self.tail - self.head
            
#     #Reset queue
#     def resetQueue(self):
#         self.tail = 0
#         self.head = 0
#         self.queue = list()
#         self.entries = 0
    
#     #Trim queue - remove dequeued elements    
#     def trimQueue(self):
#         qlen = self.size()
#         del self.queue[:self.head]
#         del self.queue[self.tail:]
#         self.head = 0
#         self.tail = qlen
    
#     # returnAll - return all active elements in queue
#     def returnAll(self):
#         data = []
#         # for message in self.queue[self.head : self.tail+1]:
#         for message in self.queue:
#             data.append(message)
#         return data
#     #end class Queue

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

#@ray.remote
class Node(Process):
    """ class Node - Node object
    
    Attributes:
        z : float
            Node current value
        zlast : float
            Node past value
        x : float
            Estimate of node value
        rQueue : Queue
            Message receive queue
        sQueue: Queue
            Message send queue
        sentQueue : Queue
            Message sent queue
        sentDict : Dictionary
            Message sent dictionary.  Key is message.id
        nodeID: int
            Node id (number)
        neighborNodes : [Node Node Node ...]
            List of neighboring nodes
        connectPaths : [Path Path Path ...]
            List of connected paths (grid-wide)
        neighborPaths : [Path Path Path ... ]
            List of neighboring paths
        sendBlock: bool
            Boolean to block sending of messages
        recvBlock : bool
            Boolean to block receiving of messages
        status : bool
            Boolean to prevent all node activity
        pathCount : int
            Count of connected paths
         density : int
            Density of this node
        nNeighbors : int
            Number of immediate neighbors
        deltaUpdateBlock : bool
            Block calculation of delta
        flipFlop : bool
            Boolean to flip between calculation modes
        discoveryMode: bool
            In discovery
            
    Methods:
        
        processRQueue()
            Processes messages in the receive queue
        processAckMessage (message)
            Processes general acknowledge message
        processDiscoverMsg (message)
            Processes network discovery message
        setNeighborNodes (neighborNodes)
            Sets neighborNode
            Initializes neightbor nodes 
        getNeighborNodes()
            Returns list of neighboring nodes
        connectPathExists (path)
            Returns true if path exists, otherwise false
        sendConnectPaths()
            Send known paths to neighbors
        sendNewPaths()
            Send newly discovered paths to neighbors
        clearConnectPaths()
            Clears discovered paths, resets connectPaths to immediate neighbors
        sendAck (message)
            Sends acknowledgement of massage
        sendData ()
            Sends either value update (MsgType.Value) or delta update (MsgTypeDelta)
        recvValue (message)
            Responds to value update message
        recvDelta (message)
            Responds to delta update message
        sendMessage (message)
            Simulates actual sending of message by queueing it in the receivers
            rQueue
        recvMessage (message)
            Simulates receiving a message.  Actually does nothing
        processSQueue ()
            Processes messages in the send queue sQueue. Calls sendMessage.
            Queues non-acknowledgement sent messages in sentQueue
        logMsg (message,queue='sQueue')
            Logs message to log file
        blockComms()
            sets communication blocking flags
        unBlockComms()
            resets communication blocking flags
        setValue (value)
            sets z to value
        getValue()
            returns z
        setX(value)
            sets x to value
        getX()
            returns x
        setStatus (status)
            sets status to boolean status
        getStatus ()
            returns status
        getPathCount ()
            returns path count
        process (loop_iter)
            node processing entry point
        setDiscoveryMode()
            sets discoveryMode True
        resetDiscoveryMode()
            sets discoveryMode False
    """    
    
    def __init__(self, nodeID, value, neighborNodes, socketNums):
        
        self.z = value
        self.zlast = value
        self.x = value
        self.rQueue = queue.LifoQueue()
        self.sQueue = queue.LifoQueue()
        self.cQueue = queue.LifoQueue()
        self.sentQueue = queue.Queue()
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
        self.socketNums = socketNums
    
        
        Thread.__init__(self)
        
        global rQueue
        global nodeCount
        
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
            
    def processCQueue(self):
        while cQueue[self.nodeID].qsize() > 0:
            cMessage = cQueue[self.nodeID].get()
            if cMessage.toNode == self.nodeID:
                if cMessage.msgType == MsgType.DisablePath:
                    self.setPathStatus(cMessage.var1,cMessage.var2, False)
                elif cMessage.msgType == MsgType.EnablePath:
                    self.setPathStatus(cMessage.var1,cMessage.var2, True)


    def processRQueue(self):
        # count = 0
        
        # acquire lock; pull my rQueue messages into self.rQueue

        while rQueue[self.nodeID].qsize() > 0:
            self.rQueue.put(rQueue[self.nodeID].get())
        
        while self.rQueue.qsize() > 0:
            # count = count+1
            message = self.rQueue.get()
            if message.fromNode not in self.neighborNodes:
                self.logMsg (message, queue='rQueue',level='warning')
            else:
                self.logMsg (message,queue = 'rQueue')

            """ note the next statement - a message on a broken path is removed from
            the rQueue at the start of the loop but not acted upon.  
            It's the same as cutting the path.  Since the same thing happens on 
            the other end, it won't receive udpates or ackowledgements either """
            

#            if self.findPath(self.nodeID,message.fromNode).status == True:
                # self.neighborStatus[self.neighborDict.get(message.fromNode)] = False    
            if message.msgType == MsgType.GenAck:
                self.processAckMsg(message)               
            elif message.msgType == MsgType.Delta:
                self.sendAck(message)
                self.recvDelta(message)
            elif message.msgType == MsgType.Value:
                self.sendAck(message)
                self.recvValue(message)
            # else:
            #     self.deltaij[self.neighborDict.get(message.fromNode)] = 0
            #     self.calcX()
                    
                              
    def processAckMsg(self, message):
#        self.sentQueue.delEntry(message.id)
        if message.id in self.sentDict:
            del self.sentDict[message.id]
                    
            
    def findPath(self,fromNode,toNode):
        # breakpoint()
        for path in self.neighborPaths:
            if fromNode == path.fromNode and toNode == path.toNode:
                return path
            elif toNode == path.fromNode and fromNode == path.toNode:
                return path
        return False

    def sendAck(self,message):
        messageAck = nodeMessage(MsgType.GenAck,self.nodeID,message.fromNode)
        messageAck.id = message.id
        self.sQueue.put(messageAck)
        if messageAck.toNode not in self.neighborNodes:
            self.logMsg(messageAck, level='warning')
        else:
            self.logMsg(messageAck)
        
        
    def sendData (self):
        # breakpoint()
        for node in self.neighborNodes:
            nodeIdx = self.neighborDict.get(node)
            if node > self.nodeID:
                messageSend = nodeMessage(MsgType.Value,self.nodeID,node)
                messageSend.value  =  self.x
            else:
                messageSend = nodeMessage(MsgType.Delta,self.nodeID,node)
                messageSend.delta = self.delta[nodeIdx]
            messageSend.sequence = self.sequence[nodeIdx]
            messageSend.density = self.density
            self.sQueue.put(messageSend)
            if messageSend.toNode not in self.neighborNodes:
                self.logMsg(messageSend,level='warning')
            else:
                self.logMsg(messageSend)

    def calcX (self):
        self.weightedDeltaij = np.multiply(self.weight, self.deltaij)
        self.x = self.z + (self.z - self.zlast) + np.sum(self.weightedDeltaij)
        self.zlast = self.z
        nodeVals[self.nodeID] = self.x

    def recvValue (self,message):
    # value is state of fromNode
        for node in self.neighborNodes:
            if node == message.fromNode:
                nodeIdx = self.neighborDict.get(node)
                if self.sequence[nodeIdx] == (message.sequence -1):    

                    self.delta[nodeIdx] = message.value - self.x      
                    self.deltaij[nodeIdx] += self.delta[nodeIdx]
                    self.weight[nodeIdx] = 2./(self.density + message.density +2.)
                    # self.weightedDeltaij[nodeIdx] = self.weight[nodeIdx] * self.deltaij[nodeIdx]
                    # self.x = self.z + (self.z - self.zlast) + np.sum(self.weightedDeltaij)
                    # self.zlast = self.z
                    self.calcX()
                    self.sequence[nodeIdx] += 1
                    self.neighborDensity[nodeIdx] = message.density

    def recvDelta (self,message):       
        # value is delta of fromNode
        for node in self.neighborNodes:

            if node == message.fromNode:
                nodeIdx = self.neighborDict.get(node)
                if message.sequence == self.sequence[nodeIdx]:
                    self.delta[nodeIdx] =  -message.delta
                    self.deltaij[nodeIdx] += self.delta[nodeIdx]
                    self.weight[nodeIdx] = 2./(self.density + message.density + 2.)
                    # self.weightedDeltaij[nodeIdx] = self.weight[nodeIdx] * self.deltaij[nodeIdx]
                    # self.x = self.z + (self.z - self.zlast) + np.sum(self.weightedDeltaij)
                    # self.zlast = self.z
                    self.calcX()
                    self.sequence[nodeIdx] += 1
                    self.neighborDensity[nodeIdx] = message.density
                            
    
    def checkSent(self):
        if np.all(self.neighborDensity):
            keys = self.sentDict.keys()
            for key in keys:
                message, count = self.sentDict[key]
                # if count > 3*np.sum(self.neighborDensity):
                #     # print (f'Count: {count} {message.msgStr()}')
                #     node = message.toNode
                #     nodeIdx = self.neighborDict.get(node)
                #     self.neighborStatus[nodeIdx] = False
                #     self.deltaij[nodeIdx] = 0;
                #     self.calcX()
                count += 1
                self.sentDict.update({message.id: (message, count)})

    """ Note that sendMessage directly manipulates the to node's rQueue.  
        It does so through the global nodeList.  In a parallel scheme, the global
        nodeList won't be available and sendMessage will have to use TCP/IP comms
    """               
    def sendMessage(self, nodeMessage):
#        print (f'Node: {self.nodeID} would send to node: {nodeMessage.toNode}')
        # pass
        if not self.sendBlock:
            toNode = nodeMessage.toNode
            rQueue[toNode].put(nodeMessage)
            if toNode not in self.neighborNodes:
                if not self.firstSendErr:
#                self.first = True
                    print (f'Node: {self.nodeID} send message to: {nodeMessage.toNode}')
                    self.firstSendErr = True

    def recvMessage(self, nodeMessage):
        if not self.recvBlock:
            pass
                               
    def processSQueue(self):
        if not self.sendBlock:
            while self.sQueue.qsize() > 0:
                message = self.sQueue.get()
                if message.msgType !=MsgType.GenAck:
                    self.sentQueue.put(message)
                    self.sentDict.update({message.id: (message, 0)})
                self.sendMessage(message)            

                     
    def logMsg (self,message,queue='sQueue',level='info'):
        # print (f'Node: {self.nodeID}: Queue: ', queue,message.msgStr(),
        #        file=log_out,flush=True)
        if level == 'info':
            logging.info(f'Node: {self.nodeID}: Queue: {queue}: {message.msgStr()}')
        elif level == 'warning':
            logging.warning(f'Node: {self.nodeID}: Queue: {queue}: {message.msgStr()}')
            
        
    def setPathStatus (self,fromNode,toNode,state):
        path = self.findPath (fromNode, toNode)
        if path:
            path.setStatus(state)
            message = path.pathStr()
            print (f'Node: {self.nodeID} {message}')
        
    def blockComms (self):
        self.sendBlock = True
        self.recvBlock = True
        
    def unBlockComms (self):
        self.sendBlock = False
        self.recvBlock = False    

    def setValue(self, value):
        self.z = value
        
    def getValue (self):
        return self.zno
    
    def setX (self, value):
        self.x = value
        
    def getX (self):
        return self.x
                
    def setStatus(self, status):
        self.status = status
        
    def getStatus (self):
        return self.status
           
    def run(self):
        while True:
            self.processCQueue()
            if nodeRun[self.nodeID]:
                if self.status == True:   
                    nodeCount[self.nodeID] += 1
                    self.processRQueue()
                    self.checkSent()
                    self.sendData()
                    self.processSQueue()
            elif nodeStop[self.nodeID]:
                break
            time.sleep(0.0005)
    
   # end class Node

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
    
    baseSocket = 49152
    nNode = mpc.bus.shape[0]
    S = np.zeros((nNode,nNode),dtype=int)
    for i in range(nNode):
        for j in range(nNode):
            S[i,j] = -1
    nBranch = mpc.branch.shape[0]
    for i in range(nBranch):
        from_bus=mpc.branch[i,0].astype(int)
        to_bus = mpc.branch[i,1].astype(int)
        S[from_bus,to_bus] = baseSocket + i
        S[to_bus,from_bus] = baseSocket + i
    return S

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
    
def findPath (nodeFrom,nodeTo):
    for path in pathList:
        if (path.fromNode == nodeFrom) and (path.toNode == nodeTo):
            return path
        elif (path.toNode == nodeFrom) and (path.fromNode == nodeTo):
            return path
    return False

def findNode (nodeID):
    for nodeCell in nodeList:
        if nodeCell.nodeID == nodeID:
            return nodeCell
    return False
    

"""
    Main Entry Point
"""    
if __name__ == '__main__':
    
    # startup manager process to manage node instances
    
    logging.basicConfig(filename='DistAvg7.log', filemode='w', level=logging.INFO, 
                        force=True)
#    log_out = open('output.txt','w')
    lock = Lock()
    
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
    S = buildSocket(mpc)
    
    # create network graph
    G = buildGraph(busDict, lineList)
    
    # build list of node names
    nodeName = ['Node'+str(i) for i in range(nNode)]
    

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
        rQueue.append(queue.LifoQueue())
        cQueue.append(queue.LifoQueue())
        
    nodeList = []
    threadList = []
    nodeCount = []

    for i in range(nNode):
        # nodeCell = Node(i,random.random())
        neighborList = [j for j in range(nNode) if C[i,j]]
        socketNums = [S[i,j] for j in range(nNode) if S[i,j] > 0]
        nodeCount.append(0)

        try:
            th = Node(i, float(i+1), neighborList, socketNums)
        except:
            print ('Error instantiating thread')
            exit()
        threadList.append(th)
        
    # create the pathList
    pathList = []
    for (f,t) in lineList:
        fromNode = findNode(f)
        toNode = findNode(t)
        pathList.append(path(fromNode, toNode))
    
    
    # seed averages array
    nodeRun = np.zeros((nNode,1),dtype=int)
#    nodeCount = np.zeros((nNode,1),dtype=int)
    max_iter = 27000
    averages = np.zeros((max_iter+1)*nNode).reshape(max_iter+1,nNode)
    x = np.zeros((nNode,),dtype=float)
    sumAvg = 0
    # for i in range(len(nodeList)):
    #     averages[0,i] = nodeList[i].getValue()
    #     sumAvg += nodeList[i].getValue()
    # average = sumAvg/nNode
    # print (f'actual average is {average}')
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
    lock = Lock()
    for i in range(nNode):
        curValue[i] = random.random()
#        nodeRun[i] = True
        try:
            threadList[i].start()
        except:
            print (f'Error starting node {i}')
            exit()
    for i in range(nNode):
        # rQueue[i].resetQueue()
        nodeRun[i] = 1
        
    for m in range (10000):
    # while abs(np.max(curValue) - np.min(curValue)) > 1e-4: 
        
        # if numSamples == 300:
        #     msg1 = cmdMessage (msgType=MsgType.DisablePath,toNode=4,var1=4,var2=5)
        #     msg2 = cmdMessage (msgType=MsgType.DisablePath,toNode=5,var1=4,var2=5)
        #     for aqueue in cQueue:
        #         aqueue.put(msg1)
        #         aqueue.put(msg2)
        
        # if np.average(counts) == 75:
        #      nodeList[8].setStatus(False)
        #      for node in nodeList:
        #          node.setPathStatus (4, 5, False)
            
        # if np.average(counts) == 200:
        #     nodeList[8].setStatus(True)
        #     for node in nodeList:
        #          node.setPathStatus (4, 5, True)
        # # if np.average(counts) > 250
        #     k = 1.0
        #     for j in range (nNode,0,-1):
        #         nodeList[j-1].setValue(k)
        #         k += 1.0
        if (loop_iter % nNode) == 0:
            averages[numSamples,0:nNode] = nodeVals
            numSamples += 1
        
                
        loop_iter += 1
        if  np.max(counts) > max_iter - 1:
            break
        time.sleep (0.001)
        
    for i in range(nNode):
        nodeRun[i] = False
        nodeStop[i] = True
        
    for th in threadList:
        th.join()
    
            
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
#    log_out.close()
            