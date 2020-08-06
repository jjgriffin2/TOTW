#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 15 10:12:14 2020

@author: jamesgriffin
"""

import numpy as np
import scipy.io as sp
import networkx as nx
import uuid
import datetime
import random
import matplotlib.pyplot as plt
import pdb

import sys


# define class for queing/dequeing.  The queue is positional
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
        peekQueue()
            returns item at head of queue without deleting it
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

    #Queueing elements
    def enqueue(self,message):
        #Checking if the queue is full

        self.queue.append(message)
        self.tail += 1
        return True     
    
    # Deleting a specific element (message)
    def delEntry(self, itemID):
        if self.size() <= 0:
            self.resetQueue()
            return False

        for i in range (self.head, self.tail):
            message = self.queue[i] 
            if message.id == itemID:
                del self.queue[i]
                if i == self.head:
                    self.head += 1
                else:
                    self.tail -= 1 
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
        return message
                
    #Calculate size
    def size(self):
        return self.tail - self.head
    
    #Peek at queue - pull the item from a queue without dequeueing it    
    def peekQueue(self):
        if self.size() <= 0:
            return False
        else:
            message = self.queue[self.head]
            return message
        
    #Reset queue
    def resetQueue(self):
        self.tail = 0
        self.head = 0
        self.queue = list()
    
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
        for message in self.queue[self.head : self.tail+1]:
            data.append(message)
        return data
    #end class Queue


class nodeMessage:
    """ Class nodeMessage - structure of message from one node to another
    
    Calling:
        obj = nodeMessage (type, fromNoe, toNode, path, value, unAckCt)
    
    Attributes:
        msgType : int
            defines the type of the message content
        fromNode : Node
            sending node
        toNode : Node
            receiving node
        path : list [Node Node ...}
            path the node takes
        value : float
            value to send
        id : uuid
            automatically allocated uuid
        unAckCt : int
            unacknowledged pass count
        
    Methods:
        printMessage()
            print message to stdout
        incUnAckCt()
            increment unacknowledged cout
        getUnAckCt()
            returns unancknowledged count
        
    
    """
    # messages are defined as:
    # Type 1 - acknowledge
    # Type 2 - connectivity
    # Type 3 - data passage
    def __init__(self, msgType, fromNode, toNode, path = [], value = 0,
                 density = 0, delay = 0):
        
        self.msgType = msgType    #1 = acknowledge        
        self.fromNode = fromNode
        self.toNode = toNode
        self.path = path
        self.value = value
        self.id = uuid.uuid1()
        self.density = density
        self.delay = delay

     
    # Method to print the contents of a message    
    def printMessage (self):
        print (f'Message type {self.msgType} from: {self.fromNode} to: {self.toNode}' +
               f' path: {self.path} value: {self.value} density: {self.density}' +
               f' delay: {self.delay} time stamp: {date_time(self.id)}')
    
    def incUnAckCt(self):
        self.unAckCt += 1
        
    def getUnAckCt(self):
        return self.unAckCt 

    #end class nodeMessage   
        
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
        printPath()
            prints path nodeIDs and status to stdout
                      
    """
    
    
    def __init__(self, fromNode, toNode, status=True):
        if fromNode.nodeID < toNode.nodeID:
            self.fromNode = fromNode
            self.toNode = toNode
        else:
            self.fromNode = toNode
            self.toNode = fromNode
            
        self.status = status
        
    def setStatus (self,status):
        self.status = status
        
    def getStatus (self):
        return self.sptatus
    
    def getPathTuple (self):
        return (self.fromNode, self.toNode)
        
    def printPath (self):
        print (f'From: {self.fromNode.nodeID}  To: {self.toNode.nodeID} ' +
               f'  Status: {self.status}')
    #end class path

#@ray.remote
class Node(object):
  
    # Node class
    #   Value:          value the node is carrying
    #   nodeID:        node number
    #   rQueue:         receive queue
    #   sQueue:         send queue
    #   neighborNodes:   Neighbor node list (initial neighbors)
    #   W:              metropolitan matrix
    
    
    def __init__(self, nodeID, value):
        self.z = value
        self.deltaZ = 0
        self.x = value
        self.pastx = value
        self.rQueue = Queue()
        self.sQueue = Queue()
        self.sentQueue = Queue()
        self.nodeID = nodeID
        self.neighborNodes = []
        self.connectPaths = []
        self.neighborPaths = []
        self.sendBlock = False
        self.recvBlock = False
        self.status = True
        self.pathCount = 0
        self.nodeCount = 0
        self.sameTicks = 0
        self.C = None
        self.W = None
        self.D = None
        self.neighborDensity = None
        self.density = 0
        self.neighborX = None
        self.delta = None
        self.dDelta = None
        self.gotUpdate = None
        self.nNeighbors = 0
        self.phi = 0.25
        self.gamma = None
        self.deltaUpdateBlock = None
        self.flipFlop = False
        self.doDiscovery = False

        
       
    def setNeighborNodes(self,neighborNodes,doDiscovery):
        self.neighborNodes = neighborNodes
        for nodeCell in self.neighborNodes:
            if self.nodeID < nodeCell.nodeID:       
                nodeTuple = (self ,nodeCell)
            else:
                nodeTuple = (nodeCell, self)
            path = findPath(nodeTuple,pathList)
            self.connectPaths.append(path)
        self.nNeighbors = len(self.connectPaths)
        self.neighborX = np.zeros((self.nNeighbors,),dtype=float)
        self.delta = np.zeros((self.nNeighbors,),dtype=float)
        self.dDelta = np.zeros((self.nNeighbors,),dtype=float)
        self.gamma = np.zeros((self.nNeighbors,),dtype=float)
        self.gotUpdate = np.zeros((self.nNeighbors,),dtype=int)
        self.deltaUpdateBlock = np.zeros((self.nNeighbors,1),dtype=int)
        self.neighborDensity = np.zeros((self.nNeighbors,1),dtype=int)
        self.density = self.nNeighbors


        # self.C = np.zeros((self.nNeighbors,self.nNeighbors),dtype=int)
        # self.W = np.zeros((self.nNeighbors,self.nNeighbors),dtype=float)
        # self.buildC()
        # self.buildD()
        # self.buildW()
        # self.doDiscovery = doDiscovery
        
        # if self.doDiscovery:
        #     self.sendConnectPaths()
                
    def connectPathExists(self,path):
        if path in self.connectPaths:
            return True
        else:
            return False
                   
    def blockComms (self):
        self.sendBlock = True
        self.recvBlock = True
        
    def unBlockComms (self):
        self.sendBlock = False
        self.recvBlock = False
    
    def processRQueue(self):
        while self.rQueue.size() > 0:
            message = self.rQueue.dequeue()
            if message.msgType == 1:        # acknowledge message
                self.processAckMsg(message)
            elif message.msgType == 2:      # process discovery message
                self.processDiscoverMsg(message)
            elif message.msgType == 3:      # process x exchange
                self.procRecvXexchange(message)
            elif message.msgType == 4:      # process dDelta message
                self.processDdelta(message)
            elif message.msgType == 5:      # process dDelta message acknowledge
                self.processAckMsg(message)
                self.processRcvdDeltaAck(message)
                
    def processAckMsg(self, message):
        self.sentQueue.delEntry(message.id)

    def processDiscoverMsg (self, message):  
        messageAck = nodeMessage(1,self.nodeID,message.fromNode,[],0)
        self.sQueue.enqueue(messageAck)
        newPaths = []
        for path in message.path:
            if path not in self.connectPaths:
                self.connectPaths.append(path)
                newPaths.append(path)
        if len(newPaths) > 0:
            self.buildC()
            self.buildD()
            self.buildW()
            self.sendNewPaths(newPaths)
            
    def procRecvXexchange(self,message):

        messageAck = nodeMessage(1,self.nodeID,message.fromNode,[],0)
        self.sQueue.enqueue(messageAck)
        # breakpoint()
        nodeFrom = message.fromNode
        for i in range(self.nNeighbors):
            if self.neighborNodes[i].nodeID == nodeFrom:
                self.neighborX[i] = message.value
                self.neighborDensity[i] = message.density
                self.gotUpdate[i] = True
                break
    
    def procSendXexchange(self):
        # breakpoint()
        for i in range (self.nNeighbors):
            self.gotUpdate[i] = False
            node = self.neighborNodes[i]
            message = nodeMessage(3, self.nodeID, node.nodeID, path=[], 
                                  value =self.pastx, density =self.density,
                                  delay = 0)
            self.sQueue.enqueue(message)
        self.processSQueue()
                                
    
    def processSQueue(self):
        if not self.sendBlock:
            while self.sQueue.size() > 0:
                message = self.sQueue.dequeue()
                self.sentQueue.enqueue(message)
                self.sendMessage(message)
        
    # send a message.  This might, for instance, be a command to send a 
    # no-wait TCP/IP message.  In this context, it sends to the message
    # to the receiver's receive queue, unless blocked.
    
    def sendMessage(self, nodeMessage):
        if not self.sendBlock:
            toNode = nodeMessage.toNode
            nodeList[toNode].rQueue.enqueue(nodeMessage)
        
    
    # receive a message.  This might, for instance, be a command to establish
    # a no-wait TCP/IP receive.  For this program, deal directly with the
    # receive queue.
    def recvMessage(self, nodeMessage):
        if not self.recvBlock:
            pass


    def sendConnectPaths(self):
        for cellNode in self.neighborNodes:
            message = nodeMessage(2, self.nodeID, cellNode.nodeID,
                                  self.connectPaths,0)
            self.sQueue.enqueue(message)

    
    def sendNewPaths(self, newPaths):
        for cellNode in self.neighborNodes:
            message = nodeMessage(2, self.nodeID, cellNode.nodeID,
                                  newPaths, 0)
            self.sQueue.enqueue(message)
                                    
    def clearConnectPaths(self):
        self.connectPaths.clear()
        for nodeCell in self.neighborNodes:
            nodeTuple = (min(self.nodeID, nodeCell.nodeID),
                         max(self.nodeID, nodeCell.nodeID))
            path = findPath(nodeTuple,pathList)
            self.connectPaths.append(path)
            
    def setValue(self, value):
        self.z = value
        
    def setx (self, value):
        self.x = value
        
    def getValue (self):
        return self.z
    
    def getx (self):
        return self.x
                
    def getPastx (self):
        return self.pastx
    
    def setPastx (self,value):
        self.pastx = value
        
    def setStatus(self, status):
        self.status = status
        
    def getStatus (self):
        return self.status
    
    def getPathCount (self):
        return len(self.connectPaths)
            
    def buildC (self):
        newSize = 0
        for path in self.connectPaths:
            fromNode, toNode = path.getPathTuple()
            maxDim = max(fromNode.nodeID, toNode.nodeID)
            newSize = max(newSize,maxDim)            
        newSize += 1
        
        self.C = np.zeros((newSize,newSize),dtype=int)
        for path in self.connectPaths:
            fromNode, toNode = path.getPathTuple()
            self.C[fromNode.nodeID, toNode.nodeID] = 1
            self.C[toNode.nodeID, fromNode.nodeID] = 1
        
    def buildD (self):
        nNode = self.C.shape[0]
        self.D = np.zeros((nNode,),dtype=int)
        for i in range(nNode):
            self.D[i] = np.sum(self.C[:,i],axis=0)

    def buildW (self):
        nNode = self.C.shape[0]            
        self.W = np.zeros((nNode, nNode),dtype=float)

        for i in range (nNode):
            for j in range (nNode):
                if self.C[i,j]:
                    self.W[i,j] = 2/(self.D[i] + self.D[j]+2)

        for ii in range(nNode):
            self.W[ii,ii] = 1-np.sum(self.W[ii,:],axis=0)
        
    
    def processValue(self):
        # self.density = len(self.neighborNodes)
        # x = self.pastx
        # x = x * self.W[self.nodeID,self.nodeID]
        # for node in self.neighborNodes:
        #     x += node.pastx * self.W[self.nodeID, node.nodeID]
        # self.x = x
        # breakpoint()
        # if not np.all(self.gotUpdate):
        #     print (f'Node: {self.nodeID}, gotUpdate: {self.gotUpdate}')
        # else:
        #     print (f'Node: {self.nodeID}, x: {self.x}, pastx: {self.pastx}')
        #     for i in range(self.nNeighbors):
        #         node = self.neighborNodes[i]
        #         print (f'\tNeighbor: {node.nodeID}, x: {node.x}, pastx: {node.pastx}')
        #         print (f'\t\tneighborX: {self.neighborX[i]}')
        for i in range(self.nNeighbors):
            node = self.neighborNodes[i]
            # nodeNo = node.nodeID
            self.dDelta[i] = self.W[self.nodeID,node.nodeID] * \
                (self.pastx - node.pastx)
            self.delta[i] += self.dDelta[i]
        
        self.x = self.z - np.sum(self.delta)
        
            

    def replacex(self):
        self.pastx = self.x
    
    def getC (self):
        return self.C
    
    def setC (self,C):
        self.C = C
        self.buildD()
        
    def setW (self,W):
        self.W = W
    
    def getW(self):
        return self.W
    
    def updateX(self):
        self.x += self.gamma * (sum(self.delta) + self.z
                                             - self.x)
                                             
    def updateDelta(self,index):
        if self.deltaUpdateBlock[index] == True:
            return
        past = self.delta[index]
        self.delta[index] += self.phi * (self.neighborX[index] - self.x)
        self.dDelta[index] = self.delta[index] - past
        
        

    def senddDelta (self,index):
        if self.deltaUpdateBlock[index] == True:
            return
        message = nodeMessage(4, self.nodeID, self.neighborNodes[index].nodeID, 
                              [], self.dDelta[index])
        self.sQueue.enqueue(message)
        self.deltaUpdateBlock[index] = True
        
    def processRcvdDeltaAck (self,message):
        sender = message.fromNode
        for i in range(len(self.neighborNodes)):
            if self.neighborNodes[i].nodeID == sender:
                self.deltaUpdateBlock[i] = False
                
        
    
    def process(self,discoveryMode):
        #pdb.set_trace()
        if self.status == True:
            # send my X value to neighbors.  
            self.procSendXexchange()
            # process messages in receive queue
            self.processRQueue()

#             # process values
#             if not discoveryMode:
#                 if not self.doDiscovery:
# #                    breakpoint()
#                     # print (f'node {self.nodeID} in process',flush=True)
#                     if not self.flipFlop:
#                         self.flipFlop = True
#                         self.updateX()
#                         self.sendUpdateX()
#                     else:
#                         self.flipFlop = False
#                         for i in range(self.nNeighbors):
#                             self.updateDelta(i)
#                             self.senddDelta(i)
#                     pass
#                 else:
# #                    pass
#                     self.processValue()
            
            self.processValue()   
            # process messages in send queue
            self.processSQueue()
    # end class Node

def buildConnection (mpc, pathList):
    
    # build the connectivity matrix
    
    nNode = mpc.bus.shape[0]
    from_bus = mpc.branch[:,0].astype(int)
    to_bus = mpc.branch[:,1].astype(int)
    C = np.zeros((nNode,nNode),dtype=int)
    C[from_bus, to_bus] = 1
    C = C + C.transpose()
    return C
    #end def buildConnection
    
def buildDegreeMatrix (nNode, C):
    D = np.zeros((nNode,nNode),dtype=int)
    for i in range(nNode):
        D[i,i] = np.sum(C[:,i],axis=0)
    return D

def buildDegreeVector (nNode, C):
    D = np.zeros((nNode,),dtype=int)
    for i in range(nNode):
        D[i] = np.sum(C[:,i],axis=0)
    return D

def buildLaplace (nNode, C, D):
    return D - C
    def buildW (self):
        nNode = self.C.shape[0]            
        self.W = np.zeros((nNode, nNode),dtype=float)

       
def buildMetropolis (nNode, C, D):
    
    # build the doubly-stochastic metropolis matrix using Sheilesh's method.
    

    W = np.zeros([nNode, nNode])
    
    for i in range (nNode):
        for j in range (nNode):
            if C[i,j]:
                W[i,j] = 2/(D[i] + D[j] + 2)
                

    for ii in range(nNode):
        W[ii,ii] = 1-np.sum(W[ii,:],axis=0)
        
    return W
    # end def buildCommunity
    
def buildGraph (busDict, lineList):
    
    # create network graph
    plt.figure()
    G = nx.Graph()
    G.add_nodes_from(busDict.values())
    G.add_edges_from(lineList)
    nx.draw(G,with_labels=True)
    return G
    # end def buildGraph
    
def buildDGraph (busDict, dLineList):
    # create directed network graph
    plt.figure()
    G = nx.DiGraph()
    G.add_nodes_from(busDict.values())
    G.add_edges_from(dLineList)
    nx.draw(G,with_labels=True)
    return G

# function date_time returns datetime from a uuid.  It is based on how
# python implements uuid.
def date_time (uuid_in):    
    return datetime.datetime.fromtimestamp((uuid_in.time - 
                                            0x01b21dd213814000)*100/1e9)
     #end date_time
    
def findPath (pathToFind,pathList):
    for path in pathList:
        if (path.fromNode, path.toNode) == pathToFind:
            return path
    return False

def findNode (nodeID):
    for nodeCell in nodeList:
        if nodeCell.nodeID == nodeID:
            return nodeCell
    return False

def resetNodeValues (nodeList):
    for node in nodeList:
        node.setx(node.getValue())
        node.setPastx(node.getValue())
    
def checkNeighbors (nodeList, numTuples):
    for i in range(len(nodeList)):
        if nodeList[i].getPathCount() != numTuples[i]:
            numTuples[i] = nodeList[i].getPathCount()
            return True
    return False
    
    
if __name__ == '__main__':
    
    orig_stdout  = sys.stdout
    # sys.stdout = open ('output.txt','w') 

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
    C = buildConnection(mpc,pathList)
    
    # create network graph
    G = buildGraph(busDict, lineList)
    
    # create directed network graph
    H = buildDGraph(busDict,dLineList)
    
    # build the degree matrix
    DMatrix = buildDegreeMatrix(nNode, C)
    
    # build the degree vector
    DVector = buildDegreeVector(nNode, C)
    
    # build the metropolix matrix
    W = buildMetropolis(nNode, C, DVector)
       
    # build the Laplace matrix
    L = buildLaplace(nNode, C, DMatrix)
    #L = (np.identity(nNode) - W) * C
    
    # Create the nodes, building a list of the nodes
    nodeList = []
    for i in range(nNode):
        # nodeCell = Node(i,random.random())
        nodeCell = Node(i,float(i+1))
        nodeList.append(nodeCell)    
        
    # create the pathList
    pathList = []
    for (f,t) in lineList:
        fromNode = findNode(f)
        toNode = findNode(t)
        pathList.append(path(fromNode, toNode))

    # send each node a list of its neighbors
    doDiscovery = True
    for i in range(nNode):
        neighborList = [nodeList[j] for j in range(nNode) if C[i,j]]        
        nodeList[i].setNeighborNodes(neighborList,doDiscovery)
        
    # send each node the metropolitan and adjancey matrices
    for node in nodeList:
        node.setC(C)
        node.setW(W)
    
    # seed numTuples and tuplesChanged  
    numTuples = [0]*nNode
    
    # seed averages array
    max_iter = 2999
    averages = np.zeros((max_iter+1)*nNode).reshape(max_iter+1,nNode)
    pr_averages = np.zeros((max_iter+1)*nNode).reshape(max_iter+1,nNode)
    x = np.zeros((nNode,),dtype=float)
    sumAvg = 0
    for i in range(len(nodeList)):
        averages[0,i] = nodeList[i].getValue()
        sumAvg += nodeList[i].getValue()
    average = sumAvg/nNode
    print (f'actual average is {average}')
    loop_iter = 1

    presentDiscoveryMode = True
    pastDiscoveryMode = False
    
    for node in nodeList:
        print (f'Node: {node.nodeID}, z: {node.z}, x: {node.x}, pastx: {node.pastx}')

    while abs(np.max(averages[loop_iter-1,:]) - 
              np.min(averages[loop_iter-1,:])) > 1e-4:
#    for i in range(100):
        # if doDiscovery:
        #     for node in nodeList:
        #         node.replacex()
        
        # if doDiscovery:    
        #     for i in range(len(nodeList)):
        #         nodeList[i].process(presentDiscoveryMode)
        #         pr_averages[loop_iter,i] = nodeList[i].getPastx()
        #         averages[loop_iter,i] = nodeList[i].getx()
       
        #     if loop_iter % 5:
        #         presentDiscoveryMode = checkNeighbors(nodeList, numTuples)
        #         if pastDiscoveryMode and not presentDiscoveryMode:
        #             print (f'Discovery finished at iter {loop_iter}, resetting values')
        #             resetNodeValues(nodeList)
        #             pastDiscoveryMode = presentDiscoveryMode       
        
        # else:
        #     for i in range(len(nodeList)):
        #         nodeList[i].process(0)
        #         averages[loop_iter,i] = nodeList[i].getx()
        for node in nodeList:
            node.replacex()
            
   
        for j in range(len(nodeList)):
            nodeList[j].process(False)
            pr_averages[loop_iter,j] = nodeList[j].getPastx()
            averages[loop_iter,j] = nodeList[j].getx()

            
        loop_iter += 1
        if loop_iter > max_iter:
            break
    
       
    print (f'Consensus reached in {loop_iter} iterations')    
    for nodeCell in nodeList:
        print (f'Node {nodeCell.nodeID} has consensus value ' +
              f'{nodeCell.x}')
    
    plt.figure()
    plt.plot(averages[0:loop_iter-1,0:nNode-1])
    plt.title("Average Convergence")
    plt.xlabel("Iterations")
    plt.ylabel("Average")
    # sys.stdout = orig_stdout
            
    
