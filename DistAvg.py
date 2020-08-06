#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May  2 16:50:23 2020

@author: jamesgriffin
"""

import numpy as np
import scipy.io as sp
import networkx as nx
import uuid
import datetime
import random
import matplotlib.pyplot as plt
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
                 unAckCt = 0):
        
        self.msgType = msgType    #1 = acknowledge        
        self.fromNode = fromNode
        self.toNode = toNode
        self.path = path
        self.value = value
        self.id = uuid.uuid1()
        self.unAckCt = unAckCt 

     
    # Method to print the contents of a message    
    def printMessage (self):
        print (f'Message type {self.type} from: {self.fromNode} to: {self.toNode}' +
               f' path: {self.path} time stamp: {date_time(self.id)}')
    
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
    """ class Node - where the action takes place
    
    Class node is the largest class, containing a process routine from which
    everything else followw.  In the sense of this program, a node is a system bus,
    it could be anything described as a vertex in a graph
    
    Attributes:
        value : float
            node value
        consensusValue :
            node view of system average value
        rQueue : Queue
            node receive queue
        sQueue : Queue
            node send queue
        sentQueue : Queue
            unacknowledge messages sent by the node.  A message starts in sQueue,
            is dequeued and appended to sentQueue when it's actually sent, and deleted
            from sentQueue upon acknowledgement
        nodeID : int
            node numerical id 
        neigborNodes : list [node, node, ...]
            list of nodes to which this node is directly connected
        neighborPaths: list [path, path, ...]
            list of paths to which this node is directly connected
        connectPaths: list [path, path ...]
            list of all discovered paths in the network
        sendBlock : bool
            boolean to block sending messages
        receiveBlock : bool
            boolean to block receiving messages
        status: bool
            boolean to disable node 
            
    Methods:
        setNeighborNodes([node, node ...])
            sets neighborNodes attribute, finds paths between self and neighbor nodes,
            appends paths to connectPaths, and sends connectPaths to immediate neighbors
        connectPathExists(path)
            returns true if path is in connectPaths, otherwise returns false
        blockComms()
            sets sendBlock, recvBlock attributes true
        unBlockComms()
            sets sendBlock, recvBlock attributes false
        processRQueue()
            processes the receive queue.  
                - If message is an acknowledgement, deletes associated message from sentQueue.
                - If message is any thing else, queues an acknowledgement.
                - If message is a path discovered message (type 2), adds path to connectPath attribute
                  if it didn't already exist, and sends path discovered message to immediate
                  neighbors
        processSQueue()
            processes the send queue.
                - Dequeues message from sQueue and enqueues it in the sentQueue
                - calls sendMessage
        sendMessage(message)
            enqueues the message in the receiving node's rQueue
        recvMessage(message)
            currently does nothing.  Functions from processRQueue to be moved here
        sendConnectPaths()
            enqueues path discovery message containing connectPaths attribute 
            for each immediate neighbor in sQueue
        sendNewPaths([path, path ...])
            enqueues path discovery message containing [path, path ...] argument for
            each immediate neighbor in sQueue
        clearConnectPaths()
            resets connectPaths to contain only the paths of immediate neighbors
        setValue(value)
            sets value attribute to value
        getValue()
            returns value attribute
        setConsensusValue(value)
            sets perceived consensus value
        getConsensusValue()
            returns perceived consensus value
        setStatus (bool)
            sets node status attribute.  a value of false effectively turns off the node
        getStatus()
            returns node status attribute
        getPathCount()
            returns the number of entries in connectPaths attribute
        process()
            if status attribute is true, calls processRQueue() and processSqueue()
        
        
        
            
    """
    # Node class
    #   Value:          value the node is carrying
    #   nodeID:        node number
    #   rQueue:         receive queue
    #   sQueue:         send queue
    #   neighborNodes:   Neighbor node list (initial neighbors)
    #   W:              metropolitan matrix
    
    
    def __init__(self, nodeID, value):
        self.value = value
        self.consensusValue = value
        self.pastConsensusValue = value
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
        self.density = 0
        
       
    def setNeighborNodes(self,neighborNodes):
        self.neighborNodes = neighborNodes
        for nodeCell in self.neighborNodes:
            if self.nodeID < nodeCell.nodeID:       
                nodeTuple = (self ,nodeCell)
            else:
                nodeTuple = (nodeCell, self)
            path = findPath(nodeTuple,pathList)
            self.connectPaths.append(path)
        nNeighbors = len(self.connectPaths)
        self.C = np.zeros((nNeighbors,nNeighbors),dtype=int)
        self.W = np.zeros((nNeighbors,nNeighbors),dtype=float)
        self.buildC()
        self.buildD(self.C)
        self.buildW(self.C, self.D)
        self.sendConnectPaths()
                
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
            if message.msgType == 1:
                self.processAckMsg(message)
            elif message.msgType == 2:
                self.processDiscoverMsg(message)
                
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
            self.buildD(self.C)
            self.buildW(self.C,self.D)
            self.sendNewPaths(newPaths)

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
        self.value = value
        
    def setConsensusValue (self, value):
        self.consensusValue = value
        
    def getValue (self):
        return self.value
    
    def getConsensusValue (self):
        return self.consensusValue
                
    def getPastConsensusValue (self):
        return self.pastConsensusValue
    
    def setPastConsensusValue (self,value):
        self.pastConsensusValue = value
        
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
        
    def buildD (self,C):
        nNode = C.shape[0]
        self.D = np.zeros((nNode,),dtype=int)
        for i in range(nNode):
            self.D[i] = np.sum(C[:,i],axis=0)

    def buildW (self,C,D):
        nNode = self.C.shape[0]            
        self.W = np.zeros((nNode, nNode),dtype=float)

        for i in range (nNode):
            for j in range (nNode):
                if C[i,j]:
                    self.W[i,j] = 2.0 / (D[i] + D[j] + 1)

        for ii in range(nNode):
            self.W[ii,ii] = 1-np.sum(self.W[ii,:],axis=0)
        


    def processValue(self):
        self.density = len(self.neighborNodes)
        x = self.pastConsensusValue
        x = x * self.W[self.nodeID,self.nodeID]
        for node in self.neighborNodes:
            x += node.pastConsensusValue * self.W[self.nodeID, node.nodeID]
        self.consensusValue = x

        # x = 0
        # sumDensityFactor = 0
        # for node in self.neighborNodes:
        #     densityFactor = 1./(max(self.density,node.density) + 1)
        #     sumDensityFactor += densityFactor
        #     x += node.consensusValue * densityFactor
        # x += self.consensusValue * (1 - sumDensityFactor)
        # self.consensusValue = x
            
    def replaceConsensusValue(self):
        self.pastConsensusValue = self.consensusValue
    
    def getC (self):
        return self.C
    
    def getW(self):
        return self.W
    
    def process(self,discoveryMode):
        if self.status == True:
            # process messages in receive queue
            self.processRQueue()
            # process messages in send queue
            self.processSQueue()
            # process values
            if not discoveryMode:
                self.processValue()
                
            
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
    
def buildDegree (nNode, C):
    D = np.zeros((nNode,nNode),dtype=int)
    for i in range(nNode):
        D[i,i] = np.sum(C[:,i],axis=0)
    return D

def buildLaplace (nNode, C, D):
    return D - C
       
def buildMetropolis (nNode, C):
    
    # build the doubly-stochastic metropolis matrix using Sheilesh's method.
    
    idx_tuple = np.where(C==1)
    [row,col] = [idx_tuple[0],idx_tuple[1]]

    W = np.zeros([nNode, nNode])
    for idx in range (row.size):
        W[row[idx],col[idx]] = 1/(np.sum(C[row[idx]],axis=0) + 1)


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
        node.setConsensusValue(node.getValue())
        node.setPastConsensusValue(node.getValue())
    
def checkNeighbors (nodeList, numTuples):
    for i in range(len(nodeList)):
        if nodeList[i].getPathCount() != numTuples[i]:
            numTuples[i] = nodeList[i].getPathCount()
            return True
    return False
    
    
if __name__ == '__main__':
    
    # stdout_fd = sys.stdout.fileno()

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
        print (max(fromNode,toNode), min(fromNode,toNode))
        
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
    
    # build the metropolix matrix
    W = buildMetropolis(nNode, C)
    
    # build the degree matrix
    D = buildDegree(nNode, C)
    
    # build the Laplace matrix
    L = buildLaplace(nNode, C, D)
    #L = (np.identity(nNode) - W) * C
    
    # Create the nodes, building a list of the nodes
    nodeList = []
    for i in range(nNode):
        nodeCell = Node(i,random.random())
        # nodeCell = Node(i,float(i))
        nodeList.append(nodeCell)    
        
    # create the pathList
    pathList = []
    for (f,t) in lineList:
        fromNode = findNode(f)
        toNode = findNode(t)
        pathList.append(path(fromNode, toNode))

    # send each node a list of its neighbors
    for i in range(nNode):
        neighborList = [nodeList[j] for j in range(nNode) if C[i,j]]        
        nodeList[i].setNeighborNodes(neighborList)  
    
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
    while abs(np.max(averages[loop_iter-1,:]) - 
              np.min(averages[loop_iter-1,:])) > 1e-4:

        for node in nodeList:
            node.replaceConsensusValue()
            
        for i in range(len(nodeList)):
            nodeList[i].process(presentDiscoveryMode)
            pr_averages[loop_iter,i] = nodeList[i].getPastConsensusValue()
            averages[loop_iter,i] = nodeList[i].getConsensusValue()

        
        if loop_iter % 5:
            presentDiscoveryMode = checkNeighbors(nodeList, numTuples)
            if pastDiscoveryMode and not presentDiscoveryMode:
                print (f'Discovery finished at iter {loop_iter}, resetting values')
                resetNodeValues(nodeList)
            pastDiscoveryMode = presentDiscoveryMode       
        loop_iter += 1
        if loop_iter > max_iter:
            break
    
       
    print (f'Consensus reached in {loop_iter} iterations')    
    for nodeCell in nodeList:
        print (f'Node {nodeCell.nodeID} has consensus value ' +
              f'{nodeCell.consensusValue}')
    
    plt.figure()
    plt.plot(averages[0:loop_iter-1,0:nNode-1])
    plt.title("Average Convergence")
    plt.xlabel("Iterations")
    plt.ylabel("Average")
            
    
