#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 16 08:09:53 2020

@author: jamesgriffin
"""

import numpy as np
import scipy.io as sp
import networkx as nx
import uuid
import datetime
import random
import matplotlib.pyplot as plt
from enum import Enum
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
    #end class Queue

class MsgType(Enum):
    """ Class MsgType - Enum
    GenAck = 1
    Discover = 2
    XUpdate = 3
    DeltaUpdate = 4
    DeltaAck = 5
    """
    GenAck = 1
    Discover = 2
    XUpdate = 3
    DeltaUpdate = 4
    DeltaAck = 5
    Data = 6
    Delta = 7
    Value = 8
    

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
        path : list [Node Node ...}
            path the node takes
        value : float
            value to send
        id : uuid
            automatically allocated uuid
        unAckCt : int
            unacknowledged pass count
        sequence: int
            message sequence
        
    Methods:
        printMessage()
            print message to stdout
        msgStr()
            returns a string with the content of the message
        incUnAckCt()
            increment unacknowledged cout
        getUnAckCt()
            returns unancknowledged count
        
    
    """
    def __init__(self, msgType, fromNode, toNode, path = [], value = 0,
                 delta = 0, density = 0, delay = 0, sequence = 0):
        
        self.msgType = msgType    #1 = acknowledge        
        self.fromNode = fromNode
        self.toNode = toNode
        self.path = path
        self.value = value
        self.delta = delta
        self.id = uuid.uuid1()
        self.density = density
        self.delay = delay
        self.sequence = sequence

     
    # Method to print the contents of a message    
    def printMessage (self,output=sys.stdout):
        print (f'Message type {self.msgType} from: {self.fromNode.nodeID} to: ' +
               f'{self.toNode.nodeID}' +
               f' path: {self.path} value: {self.value} delta: {self.delta} density: {self.density}' +
               f' sequence: {self.sequence}, time stamp: {date_time(self.id)}',
               file=output,flush=True)

    def msgStr (self):
        aString = (f'Message type {self.msgType} from: {self.fromNode.nodeID} to: ' +
               f'{self.toNode.nodeID}' +
               f' path: {self.path} value: {self.value} delta: {self.delta} density: {self.density}' +
               f' sequence: {self.sequence}, time stamp: {date_time(self.id)}')
            
        return aString
    
    def logMessage (self):
        print (self.msgStr(),file=log_out,flush=True)
    
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
        neighborUpdatedX: bool
            Neighbor has updated x
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
    
    def __init__(self, nodeID, value):
        self.z = value
        self.zlast = value
        self.x = value
        self.rQueue = Queue()
        self.sQueue = Queue()
        self.sentQueue = Queue()
        self.nodeID = nodeID
        self.neighborNodes = []
        self.neighborNos = []
        self.neighborDict = {}
        self.connectPaths = []
        self.neighborPaths = []
        self.sendBlock = False
        self.recvBlock = False
        self.status = True
        self.pathCount = 0
        self.density = 0
        self.delta = None
        self.deltaij = None
        self.weightedDeltaij = None
        self.nNeighbors = 0
        self.deltaUpdateBlock = None
        self.neighborUpdatedX = None
        self.flipFlop = True
        self.discoveryMode = False
        self.sequence = None

    def processRQueue(self):
        while self.rQueue.size() > 0:
            message = self.rQueue.dequeue()
            self.logMsg (message,queue = 'rQueue')

            if message.msgType == MsgType.GenAck:        # acknowledge message
                self.processAckMsg(message)               
            elif message.msgType == MsgType.Discover:      # process discovery message
                self.sendAck(message)
                self.processDiscoverMsg(message)
            elif message.msgType == MsgType.Delta:
                self.sendAck(message)
                self.recvDelta(message)
            elif message.msgType == MsgType.Value:
                self.sendAck(message)
                self.recvValue(message)
            else:
                print ('Received unknown message ',message.msgStr())
                sys.exit()
                              
    def processAckMsg(self, message):
        self.sentQueue.delEntry(message.id)
         
    def processDiscoverMsg (self, message):  
        newPaths = []
        for path in message.path:
            if path not in self.connectPaths:
                self.connectPaths.append(path)
                newPaths.append(path)
        if len(newPaths) > 0:
            self.sendNewPaths(newPaths)
        
    def setNeighborNodes(self,neighborNodes):
        self.neighborNodes = neighborNodes
        i = 0
        for nodeCell in self.neighborNodes:
            self.neighborDict.update({nodeCell: i})
            if self.nodeID < nodeCell.nodeID:       
                nodeTuple = (self ,nodeCell)
            else:
                nodeTuple = (nodeCell, self)
            path = findPath(nodeTuple,pathList)
            self.connectPaths.append(path)
            self.neighborNos.append(nodeCell.nodeID)
            i += 1
        self.nNeighbors = len(self.connectPaths)
        
        self.delta = np.zeros((self.nNeighbors,),dtype=float)
        self.deltaij = np.zeros((self.nNeighbors,),dtype=float)
        self.weightedDeltaij = np.zeros((self.nNeighbors,),dtype=float)
        self.deltaUpdateBlock = np.zeros((self.nNeighbors,1),dtype=int)
        self.neighborUpdatedX = np.zeros((self.nNeighbors,1),dtype=int)
        self.density = self.nNeighbors
        self.sequence = np.zeros((self.nNeighbors,1),dtype=int)
        for node in self.neighborNodes:
            if node.nodeID > self.nodeID:
                self.sequence[self.neighborDict.get(node)] = 1
            else:
                self.sequence[self.neighborDict.get(node)] = 0
                      

    def getNeighborNodes(self):
        return self.neighborNodes
                       
    def connectPathExists(self,path):
        if path in self.connectPaths:
            return True
        else:
            return False

    def sendConnectPaths(self):
        for cellNode in self.neighborNodes:
            message = nodeMessage(MsgType.Discover, self, cellNode,
                                  self.connectPaths,0)
            self.sQueue(message)
            self.logMsg(message)
                   
    def sendNewPaths(self, newPaths):
        for cellNode in self.neighborNodes:
            message = nodeMessage(MsgType.Discover, self, cellNode,
                                  newPaths, 0)
            self.sQueue.enqueue(message)
            self.logMsg(message)
                                    
    def clearConnectPaths(self):
        self.connectPaths.clear()
        for nodeCell in self.neighborNodes:
            nodeTuple = (min(self.nodeID, nodeCell.nodeID),
                         max(self.nodeID, nodeCell.nodeID))
            path = findPath(nodeTuple,pathList)
            self.connectPaths.append(path)
            
    def sendAck(self,message):
        messageAck = nodeMessage(MsgType.GenAck,self,message.fromNode)
        messageAck.id = message.id
        self.sQueue.enqueue(messageAck)
        self.logMsg(messageAck)
        
    def sendData (self):
        # breakpoint()
        for node in self.neighborNodes:
            nodeIdx = self.neighborDict.get(node)
            if node.nodeID > self.nodeID:
                messageSend = nodeMessage(MsgType.Value,self,node)
                messageSend.value  =  self.x
            else:
                messageSend = nodeMessage(MsgType.Delta,self,node)
                messageSend.delta = self.delta[nodeIdx]
            messageSend.sequence = self.sequence[nodeIdx]
            messageSend.density = self.density
            self.sQueue.enqueue(messageSend)
            self.logMsg(messageSend)

        
    def recvValue (self,message):
    # value is state of fromNode
        for node in self.neighborNodes:
            if node == message.fromNode:
                nodeIdx = self.neighborDict.get(node)
                if self.sequence[nodeIdx] == (message.sequence -1):    

                    self.delta[nodeIdx] = message.value - self.x      
                    self.deltaij[nodeIdx] += self.delta[nodeIdx]
                    weight = 2./(self.density + message.density +2.)
                    self.weightedDeltaij[nodeIdx] = weight * self.deltaij[nodeIdx]
                    self.x = self.z + np.sum(self.weightedDeltaij)
                    self.sequence[nodeIdx] += 1
                    return
                else:
                    print (f'Node {self.nodeID}: Value message: Seq({self.nodeID},{message.fromNode.nodeID}) = '+\
                            f'{self.sequence[nodeIdx]}, message sequence = {message.sequence}',
                            file=log_out,flush=True)
            
    def recvDelta (self,message):
        
        # value is delta of fromNode
        for node in self.neighborNodes:

            if node == message.fromNode:
                nodeIdx = self.neighborDict.get(node)
                if message.sequence == self.sequence[nodeIdx]:
                    self.delta[nodeIdx] =  -message.delta
                    self.deltaij[nodeIdx] += self.delta[nodeIdx]
                    weight = 2./(self.density + message.density + 2.)
                    self.weightedDeltaij[nodeIdx] = weight * self.deltaij[nodeIdx]
                    self.x = self.z + np.sum(self.weightedDeltaij)
                    self.sequence[nodeIdx] += 1
                    return
                else:
                    print (f'Node {self.nodeID}: Delta message: Seq({self.nodeID},{message.fromNode.nodeID}) = '+\
                            f'{self.sequence[nodeIdx]}, message sequence = {message.sequence}',
                            file=log_out,flush=True)
                            
    
    def sendMessage(self, nodeMessage):
        if not self.sendBlock:
            toNode = nodeMessage.toNode.nodeID
            nodeList[toNode].rQueue.enqueue(nodeMessage)

    def recvMessage(self, nodeMessage):
        if not self.recvBlock:
            pass
                               
    def processSQueue(self):
        if not self.sendBlock:
            while self.sQueue.size() > 0:
                message = self.sQueue.dequeue()
                if message.msgType !=MsgType.GenAck and message.msgType !=MsgType.DeltaAck:
                    self.sentQueue.enqueue(message)
                self.sendMessage(message)            

                     
    def logMsg (self,message,queue='sQueue'):
        print (f'Node: {self.nodeID}: Queue: ', queue,message.msgStr(),
               file=log_out,flush=True)
        
        
    def blockComms (self):
        self.sendBlock = True
        self.recvBlock = True
        
    def unBlockComms (self):
        self.sendBlock = False
        self.recvBlock = False    

    def setValue(self, value):
        self.z = value
        
    def getValue (self):
        return self.z
    
    def setX (self, value):
        self.x = value
        
    def getX (self):
        return self.x
                
    def setStatus(self, status):
        self.status = status
        
    def getStatus (self):
        return self.status
    
    def getPathCount (self):
        return len(self.connectPaths)
                          
    def setDiscoveryMode(self):
        self.discoveryMode = True
        
    def resetDiscoveryMode(self):
        self.discoveryMode = False
       
    def process(self):
        #pdb.set_trace()
        if self.status == True:
            self.processRQueue()
            if not self.discoveryMode:
#                print (f'Loop iteration: {loop_iter}',file=log_out,flush=True)
                self.sendData()
            self.processSQueue()
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
    
def checkNeighbors (nodeList, numTuples):
    for i in range(len(nodeList)):
        if nodeList[i].getPathCount() != numTuples[i]:
            numTuples[i] = nodeList[i].getPathCount()
            return True
    return False
    

"""
    Main Entry Point
"""    
if __name__ == '__main__':
    
    #orig_stdout  = sys.stdout
    # sys.stdout = open ('output.txt','w') 
    log_out = open('output.txt','w')
    
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
    
    # create network graph
    G = buildGraph(busDict, lineList)
    
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
    for i in range(nNode):
        neighborList = [nodeList[j] for j in range(nNode) if C[i,j]]        
        nodeList[i].setNeighborNodes(neighborList)
        
    
    # seed numTuples and tuplesChanged  
    numTuples = [0]*nNode
    
    # seed averages array
    max_iter = 2999
    averages = np.zeros((max_iter+1)*nNode).reshape(max_iter+1,nNode)
    x = np.zeros((nNode,),dtype=float)
    sumAvg = 0
    for i in range(len(nodeList)):
        averages[0,i] = nodeList[i].getValue()
        sumAvg += nodeList[i].getValue()
    average = sumAvg/nNode
    print (f'actual average is {average}')
    loop_iter = 1

    """
    Do network discovery
    """    
    for node in nodeList:
        node.setDiscoveryMode()

    loop_iter = 1       
    while True:
        for node in nodeList:
            node.process()
        if loop_iter % 5:
            if not checkNeighbors(nodeList,numTuples):
                print (f'Discovery finished after {loop_iter} iterations')
                break
        loop_iter += 1
            
    for node in nodeList:
        node.resetDiscoveryMode()
        print (f'Node: {node.nodeID} z: {node.z}',file=log_out,flush=True)
        print (f'Node: {node.nodeID} Neighbors: {node.neighborNos}\n\tseq: {node.sequence} ',
               file=log_out,flush=True)


    """ 
    Find consensus
    """    
    counts = np.zeros((nNode,),dtype=int)
    values = np.zeros((max_iter+1)*nNode).reshape(max_iter+1,nNode)
    curValue = np.zeros((nNode,),dtype=float)
    for i in range(nNode):
        curValue[i] = random.random()
   
    while abs(np.max(curValue) - np.min(curValue)) > 1e-4: 
            
        j = random.randint(0, nNode-1)           
        nodeList[j].process()
        curValue[j] = nodeList[j].getX()
        averages[counts[j],j] = curValue[j]
        counts[j] += 1
 
        loop_iter += 1
        if  np.max(counts) > max_iter - 1:
            break   
       
    print (f'Consensus reached in {np.average(counts)} iterations')    
    for nodeCell in nodeList:
        print (f'Node {nodeCell.nodeID} has consensus value ' +
              f'{nodeCell.x}')
    
    plt.figure()
    for i in range (nNode):
        plt.plot(averages[0:counts[i],i])
    plt.title("Average Convergence")
    plt.xlabel("Iterations")
    plt.ylabel("Average")
    plt.savefig("Consensus.pdf")
    log_out.close()
            
    
