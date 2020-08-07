#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  5 12:31:50 2020

@author: jamesgriffin
"""

import numpy as np
import scipy.io as sp
import uuid
import datetime
import time
import matplotlib.pyplot as plt
from enum import Enum
import logging
import ray
# from threading import Thread
import _thread
class nodePath:
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
        pathStr ()
            Returns string of path attributes

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
    #end class nodePath

class nodeMessage:

    """ Class nodeMessage - structure of message from one node to another

    Calling:
        obj = nodeMessage (type, fromNodse, toNode, path,
                            fromNodeID, toNodeID, value, id, density, sequence)

    Attributes:
        msgType : enum MsgType
            defines the type of the message content
        fromNode : Node
            sending node
        toNode : Node
            receiving node
        fromNodeID : int
            sending node id
        toNodeID : int
            receiving node id
        value : float
            value to send
        id : uuid
            automatically allocated uuid
        density: int
            density of sending node
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
    def __init__(self, msgType, fromNode, toNode,
                  fromNodeID, toNodeID, value = 0,
                  delta = 0, density = 0, sequence = 0):

        self.msgType = msgType
        self.fromNode = fromNode
        self.toNode = toNode
        self.toNodeID = toNodeID
        self.fromNodeID = fromNodeID
        self.value = value
        self.id = uuid.uuid1()
        self.density = density
        self.sequence = sequence

    def msgStr (self):
        aString = (f'Message type {self.msgType} from: {self.fromNodeID} to: ' +
                f'{self.toNodeID}' +
                f' value: {self.value} density: {self.density}' +
                f' sequence: {self.sequence}, time stamp: {date_time(self.id)}')
        return aString

    def logMessage (self):
        logging.info(self.msgStr())
    # end class nodeMessage

@ray.remote
class valueLog(object):
    def __init__(self,nNode):

        self.begin_time = datetime.datetime.now()
        self.nNode = nNode
        self.values = [[] for _ in range(nNode)]    

        # for _ in range (self.nNode):
        #     self.values.append([])

    def logValue  (self, node, value, datetime):
        aList = self.values[node]
        time_delta = datetime - self.begin_time
        aList.append ((time_delta.total_seconds(), value))

    def getValueLists (self):
        return self.values
    #end class valueLog

@ray.remote
class messageLog(object):
    """ Class messageHandler - Routes messages between nodes.  Runs as Ray Actor

    Attributes:
        None

    Methods:
        __init__(self)
            Initializes actor and opens logging

        async routeMsg(self,message)
            Logs message and asynchronously sends it to appropriate node via node
            recvMessage(message) method
    """
    def __init__(self):

        logging.basicConfig(filename='DistAvg12.log', filemode='w', level=logging.INFO)

    async def logMsg(self,message):
        message.logMessage()
    # end class messageLog
    
def commCheck(node):
    """
    Thread created by Ray actor (node) to monitor time lapse between node the
    last neighbor node message.  Updates node.msgDeltaTime.  Sleeps one second
    between checks

    Parameters
    ----------
    node : ray Actor handle
        handle to the Ray actor that created the thread.

    Returns
    -------
    None.

    """
    while True:
        for neighborNodeID in node.neighborNodeIDs:
            nodeIdx = node.neighborDictID.get(neighborNodeID)
            deltaTime = datetime.datetime.now() - node.msgTime[nodeIdx]
            node.msgDeltaTime[nodeIdx] = deltaTime.total_seconds()
        time.sleep(1)
        
    
@ray.remote
class Node(object):
    """ class Node - Node object

    Attributes:
        z : float
            Node current value
        zlast : float
            Node past value
        x : float
            Estimate of node value
        nodeID : int
            Node id (number)
        neighborNodes : [node node node ...]
            List of neighboring nodes
        neighborNodeIDs : [nodeID NodeID NodeID ...]
            List of neighboring node IDs
        neighborDict : {node:seq node:seq}
            Dictionary of nodes vs node order in arrays
        neighborDictID : {nodeID:seq nodeID: seq}
            Dictionary of nodeIDs vs node order in arrays
        neighborDensity : int np.array
            Density of neighbor nodes
        neighborPaths : [(fromID, toID) (fromID, toID) ... (fromID, toID)]
            List of neighboring path tuples
        msgTime : [datetime ... datetime]
            date of last neighbor communication
        msgDeltaTime : [float ... float]
            time since last neighbor communication
        status : bool
            Boolean to cause no processing on recvMessage
        density : int
            Density of this node
        delta : float np.array
            Absolute delta value between this node and neighbors
        deltaij : float np.array
            Cumulative absolute delta value between this node and neighbors
        weight : float np.array
            Weight to apply to received values.  Equals the value in a doubly
            stochastic matrix between this node and neighbor node
        weightedDeltaij : float np.array
            Weighted cumulative delta value between this node and neighbors       
        nNeighbors : int
            Number of immediate neighbors
        sequence : int np.array
            Message sequence number
        valLogger : Ray actor handle
            Handle to the Ray actor for value logging
        msgLogger : Ray actor handle
            Handle to the Ray actor for message logging
        commCheckHandle : thread handle 
            Thread handle for commCheck thread
        calcCount : int
            Count of entries into calcX
        sentCount : int
            Count of messages sent
        recvCount : int
            Count of messages received
            
    Methods:

        setNeighborNodes (neighborNodes)
            Initializes neighborNodes, creates arrrays
            Returns neighborNodes
        setNeighborNodeIDs (neighborNodeIDs)
            Initializes neighborNodeIDs, creates neighborDict, neighborIDDict, and
            neighborPaths.  Returns neighborNodeIDs
        getNeighborNodeIDs ()
            Returns neighborNodeIDs
        getNeighborNodes ()
            Returns neighborNodes
        recvMessage (message)
            Receives a message from a neighbor node, performs calculations, and
            sends a response to neighhbor node
        findPath (fromNode, toNode)
            Returns path object between fromNode and toNode.  Returns None if
            path doesn't exist
        sendData ()
            Sends either value update (MsgType.Value) or delta update (MsgType.Delta)
            to neighbor node.  Sends MsgType.Value if this nodeID is < neighbor nodeID,
            otherwise sends MsgType.Delta
        calcX ()
            Updates node value (X) based on previous value, delta-z, and 
            weightedDeltaij. Updates zlast. Calls valLogger to log the value 
        getStats ()
            Returns tuple (calcCount, sentCount, recvCount)
        recvValue (message)
            Called by recvMessage in response to MsgType.Value message.  Calculates
            weighted delta, calls calcX() and sendData()
        recvDelta (message)
            Called by recvMessage in response to MsgType.Delta message.  Updates 
            delta for neighbor node by subtracting received delta from neighbor 
            node delta.  Calls calcX() and sendData()
        setPathStatus (fromNode, toNode, status)
            Sets path.status of path between fromNode, toNode to status.
            Returns path status
        getPathStatus (fromNode, toNode)
            Returns path.status of path between fromNode, toNode
        setValue (value)
            Sets z to value
        getValue()
            Returns z
        setX(value)
            Sets x to value
        getX()
            Returns x
        setStatus (status)
            Sets status to True or False.  Returns status
        getStatus ()
            Returns status
        run ()
            Initiates processing by causing nodes to sent MstType.Value message
            to nodes whose nodeID is less than this node's nodeID, thus beginning
            the sequence of message exchange
    """

    def __init__(self, nodeID, value, valueLogHandle, msgLogHandle):

        self.z = value
        self.zlast = value
        self.x = value
        self.nodeID = nodeID
        self.neighborNodes = None
        self.neighborNodeIDs = []
        self.neighborDict = {}
        self.neighborDictID = {}
        self.neighborDensity = []
        self.neighborPaths = []
        self.msgTime = []
        self.msgDeltaTime = []
        self.status = True
        self.density = 0
        self.delta = None
        self.deltaij = None
        self.weight = None
        self.weightedDeltaij = None
        self.nNeighbors = 0
        self.sequence = None
        self.valLogger = valueLogHandle
        self.msgLogger = msgLogHandle
        self.commCheckHandle = None
        self.calcCount = 0
        self.sentCount = 0
        self.recvCount = 0

    def setNeighborNodes (self, neighborNodes):

        self.neighborNodes = neighborNodes
        self.nNeighbors = len(self.neighborNodes)
        self.delta = np.zeros((self.nNeighbors,),dtype=float)
        self.deltaij = np.zeros((self.nNeighbors,),dtype=float)
        self.weightedDeltaij = np.zeros((self.nNeighbors,),dtype=float)
        self.density = self.nNeighbors
        self.sequence = np.zeros((self.nNeighbors,),dtype=int)
        self.neighborDensity = np.zeros((self.nNeighbors,),dtype=int)
        self.weight = np.zeros((self.nNeighbors,),dtype=float)
        self.msgTime = [datetime.datetime.now() for _ in range 
            (self.nNeighbors)]
        self.msgDeltaTime = [0 for _ in range(self.nNeighbors)]
        # if self.nodeID == 0:
        #     print (self.msgTime)
        #instantiate commCheck thread
        try:
            self.commCheckHandle = _thread.start_new_thread(commCheck,(self,))
            # self.commCheckHandle.start()
        except:
            print (f'Node {self.nodeID} Error instantiating comCheck thread')

        return self.neighborNodes

    def setNeighborNodeIDs (self,neighborNodeIDs):

        self.neighborNodeIDs = neighborNodeIDs
        i = 0
        for node in self.neighborNodes:
            self.neighborDict.update({node: i})
            i += 1

        i = 0
        for nodeID in self.neighborNodeIDs:
            self.neighborDictID.update({nodeID: i})
            i += 1

        self.neighborPaths = []
        for node in self.neighborNodes:
            nodeIdx = self.neighborDict.get(node)
            nodeID = self.neighborNodeIDs[nodeIdx]
            if nodeID > self.nodeID:
                self.sequence[nodeIdx] = 1
            else:
                self.sequence[nodeIdx] = 0
            aPath = nodePath(self.nodeID,nodeID)
            self.neighborPaths.append (aPath)

        return self.neighborNodeIDs

    def getNeighborNodeIDs (self):
        return self.neighborNodeIDs

    def getNeighborNodes(self):
        return self.neighborNodes

    def recvMessage(self,message):
        self.recvCount += 1
        if self.findPath(self.nodeID,message.fromNodeID).status == True:
            nodeIdx = self.neighborDictID.get(message.fromNodeID)
            self.msgTime[nodeIdx] = datetime.datetime.now()
            if self.status:

                if message.msgType == MsgType.GenAck:
                    self.processAckMsg(message)
                elif message.msgType == MsgType.Delta:
                    self.recvDelta(message)
                elif message.msgType == MsgType.Value:
                    self.recvValue(message)

        else:
              self.deltaij[self.neighborDictID.get(message.fromNodeID)] = 0
              self.calcX()

    def findPath(self,fromNode,toNode):
        for path in self.neighborPaths:
            if fromNode == path.fromNode and toNode == path.toNode:
                return path
            elif toNode == path.fromNode and fromNode == path.toNode:
                return path
        return None

    def sendData (self):
        for node in self.neighborNodes:
            nodeIdx = self.neighborDict.get(node)
            nodeID = self.neighborNodeIDs[nodeIdx]
            self.sentCount += 1
            if nodeID > self.nodeID:
                messageSend = nodeMessage(MsgType.Value, self, node,
                                          self.nodeID, nodeID)
                messageSend.value = self.x
            else:
                messageSend = nodeMessage(MsgType.Delta, self, node,
                                          self.nodeID, nodeID)
                messageSend.value = self.delta[nodeIdx]
            messageSend.sequence = self.sequence[nodeIdx]
            messageSend.density = self.density
            node.recvMessage.remote(messageSend)

    def calcX (self):
        # print (f'Node {self.nodeID} in calcX')
        self.calcCount += 1
        self.weightedDeltaij = np.multiply(self.weight, self.deltaij)
        self.x = self.z + (self.z - self.zlast) + np.sum(self.weightedDeltaij)
        self.zlast = self.z       
        self.valLogger.logValue.remote(self.nodeID, self.x, datetime.datetime.now())
        
    def getStats (self):
        return (self.calcCount, self.sentCount, self.recvCount)

    def recvValue (self,message):
        nodeID = message.fromNodeID
        nodeIdx = self.neighborDictID.get(nodeID)
        if self.sequence[nodeIdx] == (message.sequence -1):

            self.delta[nodeIdx] = message.value - self.x
            self.deltaij[nodeIdx] += self.delta[nodeIdx]
            self.weight[nodeIdx] = 2./(self.density + message.density +2.)
            self.calcX()
            self.sequence[nodeIdx] += 1
            self.neighborDensity[nodeIdx] = message.density
            self.sendData()

    def recvDelta (self,message):
        nodeID = message.fromNodeID
        nodeIdx = self.neighborDictID.get(nodeID)
        if message.sequence == self.sequence[nodeIdx]:

            self.delta[nodeIdx] =  -message.value
            self.deltaij[nodeIdx] += self.delta[nodeIdx]
            self.weight[nodeIdx] = 2./(self.density + message.density + 2.)
            self.calcX()
            self.sequence[nodeIdx] += 1
            self.neighborDensity[nodeIdx] = message.density
            self.sendData()

    def setPathStatus (self,fromNode,toNode,state):
        path = self.findPath (fromNode, toNode)
        if path:
            path.setStatus(state)
            message = path.pathStr()
            print (f'Node: {self.nodeID} {message}')
            return path.setStatus(state)
        else:
            return state

    def getPathStatus(self,fromNode,toNode):
        path = self.findPath (fromNode, toNode)
        if path:
            return path.getStatus()
        else:
            return False

    def setValue(self, value):
        self.z = value

    def getValue (self):
        return self.z

    def setX (self, value):
        self.x = value

    def getX (self):
        return self.x
    
    def getTiming (self):
        return self.msgDeltaTime

    def setStatus(self, status):
        self.status = status
        if status == True:
            print (f'Node {self.nodeID} set to active')
        else:
            print (f'Node {self.nodeID} set to idle')
        return self.status

    def getStatus (self):
        return self.status

    def run(self):
        # kicks things off - send value message to neighbors.  They'll respond
        # with delta messages and it keeps going ad infinitum
        if self.status:
            for node in self.neighborNodes:
                nodeIdx = self.neighborDict.get(node)
                nodeID = self.neighborNodeIDs[nodeIdx]
                if nodeID > self.nodeID:
                    messageSend = nodeMessage(MsgType.Value, self, node,
                                              self.nodeID, nodeID)
                    messageSend.value = self.x
                    messageSend.sequence = self.sequence[nodeIdx]
                    messageSend.density = self.density
                    node.recvMessage.remote(messageSend)
                    self.sentCount += 1

    # end class Node

class MsgType(Enum):
    """ Class MsgType - Enum
    GenAck = 1
    Delta = 2
    Value = 3
    DiisablePath = 4
    EnablePath = 4
    
    Current code only uses MsgType.Delta and MsgType.Value
    """
    GenAck = 1
    Delta = 2
    Value = 3
    DisablePath = 4
    EnablePath = 5
    #end class MsgType

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

def convergeWait (nNode,nodeList,curValue,eps):
    """   

    Function to wait for convergence.
    Samples values, returns when abs(highest - lowest)/average is within epsilon.
    Sleeps 1 second between tests

    Parameters
    ----------
    nNode : int
        Number of nodes.
    nodeList : [node, node, ... node]
        List of nodes
    curValue : np.array (float)
        Array to hold current values
    eps : float
        Epsilon
    Returns
    -------
    When convergence is achieved.

    """
    converge = False
    while not converge:
        sleep = 1
        for i in range(nNode):
            curValue[i] = ray.get(nodeList[i].getX.remote())
        if abs( (np.max(curValue) - np.min(curValue)) / np.average(curValue)) < eps:
            converge = True
        time.sleep(sleep)
    # end def convergeWait

def date_time (uuid_in):
    """
    date_time (uuid)

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
    #end def date_time

if __name__ == '__main__':
    ray.init(ignore_reinit_error=True,redis_max_clients=1000)
    # ray.init(address='auto', redis_password='5241590000000000')

    logging.basicConfig(filename='DistAvg12.log', filemode='w', level=logging.INFO)
 
    # set convergence critera
    eps = 1e-3
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
    
    # get array for curValue
    curValue = np.zeros((nNode,),dtype=float)
      
    # get arrays for statistic counts
    calcCount = np.zeros((nNode,),dtype=int)
    sentCount = np.zeros((nNode,),dtype=int)
    recvCount = np.zeros((nNode,),dtype=int)

 
    # instantiate value logger
    valueLogHandle = valueLog.remote(nNode)
      
    # instantiate message logger
    msgLogHandle = messageLog.remote()

    # instantiate Ray instances of Node
    nodeList = []
    for i in range(nNode):
        nodeList.append(Node.remote(i,i+1,valueLogHandle,msgLogHandle))

    # using ray.get makes this a blocking call, which is desired
    for i in range(nNode):
        neighborList = [nodeList[j] for j in range(nNode) if C[i,j]]
        neighborNodes = ray.get(nodeList[i].setNeighborNodes.remote(neighborList))

    # using ray.get makes this a blocking call, which is desired
    for i in range(nNode):
        node = nodeList[i]
        neighborList = [j for j in range(nNode) if C[i,j]]
        neighborNodeIDs = ray.get(node.setNeighborNodeIDs.remote(neighborList))
          
    startTime = datetime.datetime.now()
    print (f'Start time: {startTime}')

    # using ray.get makes this a blocking call, which is desired.
    for node in nodeList:
        stat = ray.get(node.run.remote())

    # run until first convergence
    convergeWait(nNode,nodeList,curValue,eps)
    print ('First convergence reached')

    print ('Idling node 8, path (4,5)')
    stat = ray.get(nodeList[8].setStatus.remote(False))
    if stat != False:
        print ('Error setting node 8 to false')

    for node in nodeList:
        stat = ray.get(node.setPathStatus.remote(4,5,False))
        
    time.sleep(4)
    print ('Second convergence reached')

    stat = ray.get(nodeList[8].setStatus.remote(True))
    if stat != True:
        print ('Error setting node 8 to false')

    # re-enable the node path
    for node in nodeList:
        stat = ray.get(node.setPathStatus.remote(4,5,True))
          
    convergeWait (nNode,nodeList,curValue,eps)
          
    print ('Third convergence reached')
      
    # get stats
    for i in range (nNode):
        node = nodeList[i]
        calc, sent, recv = ray.get(node.getStats.remote())
        calcCount[i] = calc
        sentCount[i] = sent
        recvCount[i] = recv
          
    endTime = datetime.datetime.now()
    valueLists = ray.get(valueLogHandle.getValueLists.remote())
    for i in range(nNode):
        print (f'Node {i}: {curValue[i]}, calcs {calcCount[i]}, sent ' +
               f'{sentCount[i]}, recv {recvCount[i]}')
    print (f'Total calculations: {np.sum(calcCount)}')
    totalMsgs = np.sum(sentCount) + np.sum(recvCount)
      
    timeDiff = endTime - startTime
    timeSecs = timeDiff.total_seconds()
    msgsPSec = int(totalMsgs/timeSecs)
    print (f'Total messages: {totalMsgs}')
    print (f'Elapsed time {timeSecs} seconds')
    print (f'Message rate: {msgsPSec} messages/second')
      
    msgTiming = [ray.get(node.getTiming.remote()) for node in nodeList]
    print ('Latest time since ')
    for i in range (nNode):
        print (f'Node {i} message delays (seconds): {msgTiming[i]}')
    plt.figure()
    for v in valueLists:
        x,y = zip(*v)
        plt.plot(x,y)
    plt.title("Average Convergence")
    plt.xlabel("Seconds")
    plt.ylabel("Average")
    plt.savefig("Consensus.pdf")
    ray.shutdown()
