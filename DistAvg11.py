#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 23 11:55:13 2020

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
import sys

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
    
    #end class path
    
class nodeMessage:
    
    """ Class nodeMessage - structure of message from one node to another
    
    Calling:
        obj = nodeMessage (type, fromNodse, toNode, path, 
                           fromNodeID, toNodeID, value, id, density,
                           delay, sequence)
    
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
    def __init__(self, msgType, fromNode, toNode, 
                 fromNodeID, toNodeID, value = 0,
                 delta = 0, density = 0, delay = 0, sequence = 0):
        
        self.msgType = msgType     
        self.fromNode = fromNode
        self.toNode = toNode
        self.toNodeID = toNodeID
        self.fromNodeID = fromNodeID
        self.value = value
        self.id = uuid.uuid1()
        self.density = density
        self.delay = delay
        self.sequence = sequence

    def msgStr (self):
        aString = (f'Message type {self.msgType} from: {self.fromNodeID} to: ' +
                f'{self.toNodeID}' +
                f' value: {self.value} density: {self.density}' +
                f' sequence: {self.sequence}, time stamp: {date_time(self.id)}')    
        return aString
    
    def logMessage (self):
        logging.info(self.msgStr())
        
@ray.remote
class messageHandler(object):
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
        
        logging.basicConfig(filename='DistAvg11.log', filemode='w', level=logging.INFO) 

    async def routeMsg(self,message):
        message.logMessage()
        toNode = message.toNode
        await toNode.recvMessage.remote(message)
        
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
        nodeID: int
            Node id (number)
        neighborNodes : [Node Node Node ...]
            List of neighboring nodes
        neighborNodeIDs : [NodeID NodeID NodeID ...]
            List of neighboring node Ids
        neighborDict : {}
            Dictionary where the key is the node object and the value is
            the order of the node object among neighbornodes
        neighborDictID : {}
            Dictionary where the key is the node object ID and the value is
            the order of the node object among neighbornodes
        neighborDensity: float np array
            Numpy array of neighbor densities
        neighborPaths : [Path Path Path ... ]
            List of neighboring path objects
        sendBlock: bool
            Boolean to block sending of messages (not used)
        recvBlock : bool
            Boolean to block receiving of messages (not used)
        status : bool
            Boolean to prevent all node activity
        density : int
            Density of this node
        delta : float np array
            Numpy array of absolute delta between self and neighbor node
        deltaij : float np array
            Numpy array of cumulative delta between self and neighbor node
        weight: float np array
            Numpy array of weight of neighbor nodes value; correlates to neighbor
            nodes entry in a doubly stochastic weighting matrix
        weightedDeltaij : float np array
            Numpy array of cross product of weight and deltaij
        nNeighbors : int
            Number of immediate neighbors
        sequence : int np array
            Numpy array of neighbor message sequence numbers
        msgHandler: Ray Actor handle
            Handle to the Ray message handler actor
            
    Methods:
        
        setNeighborNodes (neighborNodes)
            Sets neighborNode
            Initializes neightbor nodes 
        setNeighborNodeIDs (neighborNodeIDs)
            Sets neighborNodeIDs. Finishes node initalization
        getNeighborNodes()
            Returns list of neighboring nodes
        get NeighborNodeIDs()
            Returns list of neighboring node IDs
        recvMessage (message)
            Receives a message from the message handler and acts on it
        findPath (fromNodeID, toNodeID)
            Returns the path object fromNodeID, toNodeID
        sendData ()
            Sends either value update (MsgType.Value) or delta update (MsgTypeDelta)
            to message handler
        calcX()
            Updates x based current x, delta-z (if any), and weightedDeltaij
        recvValue(message)
            Recieves updated x from neighbor and calculates delta
        recvDelta(message)
            Recieves updated delta from neighbor and subtracts from associated
            delta
        setPathStatus (fromNodeID, toNodeID, state)
            Sets path status
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
        getNodeIDt ()
            returns node ID number
        run() 
            starts node processing
    """    
    
    def __init__(self, nodeID, value, msgHandle):
    # def __init__(self, nodeID, value):
        
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
        self.sendBlock = False
        self.recvBlock = False
        self.status = True
        self.density = 0
        self.delta = None
        self.deltaij = None
        self.weight = None
        self.weightedDeltaij = None
        self.nNeighbors = 0
        self.sequence = None
        self.msgHandle = msgHandle
     
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
        
        # for path in self.neighborPaths:
        #     print (f'Node {self.nodeID}: Path {path.pathStr()}')
                 
            
    def getNeighborNodeIDs (self):
        return self.neighborNodeIDs
            
    def getNeighborNodes(self):
        return self.neighborNodes
    
    def recvMessage(self,message):
        
        # only process if the node status is True
        path = self.findPath(self.nodeID,message.fromNodeID)
        if not path:
            print (f"Node {self.nodeID} didn't find path {self.nodeID}-{message.fromNodeID}")  
            print (f'Node {self.nodeID} neighborPaths {self.neighborPaths}')
        elif path.status == True:         
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
        return False
        
    def sendData (self):
        for node in self.neighborNodes:   
            nodeIdx = self.neighborDict.get(node)
            nodeID = self.neighborNodeIDs[nodeIdx]
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
            self.msgHandle.routeMsg.remote(messageSend)

    def calcX (self):
        # print (f'Node {self.nodeID} in calcX')
        self.weightedDeltaij = np.multiply(self.weight, self.deltaij)
        self.x = self.z + (self.z - self.zlast) + np.sum(self.weightedDeltaij)
        self.zlast = self.z


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
    
    def getNodeID (self):
        return self.nodeID
           
    def run(self):
        self.sendData()
                
    # end class Node
    
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
     #end date_time
     
if __name__ == '__main__':
     ray.init(ignore_reinit_error=True,redis_max_clients=1000)
    
     logging.basicConfig(filename='DistAvg11.log', filemode='w', level=logging.INFO)
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
    
     # instantiate Ray instance of messageHandler
     msgHandle = messageHandler.remote()

     # instantiate Ray instances of Node
     nodeList = []
     for i in range(nNode):
         nodeList.append(Node.remote(i,i+1,msgHandle))        
    
     for i in range(nNode):
         neighborList = [nodeList[j] for j in range(nNode) if C[i,j]]
         nodeList[i].setNeighborNodes.remote(neighborList)
     time.sleep(1)

     for i in range(nNode):
         node = nodeList[i]
         neighborList = [j for j in range(nNode) if C[i,j]]
         node.setNeighborNodeIDs.remote(neighborList)
     time.sleep(1)
    
     for node in nodeList:
         node.run.remote()
        
     duration = 40
     sleep = 0.05
   
     curVals = np.zeros((nNode,),dtype=float)
     histVals = np.zeros((int(duration/sleep)+1,nNode),dtype=float)

     timer = 0
     row = 0
     while timer < duration:
         time.sleep(sleep)
         for i in range(nNode):           
             node = nodeList[i]
             curVals[i] = ray.get(node.getX.remote())
            
         if row == 300:
             nodeList[8].setStatus.remote(False)
             nodeList[4].setPathStatus.remote (4,5,False)
             nodeList[5].setPathStatus.remote (4,5,False)
         if row == 500:
             nodeList[8].setStatus.remote(True)
             nodeList[4].setPathStatus.remote (4,5,True)
             nodeList[5].setPathStatus.remote (4,5,True)              

         histVals[row,0:nNode] = curVals
         timer += sleep
         row += 1
    
     for i in range(nNode):
         print (f'Node: {i} value {curVals[i]}')
    
     plt.figure()    
     plt.plot(histVals[0:row])
     plt.title("Average Convergence")
     plt.xlabel("50 mSec Intervals")
     plt.ylabel("Average")
     plt.savefig("Consensus.pdf")
     ray.shutdown()
    