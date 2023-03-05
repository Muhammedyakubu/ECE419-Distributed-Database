**# ECE419-Distributed-Database-M2

## KVClient/KVStore Updates

In order to extend the client to be compatible with the M2 functionality, Team 50 added components to handle keyrange requests and SERVER_NOT_RESPONSIBLE messages. The keyrange requests to the connected server are issued similarly to put/get requests (using KVMessage and KVStore), but instead with status KEYRANGE. The server populates the value field of KVMessage with its metadata and sends it back to client with status KEYRANGE_SUCCESS, where it gets saved to the client’s metadata variable. The client searches this metadata to identify the correct server for each put/get request. If it needs to change servers, it will disconnect from the current server and reconnect to the new one. Upon the receival of a SERVER_NOT_RESPONSIBLE message, the client issues a keyrange request to the server to receive updated metadata, connects to the correct server, and reissues the request.

## KVServer Updates (Jas)
// pls introduce what a keyrange is

## Metadata

To keep track of the metadata, Team 50 implemented three classes: Pair (given that the Pair class in Java is not available before Java 8), Range, and Metadata. The metadata is stored in a vector of Pairs consisting of a String and a Range. Range holds the start and endpoint of the keyrange (in the form of BigInteger). Within the metadata class, FUNCTIONS

-mention range

## KVMessage Updates (Muhammed)


## ECSNode (Muhammed)

The ECSNode is used by the ECS to store information about each KVServer in the storage service. It contains the hostname, port number, and keyrange of the KVServer. It also contains a reference to the socket the ECS uses to communicate with the KVServer socket. The ECSNode class also impplements methods to simplify communication with the KVServer. It implements methods to UPDATE_METADATA, initiate REBALANCEs, SET_STATEs and handle their corresponding acknowledgements messages.

## ECS Client (Muhammed)
The External Configuration Service (ECS) is the only completely new component of the project. It enables dynamic scaling of the storage system by maintaining a consistent hash-ring (KVMetadata) of the KVServers. The ECS client is responsible for adding and removing KVServers from the storage service. It is also responsible for initiating the transfer of data between KVServers when a KVServer is added or removed.
Here is the protocol for adding a KVServer to the storage service:
1. On initialization, KVServers are expected to send to ECS a CONNECT_ECS message containing their hostname and port number. The ECS will then add the KVServer to the KVMetadata.
2. If this is not the only KVServer in the storage service, the ECS will begin the rebalance procedure with the new KVServer as the "receiver" and it's successor in the hash-ring as the "sender".

The rebalance procedure is as follows:
1. It begins with the ECS sending the receiver (new KVServer) an UPDATE_METADATA message with the most recent metadata. 
2. The ECS will then send the sender (new KVServer's successor) a REBALANCE message containing the receiver's hostname, port number and keyrange. This message also activate a SERVER_WRITE_LOCK on the sender.
3. The sender will then initiate a connection with the receiver and transfer all the affected tuples. To do this, it sends a SERVER_PUT message to the receiver for each tuple in its keyrange. The receiver will then send the ECS a REBALANCE_SUCCESS message when it has finished transferring all the tuples.
4. On receiving the REBALANCE_SUCCESS message, the ECS then sends UPDATE_METADATA to all existing KVServers in the storage service after which it releases the write-lock on the sender by sending a SET_STATE message with the state "ACTIVE".
5. The ECS then adds this new KVServer's corresponding ECSNode to its Map of connected KVServers, kvNodes.

While idling, the ECS will listen for new KVServer connections and shutdown notifications from existing KVServers on a single Thread. This is achieved by setting a very short timeout on the ServerSocket, which enables the accept() method to be functionally non-blocking. We then endlessly poll the ECS's ServerSocket as well as all connected KVServer sockets. We chose a single-threaded approach for two major reasons: 
1. We wanted to ensure that multiple KVservers are never added or removed simultaneously, to prevent data loss,
2. Once a KVServer is connected, we only expect a SHUTTING_DOWN message from it. Dedicating a thread to each KVServer would be overkill.

When the ECS receives a SHUTTING_DOWN message from a KVServer, it removes it from the metadata and extends the keyrange of its predecessor to include the removed KVServer's keyrange. It then initiates the rebalance procedure with the removed KVServer as the sender and the node's predecessor as the receiver.
sends an UPDATE_METADATA message to all running KVServers in the storage service. The ECS also removes the KVServer's corresponding ECSNode from kvNodes.  





