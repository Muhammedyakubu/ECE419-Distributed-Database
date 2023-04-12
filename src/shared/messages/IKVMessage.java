package shared.messages;

public interface IKVMessage {
	
	public enum StatusType {
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		REPLICATE,
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, 	/* Delete - request unsuccessful */
		FAILED,			/* message format unknown, message size exceeded, etc. */

		//Client-Server Status
		SERVER_NOT_RESPONSIBLE,	/* Put/Get - Key was not found in connected server */

		SERVER_STOPPED,	/* Put/Get - Server not configured yet */
		SERVER_WRITE_LOCK, /*Put - Server rebalancing keys, only gets can be made */

		//ECS-Server Status Messages
		CONNECT_ECS, 	/* Server sends to ECS to configure */
		UPDATE_METADATA, /* ECS sends to servers to update their stored metadata */
		SET_STATE, 		/* ECS sends to server to update its ServerState */
		REBALANCE, 		/* ECS sends to successor to initiate rebalance */
		REBALANCE_SUCCESS, /* Successor sends to ECS to confirm success */
		REBALANCE_ERROR, /* Rebalance not successful */
		SHUTTING_DOWN, 	/* Server sends upon its own shutdown */

		TRANSFER, 		/* ECS sends to server to transfer keys within specified range to specified server */
		TRANSFER_SUCCESS, /* Server sends to ECS to confirm success */
		TRANSFER_ERROR, /* Server sends to ECS in case of failure */

		DELETE_KEYRANGE, /* ECS sends to server to delete keys within specified range */
		DELETE_KEYRANGE_SUCCESS, /* Server sends to ECS to confirm success */
		DELETE_KEYRANGE_ERROR, /* Server sends to ECS in case of failure */

		//Keyrange commands
		KEYRANGE,		/* Request keyrange of server */
		KEYRANGE_SUCCESS, /* Successful keyrange return */

		KEYRANGE_READ,		/* Request keyrange_read of server */
		KEYRANGE_READ_SUCCESS, /* Successful keyrange_read return */

		// Server-Server Status Messages
		CONNECT_SERVER, /* Server sends to server to connect */
		SERVER_PUT,		/* Server put command when rebalancing */
		WAGWAN, /*Heartbeat message*/

		// M4 Status Messages

		// new connection protocol messages
		CONNECT,			/* Client sends to server to connect (with client ID in key) */
		CONNECT_SUCCESS, 	/* Server sends to client to confirm connection */
		CONNECT_ERROR,		/* Server sends to client to reject connection */
		REQUEST_ID,			/* Client sends to server to request ID */
		SET_CLIENT_ID,		/* Server sends to client to set ID */

		// subscription protocol messages
		SUBSCRIBE,			/* Client sends to server to subscribe to a key */
		SUBSCRIBE_SUCCESS,	/* Server sends to client to confirm subscription */
		SUBSCRIBE_ERROR,	/* Server sends to client to reject subscription */
		UNSUBSCRIBE,		/* Client sends to server to unsubscribe from a key */
		UNSUBSCRIBE_SUCCESS,/* Server sends to client to confirm unsubscription */
		UNSUBSCRIBE_ERROR,	/* Server sends to client to reject unsubscription */

		// ECS-server subscription protocol messages
		NOTIFY_SUBSCRIBERS,	/* <key> <List<clientID>. Server sends to ECS to notify subscribers of a key.
		 							ECS forwards to all server */
		NOTIFY_SUBSCRIBERS_SUCCESS,	/* ECS sends to server to confirm that all subscribers have been notified.
										Server overloads this and returns list of successfully notified clients
										in the key field */
		NOTIFY_SUBSCRIBERS_FAIL,
		UNSUBSCRIBE_CLIENTS,	/* <key> <List<ClientID>>. (Synonymous to notify_fail) In the case that not all clients are notified
									=> at least one client has disconnected, ECS sends to a server to
									unsubscribe all referenced clients in from <key> */

		// client-server subscription protocol messages
		NOTIFY,				/* <key>. Server sends to client to notify it of a key update */
		NOTIFY_SUCCESS,		/* Client sends to server to confirm that it has been notified */
		NOTIFY_ERROR,		/* Client sends to server in the case of notification error */
	}
	public enum ServerState {
		SERVER_STOPPED,
		ACTIVE,
		SERVER_WRITE_LOCK
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();
	
}


