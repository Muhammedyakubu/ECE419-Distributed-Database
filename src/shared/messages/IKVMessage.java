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
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, 	/* Delete - request successful */
		FAILED,			/* message format unknown, message size exceeded, etc. */

		//Client-Server Status
		SERVER_NOT_RESPONSIBLE,	/* Put/Get - Key was not found in connected server*/

		SERVER_STOPPED,	/* Put/Get - Server not configured yet*/
		SERVER_WRITE_LOCK, /*Put - Server rebalancing keys, only gets can be made */

		//ECS-Server Status Messages
		CONNECT_ECS, 	/*Server sends to ECS to configure*/
		UPDATE_METADATA, /*ECS sends to servers to update their stored metadata*/
		SET_STATE, 		/*ECS sends to server to update its serverStatus*/
		REBALANCE, 		/*ECS sends to successor to initiate rebalance*/
		REBALANCE_SUCCESS, /*Successor sends to ECS to confirm success*/
		REBALANCE_ERROR, /*Rebalance not successful,*/
		SHUTTING_DOWN, 	/*Server sends upon its own shutdown*/

		//Keyrange commands
		KEYRANGE,		/* Request keyrange of server */
		KEYRANGE_SUCCESS, /* Successful keyrange return */


	}
	public enum serverStatus {
	SERVER_STOPPED,
		ACTIVE,
		SERVER_WRITE_LOCK}

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


