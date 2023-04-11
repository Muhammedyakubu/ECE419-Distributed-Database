package ecs;

import shared.messages.KVMessage;

/**
 * This class is a basic data structure to hold information about notifications
 * received from other ECSNodes
 */
public class Notification {
    public ECSNode initiator;
    public KVMessage message;

    Notification (ECSNode initiator, KVMessage message) {
        this.initiator = initiator;
        this.message = message;
    }

    public ECSNode getInitiator() {
        return initiator;
    }

    public KVMessage getMessage() {
        return message;
    }
}
