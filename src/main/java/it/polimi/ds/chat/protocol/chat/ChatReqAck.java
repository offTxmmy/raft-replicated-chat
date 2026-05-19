package it.polimi.ds.chat.protocol.chat;

import it.polimi.ds.chat.protocol.broker.BrokerMessage;

public class ChatReqAck extends BrokerMessage {
    private final String localMsgId;
    private final long globalSeq; // numero di sequenza globale assegnato

    public ChatReqAck(String localMsgId, long globalSeq) {
        this.localMsgId = localMsgId;
        this.globalSeq = globalSeq;
    }

    public String getLocalMsgId() {
        return localMsgId;
    }

    public long getGlobalSeq() {
        return globalSeq;
    }
}
