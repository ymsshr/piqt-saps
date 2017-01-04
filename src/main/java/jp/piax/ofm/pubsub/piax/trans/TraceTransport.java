package jp.piax.ofm.pubsub.piax.trans;

import java.io.IOException;

import jp.piax.ofm.pubsub.common.PublishMessage;
import jp.piax.ofm.pubsub.common.TransPathInfo;

import org.piax.agent.impl.AgentHomeImpl;
import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.RPCInvoker;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.impl.OneToOneMappingTransport;
import org.piax.gtrans.sg.RQMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceTransport<E extends Endpoint> extends OneToOneMappingTransport<E> {
    private static final Logger logger = LoggerFactory.getLogger(TraceTransport.class);

    private static final TransportId DEFAULT_TRANSPORT_ID = new TransportId("tracetrans");

    private PeerId pid;

    public TraceTransport(ChannelTransport<E> lowerTrans, PeerId pid)
            throws IdConflictException {
        this(DEFAULT_TRANSPORT_ID, lowerTrans, pid);
    }

    public TraceTransport(TransportId transId, ChannelTransport<E> lowerTrans, PeerId pid)
            throws IdConflictException {
        super(transId, lowerTrans);
        if (pid == null)
            throw new IllegalArgumentException("pid should not be null");

        this.pid = pid;
    }

    @Override
    public void fin() {
        super.fin();
        lowerTrans.fin();
    }

    /**
     * 送信メッセージが下位Transportに渡される前に呼ばれる hook method
     */
    @Override
    protected Object _preSend(ObjectId sender, ObjectId receiver,
            E dst, Object msg) throws IOException {
        return msg;
    }

    
    /**
     * 受信メッセージが上位Transportに渡される前に呼ばれる hook method
     */
    @Override
    protected Object _postReceive(ObjectId sender, ObjectId receiver,
            E src, Object msg) {
        
        // 下位transportから送られてきたメッセージを解析して PublishMessage を取り出す
        // この transport の直上が SkipGraph で無い場合は補足不能
        if (msg instanceof RPCInvoker.MethodCall) {
            RPCInvoker.MethodCall mc = (RPCInvoker.MethodCall) msg;

            // SGMessagingFramework#send が呼び出している
            // SGMessagingFramework#requestMsgReceived への RPC を補足
            // 
            //  呼び出し階層(一部省略, r989時点) 
            //  publish -..-> DOLR#request -> MSkipGraph#request -..-> SkipGraph#scalableRangeQuery
            //  -> SkipGraph#rqStartRawRange ->  SkipGraph#rqStartKeyRange
            //  -> SkipGraph#rqDisseminate -..-> SGMessagingFramework#send
            //  -> RPC で SGMessagingFramework#requestMsgReceived
            if ("requestMsgReceived".equals(mc.getMethod())) {
                // RPC requestMsgReceived の引数 RQMessage を取り出す
                if (mc.getArgs()[0] instanceof RQMessage) {
                    RQMessage rqm = (RQMessage) mc.getArgs()[0];
                    // PublishMessage の取り出し
                    Object wrappedmsg = rqm.getQuery();
                    
                    // 上位階層のメッセージが NestedMessage でラップされているのでそれを剥ぐ
                    while (wrappedmsg instanceof NestedMessage) {
                        wrappedmsg = ((NestedMessage) wrappedmsg).getInner();
                    }
                    // エージェント層の RPC の内容を解析
                    if (wrappedmsg instanceof AgentHomeImpl.AgCallPack) {
                        AgentHomeImpl.AgCallPack callpack = (AgentHomeImpl.AgCallPack) wrappedmsg;
                        // pub/sub メッセージ相当の RPC (PubSubAgent#onReceivePublishの呼び出し) を取り出す
                        if ("onReceivePublish".equals(callpack.method) && callpack.args.length == 1) {
                            // 引数の PublishMessage を抽出
                            PublishMessage pmsg = (PublishMessage) callpack.args[0];
                            // PublishMessage にパス情報を追記
                            TransPathInfo pi = new TransPathInfo(pid);
                            pmsg.addTransPath(pi);
                        }
                    }
                }
            }
        }
        return msg;
    }
}
