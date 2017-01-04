package jp.piax.ofm.pubsub.web.servlet;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import jp.piax.ofm.pubsub.adaptor.WebUserAdaptor;
import jp.piax.ofm.pubsub.adaptor.WebUserAdaptorManager;
import jp.piax.ofm.pubsub.common.CommonValues;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Web API Servlet
 * 
 * Web API と PIAX 制御層とのインタフェースをする
 */
public class APIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(APIServlet.class);

    private static final String CONTENT_TYPE = "Content-Type: application/json";

    protected WebUserAdaptorManager manager = null;

    /**
     * 
     * @param manager
     */
    public APIServlet(WebUserAdaptorManager manager) {
        if (manager == null)
            throw new NullPointerException("manager should not be null");

        this.manager = manager;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        logger.info("doPost : {}", req.getQueryString());
        HttpSession session = (HttpSession) req.getSession();
        String userid = (String) session.getAttribute(CommonValues.SESSION_USERID);
        logger.debug("UserID {} ", userid);

        WebUserAdaptor adaptor = manager.getWebUserAdaptor(userid);
        if (adaptor == null) {
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            resp.getWriter().flush();
            logger.error("500 Internal Server Error. Cannot get a WebUserAdaptor for {}", userid);
            return;
        }

        String cmd = req.getParameter("cmd");
        cmd = cmd.toLowerCase();
        String topic = req.getParameter("topic");

        // パラメータ異常 -> 400
        if (cmd == null || cmd.isEmpty()) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp.getWriter().flush();
            logger.error("400 Bad request. cmd should not be null or empty");
            return;
        }
        if (topic == null || topic.isEmpty()) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp.getWriter().flush();
            logger.error("400 Bad request. topic should not be null or empty");
            return;
        }
        if (!topic.matches("^[0-9a-zA-Z_@]+$")) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp.getWriter().flush();
            logger.error("400 Bad request. topic name should be made of [0-9a-zA-Z_]");
            return;
        }

        
        try {
            JSONObject ret = null;
            if ("pub".equals(cmd)) {    // publish ( topic, content )
                String content = req.getParameter("content");
                // パラメータ異常 -> 400
                if (content == null || content.isEmpty()) {
                    resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    resp.getWriter().flush();
                    logger.error("400 Bad request. content should not be null or empty");
                    return;
                }
                adaptor.publish(topic, content);
                ret = new JSONObject();
            } else if  ("sub".equals(cmd)) {    // subscribe ( topic )
                adaptor.subscribe(topic);

                ret = getSubscribedTopicsByJSONArray(adaptor);
            } else if  ("unsub".equals(cmd)) {  // unsubscribe ( topic )
                adaptor.unsubscribe(topic);

                ret = getSubscribedTopicsByJSONArray(adaptor);
            } else {
                // それ以外はパラメータ異常としてエラーを返す
                resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                resp.getWriter().flush();
                logger.error("400 Bad request. content should not be null or empty");
                return;
            }
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.setContentType(CONTENT_TYPE);
            resp.setHeader("Connection", "Close");
            if (ret != null) {
                resp.getWriter().write(ret.toString());
            }
            resp.getWriter().flush();
        } catch (Exception e) {
            // 例外発生時は Internal Server Error
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            resp.setContentType(CONTENT_TYPE);
            resp.setHeader("Connection", "Close");
            resp.getWriter().flush();
            logger.error("500 Internal Server Error.", e);
        }
        logger.debug("doPost finished");
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        logger.debug("doGet");
        HttpSession session = (HttpSession) req.getSession();
        String userid = (String) session.getAttribute(CommonValues.SESSION_USERID);
        logger.debug("UserID {} ", userid);

        WebUserAdaptor adaptor = manager.getWebUserAdaptor(userid);
        if (adaptor == null) {
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            resp.getWriter().flush();
            logger.error("500 Internal Server Error. Cannot get a WebUserAdaptor for {}", userid);
            return;
        }

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType(CONTENT_TYPE);
        resp.setHeader("Connection", "Close");

        // subscribe 中のトピックを取得
        try {
            JSONObject ret = getSubscribedTopicsByJSONArray(adaptor);
            OutputStream out = new BufferedOutputStream(resp.getOutputStream());
            out.write(ret.toString().getBytes("UTF-8"));
            out.flush();
        } catch (Exception e) {
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            resp.setContentType(CONTENT_TYPE);
            resp.setHeader("Connection", "Close");
            resp.getWriter().flush();
            logger.error("500 Internal Server Error.", e);
        }
        logger.debug("doGet finished");
    }

    /**
     * get subscribed topics of adaptor
     * @param adaptor
     * @return subscribed topics of the adaptor
     * @throws JSONException 
     */
    private JSONObject getSubscribedTopicsByJSONArray(WebUserAdaptor adaptor) throws JSONException {
        List<String> topics = adaptor.getSubscribeTopic();
        String userid = adaptor.getUserId();
        JSONObject ret = new JSONObject();
        JSONArray subtopics = new JSONArray();
        for (String topic : topics) {
            subtopics.put(topic);
        }
        ret.put("subscribing", subtopics);
        ret.put("userid", userid);
        return ret;
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        if (resp.isCommitted())
            return;

        String method = req.getMethod();
        // GET, POST のみ許可
        if ("GET".equals(method) || "POST".equals(method)) {
            super.service(req, resp);
        } else {
            resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED, req.getMethod() + " is not supported");
        }
    }

}
