package jp.piax.ofm.pubsub.web.servlet;

import java.io.IOException;
import java.net.URI;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import jp.piax.ofm.pubsub.common.CommonValues;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ログアウト処理を行う Servlet
 */
public class LogoutServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(LogoutServlet.class);

    private URI redirectToPage = null;      // ログアウト後リダイレクト先URL

    /**
     * 
     * @param redirectto ログアウト後リダイレクト先URL
     */
    public LogoutServlet(String redirectto) {
        if (redirectto == null)
            throw new NullPointerException("redirectto should not be null");
        if (redirectto.isEmpty())
            throw new IllegalArgumentException("redirectto should not be empty");

        redirectToPage = URI.create(redirectto).normalize();
    }


    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        String method = req.getMethod();
        if ("GET".equalsIgnoreCase(method) || "POST".equalsIgnoreCase(method)) {
            // セッションを破棄してログアウト処理
            HttpSession session = req.getSession(false);
            if (session != null) {
                String uid = (String) session.getAttribute(CommonValues.SESSION_USERID);
                logger.info("Logout UID: {} , SESSION_ID {} , [{}]", uid, session.getId(), req.getRemoteAddr());
                session.invalidate();
            }
            logger.trace("Redirect to {}", redirectToPage);
            resp.sendRedirect(redirectToPage.toString());
        } else {
            super.service(req, resp);
        }
    }
}
