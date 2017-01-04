package jp.piax.ofm.pubsub.web.servlet;

import java.io.IOException;
import java.net.URI;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import jp.piax.ofm.pubsub.common.CommonValues;
import jp.piax.ofm.pubsub.web.Authenticator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ログイン処理を行う Servlet
 */
public class LoginServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(LoginServlet.class);

    /**
     * セッション有効期間 (sec)
     */
    public static int SessionTimeout = 15*60;

    private URI loginPage = null;           // ログインページURL
    private URI redirectToPage = null;      // ログイン後リダイレクト先URL
    private Authenticator authenticator = null;   // 認証アルゴリズム

    /**
     * 
     * @param login ログインページURL
     * @param redirectto ログイン後リダイレクト先URL
     * @param auth ユーザ認証アルゴリズム
     */
    public LoginServlet(String login, String redirectto, Authenticator auth) {
        if (login == null)
            throw new NullPointerException("login should not be null");
        if (login.isEmpty())
            throw new IllegalArgumentException("login should not be empty");
        if (redirectto == null)
            throw new NullPointerException("redirectto should not be null");
        if (redirectto.isEmpty())
            throw new IllegalArgumentException("redirectto should not be empty");
        if (auth == null)
            throw new NullPointerException("auth should not be null");

        loginPage = URI.create(login).normalize();
        redirectToPage = URI.create(redirectto).normalize();
        authenticator = auth;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        logger.trace("doPost");

        // 認証とユーザID取得
        String uid = authenticator.authenticate(req);
        HttpSession session = req.getSession();
        if (session.isNew()) {
            logger.debug("New session SESSION_ID {} , [{}]", session.getId(), req.getRemoteAddr());
        }
        if (uid == null) {
            // 未認証
            // 再度ログインページへ
            logger.debug("Unauthenticated session SESSION_ID {} , [{}]", session.getId(), req.getRemoteAddr());
            resp.sendRedirect(loginPage.toString());
            return;
        }

        session.setMaxInactiveInterval(SessionTimeout);
        session.setAttribute(CommonValues.SESSION_USERID, uid);
        resp.sendRedirect(redirectToPage.toString());
        logger.info("Login UID: {} , SESSION_ID {} , [{}]", uid, session.getId(), req.getRemoteAddr());
    }
}
