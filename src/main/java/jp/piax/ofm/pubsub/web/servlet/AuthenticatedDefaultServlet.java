package jp.piax.ofm.pubsub.web.servlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import jp.piax.ofm.pubsub.common.CommonValues;

import org.eclipse.jetty.servlet.DefaultServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Session による認証情報を要求する DefaultServlet
 * 
 * 未認証時は
 *  A) 401 unauthorized を返す
 *  B) 指定の URL に Redirect する
 * のどちらかの動作をインスタンス生成時に選択する。
 */
public class AuthenticatedDefaultServlet extends DefaultServlet {
    private static final long serialVersionUID = 1L;
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(AuthenticatedDefaultServlet.class);
    private String loginRedirectURL = null;

    /**
     * 未認証時に 401 unauthorized を返す Servlet を作成する
     */
    public AuthenticatedDefaultServlet() {
        loginRedirectURL = null;
    }

    /**
     * 未認証時にリダイレクトする Servlet を作成する
     * 
     * @param loginredirecturl 未認証時のリダイレクト先URL
     */
    public AuthenticatedDefaultServlet(String loginredirecturl) {
        if (loginredirecturl == null)
            throw new NullPointerException("loginredirecturl should not be null");
        if (loginredirecturl.isEmpty())
            throw new IllegalArgumentException("loginredirecturl should not be empty");

        this.loginRedirectURL = loginredirecturl;
    }

    /* (非 Javadoc)
     * @see javax.servlet.http.HttpServlet#service(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
     */
    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        HttpSession session = req.getSession(false);
        // セッションが無い場合
        if (session == null) {
            logger.warn("No session : {}", req.getRemoteAddr());
            if (loginRedirectURL != null) {
                resp.sendRedirect(loginRedirectURL);
            } else {
                resp.sendError(HttpServletResponse.SC_UNAUTHORIZED, "No valid session");
            }
            return;
        }

        String userid = (String) session.getAttribute(CommonValues.SESSION_USERID);
        // セッション中にユーザIDが登録されていない場合（未認証）
        if (userid == null) {
            logger.warn("Not authenticated session SESSION_ID {} , [{}]", session.getId(), req.getRemoteAddr());
            if (loginRedirectURL != null) {
                resp.sendRedirect(loginRedirectURL);
            } else {
                resp.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Not authorized");
            }
            return;
        }
        super.service(req, resp);
    }
}
