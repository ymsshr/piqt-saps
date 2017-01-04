package jp.piax.ofm.pubsub.web;

import javax.servlet.http.HttpServletRequest;

/**
 * ユーザ認証インタフェース
 * 
 * ユーザ認証の仕組みは authenticate を実装し、LoginServlet に与えることで実現する。
 */
public interface Authenticator {
    /**
     * HTTP パラメータからを元に、何らかの認証を行いユーザIDを取得する
     * 
     * @param req 認証が要求された HTTP request に対応する HttpServletRequest
     * @return 認証成功時: ユーザID  認証失敗時: null
     */
    String authenticate(HttpServletRequest req);
}
