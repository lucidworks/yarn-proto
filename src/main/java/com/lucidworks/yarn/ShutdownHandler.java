package com.lucidworks.yarn;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

public class ShutdownHandler extends HandlerWrapper {

  private static final Logger LOG = Log.getLogger(ShutdownHandler.class);

  private String shutdownToken;
  private Server _server;
  private Set<String> acceptShutdownFrom = null;

  public ShutdownHandler() {
    this.shutdownToken = System.getProperty("STOP.KEY");

    String acceptShutdownFromProp = System.getProperty("yarn.acceptShutdownFrom");
    if (acceptShutdownFromProp != null) {
      acceptShutdownFrom = new HashSet<String>();
      for (String next : acceptShutdownFromProp.split(",")) {
        String addr = next.trim();
        if (addr.length() > 0)
          acceptShutdownFrom.add(addr);
      }
      if (acceptShutdownFrom.isEmpty())
        acceptShutdownFrom = null;
    }
  }

  public void setServer(Server server) {
    this._server = server;
  }

  public void handle(String target, Request request, HttpServletRequest httpServletRequest, HttpServletResponse response)
    throws IOException, ServletException
  {
    if (!target.equals("/shutdown"))
      return;

    if (!hasCorrectSecurityToken(httpServletRequest)) {
      response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
      return;
    }

    LOG.warn("Shutdown requested by "+request.getRemoteAddr());
    new Thread() {
      public void run() {
        try {
          _server.stop();
        } catch (Exception exc) {
          LOG.warn("Graceful stop Jetty server failed due to: "+exc, exc);
        } finally {
          System.exit(0);
        }
      }
    }.start();
  }

  private boolean hasCorrectSecurityToken(HttpServletRequest request) {
    if (shutdownToken != null && shutdownToken.equals(request.getParameter("token"))) {
      return (acceptShutdownFrom != null) ?
              (acceptShutdownFrom.contains(request.getRemoteAddr()) ||
                      acceptShutdownFrom.contains(request.getRemoteHost())) : true;
    }
    return false;
  }
}
