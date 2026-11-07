package kr.dailyreport.app;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;

public final class AppUrl {
    public static final String HOME = "https://appassets.androidplatform.net/assets/mobile/index.html";
    private static final Set<String> PATHS = Set.of("/assets/mobile/index.html", "/assets/mobile/style.css", "/assets/mobile/app.js");
    private AppUrl() { }

    public static boolean isLocal(String value) {
        try {
            URI uri = new URI(value);
            return "https".equals(uri.getScheme()) && "appassets.androidplatform.net".equals(uri.getHost())
                    && uri.getRawUserInfo() == null && uri.getPort() == -1 && uri.getRawQuery() == null
                    && uri.getRawFragment() == null && PATHS.contains(uri.getRawPath());
        } catch (URISyntaxException | NullPointerException e) { return false; }
    }
}
