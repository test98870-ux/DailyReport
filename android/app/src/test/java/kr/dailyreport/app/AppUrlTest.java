package kr.dailyreport.app;

import org.junit.Test;
import static org.junit.Assert.*;

public class AppUrlTest {
    @Test public void allowsOnlyPackagedAppAssets() {
        assertTrue(AppUrl.isLocal(AppUrl.HOME));
        assertTrue(AppUrl.isLocal("https://appassets.androidplatform.net/assets/mobile/app.js"));
        assertTrue(AppUrl.isLocal("https://appassets.androidplatform.net/assets/mobile/style.css"));
    }
    @Test public void rejectsRemoteOriginsCredentialsAndTraversal() {
        String[] urls = {"https://evil.com/assets/mobile/index.html", "javascript:alert(1)", "file:///etc/passwd",
            "https://appassets.androidplatform.net.evil.com/assets/mobile/index.html",
            "http://appassets.androidplatform.net/assets/mobile/index.html",
            "https://user@appassets.androidplatform.net/assets/mobile/index.html",
            "https://appassets.androidplatform.net:8000/assets/mobile/index.html",
            "https://appassets.androidplatform.net/assets/mobile/../index.html",
            "https://appassets.androidplatform.net/assets/mobile/%2e%2e/index.html",
            "https://appassets.androidplatform.net/assets/mobile/index.html?code=evil",
            "https://appassets.androidplatform.net/assets/mobile/index.html#evil", "/assets/mobile/index.html", null};
        for (String url : urls) assertFalse(url, AppUrl.isLocal(url));
    }
}
