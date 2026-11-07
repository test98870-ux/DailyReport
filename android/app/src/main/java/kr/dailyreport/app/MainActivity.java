package kr.dailyreport.app;

import android.annotation.SuppressLint;
import android.app.Activity;
import android.content.ActivityNotFoundException;
import android.content.Intent;
import android.content.pm.ApplicationInfo;
import android.graphics.Color;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.view.View;
import android.view.WindowInsets;
import android.webkit.JavascriptInterface;
import android.webkit.WebResourceRequest;
import android.webkit.WebResourceResponse;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.widget.FrameLayout;
import android.widget.Toast;
import android.window.OnBackInvokedDispatcher;

import com.chaquo.python.PyObject;
import com.chaquo.python.Python;
import com.chaquo.python.android.AndroidPlatform;

import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/** Only APK-owned pages can use the bridge. No server or scheduled Android work. */
public final class MainActivity extends Activity {
    private WebView web;
    private final ExecutorService worker = Executors.newFixedThreadPool(2);
    private final AtomicBoolean collecting = new AtomicBoolean(false);
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private volatile boolean visible;
    private volatile boolean destroyed;
    private PyObject backend;

    @Override
    public void onCreate(Bundle state) {
        super.onCreate(state);
        FrameLayout root = new FrameLayout(this);
        root.setBackgroundColor(Color.rgb(247, 248, 250));
        root.setOnApplyWindowInsetsListener((view, insets) -> {
            if (Build.VERSION.SDK_INT >= 30) {
                android.graphics.Insets safe = insets.getInsets(WindowInsets.Type.systemBars()
                        | WindowInsets.Type.displayCutout() | WindowInsets.Type.ime());
                view.setPadding(safe.left, safe.top, safe.right, safe.bottom);
            } else {
                view.setPadding(insets.getSystemWindowInsetLeft(), insets.getSystemWindowInsetTop(),
                        insets.getSystemWindowInsetRight(), insets.getSystemWindowInsetBottom());
            }
            return insets;
        });
        web = new WebView(this);
        root.addView(web, new FrameLayout.LayoutParams(-1, -1));
        setContentView(root);
        configureWebView();
        if (Build.VERSION.SDK_INT >= 33) {
            getOnBackInvokedDispatcher().registerOnBackInvokedCallback(
                    OnBackInvokedDispatcher.PRIORITY_DEFAULT, this::handleBack);
        }
        web.loadUrl(AppUrl.HOME);
        root.requestApplyInsets();
    }

    @SuppressLint({"SetJavaScriptEnabled", "AddJavascriptInterface"})
    private void configureWebView() {
        WebView.setWebContentsDebuggingEnabled((getApplicationInfo().flags & ApplicationInfo.FLAG_DEBUGGABLE) != 0);
        WebSettings settings = web.getSettings();
        settings.setJavaScriptEnabled(true);
        settings.setDomStorageEnabled(true);
        settings.setAllowFileAccess(false);
        settings.setAllowContentAccess(false);
        settings.setMixedContentMode(WebSettings.MIXED_CONTENT_NEVER_ALLOW);
        settings.setSafeBrowsingEnabled(true);
        settings.setSupportMultipleWindows(false);
        settings.setJavaScriptCanOpenWindowsAutomatically(false);
        settings.setUseWideViewPort(true);
        settings.setLoadWithOverviewMode(true);
        // Bridge is reachable only from the three trusted local assets: see CSP and AppUrl.
        web.addJavascriptInterface(new Bridge(), "DailyReport");
        web.setWebViewClient(new WebViewClient() {
            @Override
            public boolean shouldOverrideUrlLoading(WebView view, WebResourceRequest request) {
                if (AppUrl.isLocal(request.getUrl().toString())) return false;
                if (request.isForMainFrame() && request.hasGesture()) openExternal(request.getUrl());
                return true;
            }

            @Override
            public WebResourceResponse shouldInterceptRequest(WebView view, WebResourceRequest request) {
                String url = request.getUrl().toString();
                if (AppUrl.isLocal(url)) {
                    String path = request.getUrl().getPath();
                    String filename = path.substring(path.lastIndexOf('/') + 1);
                    String type = filename.endsWith(".html") ? "text/html"
                            : filename.endsWith(".css") ? "text/css" : "application/javascript";
                    try {
                        return new WebResourceResponse(type, "UTF-8", getAssets().open("mobile/" + filename));
                    } catch (IOException e) { return blocked(); }
                }
                if (request.isForMainFrame() || "appassets.androidplatform.net".equals(request.getUrl().getHost())) {
                    return blocked();
                }
                // CSP permits only remote images; scripts, frames and fetch are local-only/disabled.
                return null;
            }
        });
    }

    private WebResourceResponse blocked() {
        return new WebResourceResponse("text/plain", "UTF-8", 403, "Forbidden", null,
                new ByteArrayInputStream(new byte[0]));
    }

    private synchronized PyObject backend() {
        if (backend == null) {
            if (!Python.isStarted()) Python.start(new AndroidPlatform(getApplicationContext()));
            PyObject candidate = Python.getInstance().getModule("mobile_backend");
            candidate.callAttr("initialize", getFilesDir().getAbsolutePath() + "/clippings");
            backend = candidate;
        }
        return backend;
    }

    public final class CollectionCallback {
        public boolean isCancelled() { return cancelled.get() || !visible || destroyed; }
        public void onProgress(String data) { emit("progress", 0, data); }
    }

    public final class VisibilityCallback {
        public boolean isCancelled() { return !visible || destroyed; }
    }

    public final class Bridge {
        @JavascriptInterface
        public String restoreUiState() {
            return getSharedPreferences("ui_state", MODE_PRIVATE).getString("session", "");
        }

        @JavascriptInterface
        public boolean saveUiState(String raw) {
            if (destroyed || raw == null || raw.length() > 16384) return false;
            try {
                JSONObject input = new JSONObject(raw);
                JSONObject session = new JSONObject();
                for (String key : new String[]{"tab", "read", "range", "font", "category", "query", "limit",
                        "screen", "selectedId", "listScroll", "readerScroll", "readerImages"}) {
                    if (input.has(key)) session.put(key, input.get(key));
                }
                // WebView localStorage may still be buffered when Android kills the process.
                // Commit the small session synchronously on the JavascriptInterface thread.
                return getSharedPreferences("ui_state", MODE_PRIVATE).edit()
                        .putString("session", session.toString()).commit();
            } catch (Exception ignored) { return false; }
        }

        @JavascriptInterface
        public void postMessage(String raw) {
            if (destroyed || raw == null || raw.length() > 131072) return;
            try {
                JSONObject request = new JSONObject(raw);
                int id = request.getInt("id");
                String command = request.getString("command");
                JSONObject args = request.optJSONObject("args");
                if (args == null) args = new JSONObject();
                if (command.equals("cancel")) {
                    cancelled.set(true);
                    emit("result", id, "{}");
                    return;
                }
                if (command.equals("external")) {
                    Uri uri = Uri.parse(args.getString("url"));
                    runOnUiThread(() -> openExternal(uri));
                    emit("result", id, "{}");
                    return;
                }
                if (!(command.equals("load") || command.equals("collect") || command.equals("mark")
                        || command.equals("article") || command.equals("settings") || command.equals("saveSettings"))) {
                    emitError(id, "지원하지 않는 요청입니다.");
                    return;
                }
                if (command.equals("collect")) {
                    if (!visible || !collecting.compareAndSet(false, true)) {
                        emitError(id, "수집 중입니다. 화면에서 완료 상태를 확인해 주세요.");
                        return;
                    }
                    cancelled.set(false);
                }
                if (command.equals("saveSettings") && collecting.get()) {
                    emitError(id, "수집을 마치거나 중지한 뒤 설정을 저장하세요.");
                    return;
                }
                JSONObject arguments = args;
                worker.execute(() -> execute(id, command, arguments));
            } catch (Exception ignored) {
                // Malformed bridge input must not crash the app or expose stored credentials.
            }
        }
    }

    private void execute(int id, String command, JSONObject args) {
        try {
            PyObject engine = backend();
            String data;
            switch (command) {
                case "load": data = engine.callAttr("snapshot").toString(); break;
                case "settings": data = engine.callAttr("settings").toString(); break;
                case "saveSettings": data = engine.callAttr("save_settings", args.toString()).toString(); break;
                case "mark": data = engine.callAttr("mark", args.getInt("itemId"), args.getString("field"), args.getBoolean("value")).toString(); break;
                case "article": data = engine.callAttr("article", args.getInt("itemId"), new VisibilityCallback()).toString(); break;
                case "collect": data = engine.callAttr("collect", new CollectionCallback()).toString(); break;
                default: return;
            }
            emit("result", id, data);
        } catch (Exception error) {
            emitError(id, command.equals("article") ? "본문을 가져올 수 없습니다. 저장된 본문이 없거나 사이트 연결이 제한되어 있습니다. 원문에서 확인해 주세요."
                    : command.equals("saveSettings") ? "설정을 저장하지 못했습니다. 수집 기간과 40자리 OpenDART 인증키를 확인해 주세요."
                    : "작업을 완료하지 못했습니다. 연결 상태를 확인한 뒤 다시 시도해 주세요.");
        } finally {
            if (command.equals("collect")) collecting.set(false);
        }
    }

    private void emitError(int id, String message) {
        try { emit("error", id, new JSONObject().put("message", message).toString()); }
        catch (Exception ignored) { }
    }

    private void emit(String type, int id, String data) {
        try {
            JSONObject event = new JSONObject().put("type", type).put("id", id).put("data", new JSONObject(data));
            String script = "window.MobileApp && window.MobileApp.receive(JSON.parse(" + JSONObject.quote(event.toString()) + "));";
            runOnUiThread(() -> { if (!destroyed) web.evaluateJavascript(script, null); });
        } catch (Exception ignored) { }
    }

    private void openExternal(Uri uri) {
        String scheme = uri.getScheme();
        if (!("https".equalsIgnoreCase(scheme) || "http".equalsIgnoreCase(scheme)) || uri.getHost() == null) return;
        try { startActivity(new Intent(Intent.ACTION_VIEW, uri).addCategory(Intent.CATEGORY_BROWSABLE)); }
        catch (ActivityNotFoundException e) { Toast.makeText(this, "링크를 열 브라우저가 없습니다.", Toast.LENGTH_LONG).show(); }
    }

    private void handleBack() {
        web.evaluateJavascript("window.MobileApp ? window.MobileApp.back() : false", result -> {
            if (!"true".equals(result)) moveTaskToBack(true);
        });
    }

    @Override
    @SuppressLint("GestureBackNavigation") // API 33+ uses OnBackInvokedDispatcher above; legacy support only.
    @SuppressWarnings("deprecation")
    public void onBackPressed() { handleBack(); }

    @Override
    protected void onResume() {
        super.onResume();
        visible = true;
        web.onResume();
        emit("resume", 0, "{}");
    }

    @Override
    protected void onPause() {
        web.evaluateJavascript("window.MobileApp && window.MobileApp.suspend()", null);
        super.onPause();
    }

    @Override
    protected void onStop() {
        visible = false;
        cancelled.set(true);
        web.onPause();
        super.onStop();
    }

    @Override
    protected void onDestroy() {
        destroyed = true;
        cancelled.set(true);
        worker.shutdownNow();
        web.removeJavascriptInterface("DailyReport");
        ((FrameLayout) web.getParent()).removeView(web);
        web.destroy();
        super.onDestroy();
    }
}
