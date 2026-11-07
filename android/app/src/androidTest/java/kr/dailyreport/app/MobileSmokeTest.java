package kr.dailyreport.app;

import android.app.Activity;
import android.app.Instrumentation;
import android.os.Bundle;
import com.chaquo.python.PyObject;
import com.chaquo.python.Python;
import com.chaquo.python.android.AndroidPlatform;
import org.json.JSONObject;

/** Explicit runtime smoke test on emulator/device, using an isolated cache database. */
public final class MobileSmokeTest extends Instrumentation {
    private boolean live;

    @Override public void onCreate(Bundle arguments) {
        super.onCreate(arguments);
        live = "true".equals(arguments.getString("live"));
        start();
    }

    @Override public void onStart() {
        Bundle report = new Bundle();
        try {
            if (!Python.isStarted()) Python.start(new AndroidPlatform(getTargetContext()));
            Python python = Python.getInstance();
            String path = getTargetContext().getCacheDir().getAbsolutePath() + "/mobile-smoke-" + System.nanoTime();
            String script = "test_directory = " + JSONObject.quote(path) + "\nlive_enabled = " + (live ? "True" : "False") + "\n" + """
                import json, shutil
                from datetime import datetime
                import mobile_backend as mobile, sources, article_reader
                class Callback:
                    cancelled = False
                    def isCancelled(self): return self.cancelled
                    def onProgress(self, raw): pass
                callback = Callback()
                mobile.initialize(test_directory)
                original = sources.build_sources
                original_article = article_reader.load_article
                class Fixture:
                    name = '런타임 테스트'
                    def fetch(self):
                        return [sources.SourceItem('뉴스', self.name, '휴대폰 단독 수집 테스트',
                            'https://v.daum.net/v/20261107120000001', datetime.now(sources.KST).isoformat(), '', [])]
                try:
                    sources.build_sources = lambda: [Fixture()]
                    data = json.loads(mobile.collect(callback))
                    assert len(data['items']) == 1, data
                    item_id = data['items'][0]['id']
                    mobile.mark(item_id, 'read', True)
                    mobile.mark(item_id, 'starred', True)
                    stored = json.loads(mobile.snapshot())['items'][0]
                    assert stored['read'] and stored['starred']
                    article_reader.load_article = lambda url: [{'type': 'text', 'text': '저장한 본문'}]
                    assert not json.loads(mobile.article(item_id))['cached']
                    article_reader.load_article = lambda url: (_ for _ in ()).throw(AssertionError('network must not be called'))
                    assert json.loads(mobile.article(item_id))['cached']
                    callback.cancelled = True
                    assert json.loads(mobile.collect(callback))['lastResult']['cancelled']
                    diagnostic_result = {'runtime': 'passed', 'live': 'not requested'}
                    if live_enabled:
                        callback.cancelled = False
                        first = next(iter(original()))
                        sources.build_sources = lambda: [first]
                        result = json.loads(mobile.collect(callback))['lastResult']
                        diagnostic_result['live'] = result
                        assert result['stored'] > 0, result
                finally:
                    sources.build_sources = original
                    article_reader.load_article = original_article
                    shutil.rmtree(test_directory, ignore_errors=True)
                """;
            PyObject namespace = python.getBuiltins().callAttr("dict");
            python.getBuiltins().callAttr("exec", script, namespace);
            String result = python.getBuiltins().callAttr("eval", "json.dumps(diagnostic_result, ensure_ascii=False)", namespace).toString();
            report.putString("stream", "\n" + result + "\n");
            finish(Activity.RESULT_OK, report);
        } catch (Exception error) {
            report.putString("stream", "\nRuntime smoke test failed: " + error + "\n");
            finish(Activity.RESULT_CANCELED, report);
        }
    }
}
