from __future__ import annotations

import html
import json
import re
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from db import fetch_recent_items, init_db
from pipeline import pipeline_result_to_dict, run_pipeline
from sources import news_collection_window_start


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
AUTO_RUN_INTERVAL_SECONDS = 3 * 60 * 60
JOB_LOCK = threading.Lock()
JOB_STATE: dict[str, object] = {
    "running": False,
    "phase": "idle",
    "message": "대기 중",
    "fetched": 0,
    "stored": 0,
    "total_items": 0,
    "current_index": 0,
    "current_title": "",
    "current_source": "",
    "errors": 0,
    "mode": "",
    "last_result": None,
}


class ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True


class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._respond_html(render_dashboard())
            return
        if parsed.path == "/api/items":
            items = [dict(row) for row in fetch_recent_items(since=news_collection_window_start())]
            self._respond_json({"items": items})
            return
        if parsed.path == "/api/status":
            self._respond_json(get_job_state())
            return
        if parsed.path.startswith("/static/"):
            asset_path = STATIC_DIR / parsed.path.removeprefix("/static/")
            if asset_path.exists():
                content_type = "text/css; charset=utf-8" if asset_path.suffix == ".css" else "text/plain"
                self._respond_bytes(asset_path.read_bytes(), content_type)
                return
        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/run":
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return
        state = get_job_state()
        if state["running"]:
            self._respond_json({"started": False, "status": state})
            return
        started = start_background_run(trigger="manual")
        self._respond_json({"started": started, "status": get_job_state()})

    def log_message(self, format: str, *args: object) -> None:
        return

    def _respond_html(self, content: str) -> None:
        self._respond_bytes(content.encode("utf-8"), "text/html; charset=utf-8")

    def _respond_json(self, payload: dict[str, object]) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self._respond_bytes(body, "application/json; charset=utf-8")

    def _respond_bytes(self, body: bytes, content_type: str) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def render_dashboard() -> str:
    rows = fetch_recent_items(since=news_collection_window_start())
    reports = [dict(row) for row in rows if is_report(dict(row))]
    disclosures = [dict(row) for row in rows if is_disclosure(dict(row))]
    news_items = [dict(row) for row in rows if is_news(dict(row))]
    spotlight_cards = render_spotlight_section(reports, disclosures, news_items)
    all_cards = "\n".join(render_card(dict(row)) for row in rows) or render_empty("어제 16시 이후 자료가 아직 없습니다.")
    report_cards = render_report_sections(reports)
    disclosure_cards = render_disclosure_sections(disclosures)
    news_cards = render_news_sections(news_items)
    script = """
    <script>
      const form = document.getElementById("run-form");
      const statusText = document.getElementById("status-text");
      const progressFill = document.getElementById("progress-fill");
      const progressMeta = document.getElementById("progress-meta");
      const progressTitle = document.getElementById("progress-title");
      const panelDescription = document.getElementById("panel-description");
      const slideshowToggleButton = document.getElementById("slideshow-toggle");
      const tabButtons = Array.from(document.querySelectorAll(".tab-button"));
      const tabPanels = Array.from(document.querySelectorAll(".tab-panel"));
      const newsFilterButtons = Array.from(document.querySelectorAll(".news-filter-button"));
      const newsCategoryPanels = Array.from(document.querySelectorAll(".news-category-block"));
      const itemCards = Array.from(document.querySelectorAll("[data-item-key]"));
      const hiddenItemsStorageKey = "daily-report-hidden-item-keys";
      const hiddenItemsResetAtStorageKey = "daily-report-hidden-item-reset-at";
      const hiddenItemsResetAfterMs = 24 * 60 * 60 * 1000;
      let pollTimer = null;
      let awaitingRunCompletion = false;
      let observedCollectionRunning = false;
      let newsRotationTimer = null;
      let slideshowTimer = null;
      let slideshowEnabled = false;
      let slideshowIndex = 0;
      let newsCategoryCycleIndex = 0;
      const newsCategoryOffsets = {};
      const tabCopy = {
        "all-panel": "리포트와 공시를 시간순으로 함께 보여줍니다.",
        "reports-panel": "기업, 산업, 시황, 전략, 경제 리포트를 성격별로 묶어 보여줍니다.",
        "disclosures-panel": "DART, KIND, EDGAR와 공시 성격을 구분해 한 번에 확인합니다.",
        "news-panel": "네이버 뉴스와 커뮤니티 새소식을 모아 중복을 줄여 보여줍니다.",
      };

      function activateTab(targetId) {
        tabButtons.forEach((button) => {
          const active = button.dataset.tabTarget === targetId;
          button.classList.toggle("is-active", active);
          button.setAttribute("aria-selected", String(active));
        });
        tabPanels.forEach((panel) => {
          const active = panel.id === targetId;
          panel.classList.toggle("is-active", active);
          panel.hidden = !active;
        });
        panelDescription.textContent = tabCopy[targetId] || "";
        if (slideshowEnabled) {
          renderSlideshowFrame();
        }
      }

      function activateNewsCategory(targetId) {
        if (!targetId) {
          return;
        }
        newsFilterButtons.forEach((button) => {
          const active = button.dataset.newsTarget === targetId;
          button.classList.toggle("is-active", active);
          button.setAttribute("aria-pressed", String(active));
        });
        newsCategoryPanels.forEach((panel) => {
          const active = panel.id === targetId;
          panel.classList.toggle("is-active", active);
          panel.hidden = !active;
        });
      }

      function getHiddenItemIds() {
        try {
          const resetAt = Number(window.localStorage.getItem(hiddenItemsResetAtStorageKey) || 0);
          if (resetAt && Date.now() >= resetAt) {
            window.localStorage.removeItem(hiddenItemsStorageKey);
            window.localStorage.removeItem(hiddenItemsResetAtStorageKey);
            return [];
          }
          const raw = window.localStorage.getItem(hiddenItemsStorageKey);
          const parsed = raw ? JSON.parse(raw) : [];
          return Array.isArray(parsed) ? parsed : [];
        } catch (_error) {
          return [];
        }
      }

      function setHiddenItemIds(ids) {
        window.localStorage.setItem(hiddenItemsStorageKey, JSON.stringify(ids));
        if (ids.length && !Number(window.localStorage.getItem(hiddenItemsResetAtStorageKey) || 0)) {
          window.localStorage.setItem(hiddenItemsResetAtStorageKey, String(Date.now() + hiddenItemsResetAfterMs));
        }
      }

      function renderItemVisibility() {
        const hiddenIds = new Set(getHiddenItemIds());
        itemCards.forEach((card) => {
          const itemKey = card.dataset.itemKey;
          const shouldHide = Boolean(itemKey && hiddenIds.has(itemKey));
          card.hidden = shouldHide;
          card.classList.toggle("is-hidden-by-user", shouldHide);
        });
        newsCategoryPanels.forEach((panel) => {
          const visibleCards = Array.from(panel.querySelectorAll("[data-item-key]")).filter((card) => !card.hidden);
          const emptyState = panel.querySelector(".news-category-empty");
          if (emptyState) {
            emptyState.hidden = visibleCards.length > 0;
          }
        });
        updateCategoryBlocks();
      }

      function hideItem(item) {
        const card = typeof item === "string" ? itemCards.find((candidate) => candidate.dataset.itemKey === item) : item;
        const itemKey = typeof item === "string" ? item : card?.dataset.itemKey;
        if (!itemKey) {
          return;
        }
        const wasNavigationCurrent = Boolean(card?.classList.contains("is-navigation-current"));
        const navigationCards = card ? getNewsNavigationCards(card) : [];
        const navigationIndex = card ? navigationCards.indexOf(card) : -1;
        const hiddenIds = getHiddenItemIds();
        if (!hiddenIds.includes(itemKey)) {
          hiddenIds.push(itemKey);
          setHiddenItemIds(hiddenIds);
        }
        renderItemVisibility();
        if (wasNavigationCurrent && card && navigationCards.length > 1) {
          const nextIndex = (navigationIndex + 1 + navigationCards.length) % navigationCards.length;
          const target = navigationCards[nextIndex] === card ? navigationCards[(nextIndex + 1) % navigationCards.length] : navigationCards[nextIndex];
          if (target && !target.hidden) {
            showNewsNavigationTarget(card, target);
          }
        } else if (wasNavigationCurrent && card) {
          const category = card.closest(".category-block");
          const tabPanel = card.closest(".tab-panel");
          clearNewsNavigation(category || tabPanel || document);
        }
        ensureActiveNewsCategory();
        if (slideshowEnabled) {
          const activePanel = getActivePanel();
          if (activePanel && activePanel.id === "news-panel") {
            const visiblePanels = getVisibleNewsPanels();
            if (visiblePanels.length) {
              newsCategoryCycleIndex = newsCategoryCycleIndex % visiblePanels.length;
            } else {
              newsCategoryCycleIndex = 0;
            }
          } else {
            const activeCards = getVisibleCardsForSlideshow();
            if (activeCards.length) {
              slideshowIndex = Math.min(slideshowIndex, activeCards.length - 1);
            } else {
              slideshowIndex = 0;
            }
          }
          refreshSlideshow();
        }
      }

      function getVisibleNewsButtons() {
        return newsFilterButtons.filter((button) => {
          const panel = document.getElementById(button.dataset.newsTarget || "");
          return Boolean(panel) && Array.from(panel.querySelectorAll("[data-item-key]")).some((card) => !card.hidden);
        });
      }

      function ensureActiveNewsCategory() {
        const visibleButtons = getVisibleNewsButtons();
        const activeButton = newsFilterButtons.find((button) => button.classList.contains("is-active"));
        if (!visibleButtons.length) {
          newsCategoryPanels.forEach((panel) => {
            panel.hidden = true;
            panel.classList.remove("is-active");
          });
          newsFilterButtons.forEach((button) => {
            button.classList.remove("is-active");
            button.setAttribute("aria-pressed", "false");
          });
          return;
        }
        if (!activeButton || !visibleButtons.includes(activeButton)) {
          activateNewsCategory(visibleButtons[0].dataset.newsTarget);
        }
      }

      function clearNewsRotation() {
        if (newsRotationTimer) {
          window.clearInterval(newsRotationTimer);
          newsRotationTimer = null;
        }
      }

      function startNewsRotation() {
        clearNewsRotation();
        newsRotationTimer = window.setInterval(() => {
          if (slideshowEnabled) {
            return;
          }
          const newsPanelActive = document.getElementById("news-panel")?.classList.contains("is-active");
          if (!newsPanelActive) {
            return;
          }
          const visibleButtons = getVisibleNewsButtons();
          if (visibleButtons.length <= 1) {
            return;
          }
          const currentIndex = visibleButtons.findIndex((button) => button.classList.contains("is-active"));
          const nextButton = visibleButtons[(currentIndex + 1) % visibleButtons.length];
          if (nextButton) {
            activateNewsCategory(nextButton.dataset.newsTarget);
          }
        }, 15000);
      }

      function getActivePanel() {
        return tabPanels.find((panel) => panel.classList.contains("is-active")) || null;
      }

      function getVisibleCardsForSlideshow() {
        const activePanel = getActivePanel();
        if (!activePanel) {
          return [];
        }
        return Array.from(activePanel.querySelectorAll("[data-item-key]")).filter((card) => !card.hidden);
      }

      function getVisibleNewsPanels() {
        return newsCategoryPanels.filter((panel) => Array.from(panel.querySelectorAll("[data-item-key]")).some((card) => !card.hidden));
      }

      function getNewsNavigationCards(card) {
        const category = card.closest(".category-block");
        const tabPanel = card.closest(".tab-panel");
        const scope = category || tabPanel || document;
        return Array.from(scope.querySelectorAll("[data-item-key]")).filter((candidate) => {
          return !candidate.hidden;
        });
      }

      function clearNewsNavigation(scope = document) {
        scope.querySelectorAll(".is-navigation-current, .is-navigation-hidden, .is-navigation-focus").forEach((card) => {
          card.classList.remove("is-navigation-current");
          card.classList.remove("is-navigation-hidden");
          card.classList.remove("is-navigation-focus");
        });
      }

      function showNewsNavigationTarget(fromCard, target) {
        const category = fromCard.closest(".category-block");
        const tabPanel = fromCard.closest(".tab-panel");
        const scope = category || tabPanel || document;
        const cards = Array.from(scope.querySelectorAll("[data-item-key]")).filter((candidate) => !candidate.hidden);
        cards.forEach((candidate) => {
          const active = candidate === target;
          candidate.classList.toggle("is-navigation-current", active);
          candidate.classList.toggle("is-navigation-hidden", !active);
          candidate.classList.toggle("is-navigation-focus", active);
        });
        window.setTimeout(() => target.classList.remove("is-navigation-focus"), 900);
        updateCategoryBlocks();
      }

      function navigateNewsCard(card, direction) {
        const cards = getNewsNavigationCards(card);
        if (!cards.length) {
          return;
        }
        const currentIndex = cards.indexOf(card);
        const baseIndex = currentIndex >= 0 ? currentIndex : 0;
        const offset = direction === "previous" ? -1 : 1;
        const target = cards[(baseIndex + offset + cards.length) % cards.length];
        showNewsNavigationTarget(card, target);
      }

      function getNewsSlideshowTarget() {
        const visiblePanels = getVisibleNewsPanels();
        if (!visiblePanels.length) {
          return null;
        }
        newsCategoryCycleIndex = newsCategoryCycleIndex % visiblePanels.length;
        const panel = visiblePanels[newsCategoryCycleIndex];
        const panelId = panel.id;
        const cards = Array.from(panel.querySelectorAll("[data-item-key]")).filter((card) => !card.hidden);
        if (!cards.length) {
          return null;
        }
        const offset = Number(newsCategoryOffsets[panelId] || 0) % cards.length;
        return { panelId, card: cards[offset] };
      }

      function updateCategoryBlocks() {
        const categoryBlocks = Array.from(document.querySelectorAll(".category-block"));
        categoryBlocks.forEach((block) => {
          const blockCards = Array.from(block.querySelectorAll("[data-item-key]"));
          if (!blockCards.length) {
            return;
          }
          const visibleCards = blockCards.filter((card) => {
            return !card.hidden && !card.classList.contains("is-slideshow-hidden") && !card.classList.contains("is-navigation-hidden");
          });
          const emptyState = block.querySelector(".news-category-empty");
          if (emptyState && !emptyState.hidden) {
            block.hidden = false;
            return;
          }
          block.hidden = visibleCards.length === 0;
        });
      }

      function renderSlideshowFrame() {
        itemCards.forEach((card) => {
          card.classList.remove("is-slideshow-current");
          card.classList.remove("is-slideshow-hidden");
        });
        if (!slideshowEnabled) {
          updateCategoryBlocks();
          return;
        }
        const activePanel = getActivePanel();
        if (activePanel && activePanel.id === "news-panel") {
          const newsTarget = getNewsSlideshowTarget();
          if (!newsTarget) {
            updateCategoryBlocks();
            return;
          }
          activateNewsCategory(newsTarget.panelId);
          itemCards.forEach((card) => {
            const active = card === newsTarget.card;
            card.classList.toggle("is-slideshow-current", active);
            card.classList.toggle("is-slideshow-hidden", !active);
          });
          updateCategoryBlocks();
          return;
        }
        const activeCards = getVisibleCardsForSlideshow();
        if (!activeCards.length) {
          updateCategoryBlocks();
          return;
        }
        slideshowIndex = slideshowIndex % activeCards.length;
        activeCards.forEach((card, index) => {
          const active = index === slideshowIndex;
          card.classList.toggle("is-slideshow-current", active);
          card.classList.toggle("is-slideshow-hidden", !active);
        });
        updateCategoryBlocks();
      }

      function clearSlideshow() {
        if (slideshowTimer) {
          window.clearInterval(slideshowTimer);
          slideshowTimer = null;
        }
      }

      function syncSlideshowButton() {
        slideshowToggleButton.textContent = slideshowEnabled ? "슬라이드쇼 중지" : "슬라이드쇼 시작";
        slideshowToggleButton.setAttribute("aria-pressed", String(slideshowEnabled));
      }

      function refreshSlideshow() {
        if (slideshowEnabled) {
          clearNewsNavigation();
        }
        clearSlideshow();
        syncSlideshowButton();
        renderSlideshowFrame();
        if (!slideshowEnabled) {
          return;
        }
        slideshowTimer = window.setInterval(() => {
          const activePanel = getActivePanel();
          if (activePanel && activePanel.id === "news-panel") {
            const visiblePanels = getVisibleNewsPanels();
            if (!visiblePanels.length) {
              return;
            }
            const currentPanelId = visiblePanels[newsCategoryCycleIndex % visiblePanels.length].id;
            const currentCards = Array.from(document.getElementById(currentPanelId)?.querySelectorAll("[data-item-key]") || []).filter((card) => !card.hidden);
            if (currentCards.length) {
              newsCategoryOffsets[currentPanelId] = (Number(newsCategoryOffsets[currentPanelId] || 0) + 1) % currentCards.length;
            }
            newsCategoryCycleIndex = (newsCategoryCycleIndex + 1) % visiblePanels.length;
            renderSlideshowFrame();
            return;
          }
          const activeCards = getVisibleCardsForSlideshow();
          if (!activeCards.length) {
            return;
          }
          slideshowIndex = (slideshowIndex + 1) % activeCards.length;
          renderSlideshowFrame();
        }, 15000);
      }

      function renderStatus(status) {
        const total = Number(status.total_items || 0);
        const current = Number(status.current_index || 0);
        const stored = Number(status.stored || 0);
        const fetched = Number(status.fetched || 0);
        const errors = Number(status.errors || 0);
        let percent = 0;
        if (status.phase === "completed") {
          percent = 100;
        } else if (total > 0) {
          percent = Math.min(100, Math.round((Math.max(current, stored) / total) * 100));
        }
        progressFill.style.width = `${percent}%`;
        progressMeta.textContent = `${status.message || "진행 중"} · 수집 ${fetched}건 · 저장 ${stored}건 · 오류 ${errors}건`;
        progressTitle.textContent = status.current_title || status.current_source || "";
        if (status.running) {
          statusText.textContent = `${status.phase} 진행 중...`;
        } else if (status.phase === "completed") {
          statusText.textContent = `완료: ${stored}건 저장 / 모드 ${status.mode}`;
        }
      }

      async function pollStatus() {
        const response = await fetch("/api/status");
        const status = await response.json();
        renderStatus(status);
        if (status.running) {
          observedCollectionRunning = true;
          pollTimer = window.setTimeout(pollStatus, 1200);
          return;
        }
        if (awaitingRunCompletion && status.phase === "completed") {
          awaitingRunCompletion = false;
          window.location.reload();
          return;
        }
        if (observedCollectionRunning && status.phase === "completed") {
          observedCollectionRunning = false;
          window.location.reload();
          return;
        }
        pollTimer = window.setTimeout(pollStatus, 60000);
      }

      form.addEventListener("submit", async (event) => {
        event.preventDefault();
        if (pollTimer) {
          window.clearTimeout(pollTimer);
          pollTimer = null;
        }
        statusText.textContent = "작업을 시작합니다...";
        const response = await fetch("/run", { method: "POST" });
        const result = await response.json();
        awaitingRunCompletion = Boolean(result.started);
        renderStatus(result.status);
        pollStatus();
      });

      tabButtons.forEach((button) => {
        button.addEventListener("click", () => activateTab(button.dataset.tabTarget));
      });

      newsFilterButtons.forEach((button) => {
        button.addEventListener("click", () => {
          activateNewsCategory(button.dataset.newsTarget);
          const targetIndex = getVisibleNewsPanels().findIndex((panel) => panel.id === button.dataset.newsTarget);
          if (targetIndex >= 0) {
            newsCategoryCycleIndex = targetIndex;
          }
          if (slideshowEnabled) {
            renderSlideshowFrame();
          } else {
            startNewsRotation();
          }
        });
      });

      itemCards.forEach((card) => {
        const button = card.querySelector(".item-hide-button");
        if (!button) {
          return;
        }
        button.addEventListener("click", () => hideItem(card));
      });

      itemCards.forEach((card) => {
        card.querySelectorAll(".item-nav-button").forEach((button) => {
          button.addEventListener("click", () => navigateNewsCard(card, button.dataset.itemNav || button.dataset.newsNav));
        });
      });

      slideshowToggleButton.addEventListener("click", () => {
        slideshowEnabled = !slideshowEnabled;
        slideshowIndex = 0;
        newsCategoryCycleIndex = 0;
        refreshSlideshow();
      });

      renderItemVisibility();
      ensureActiveNewsCategory();
      startNewsRotation();
      refreshSlideshow();
      pollStatus();
    </script>
    """
    return f"""<!doctype html>
<html lang=\"ko\">
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>Morning Market Dashboard</title>
    <link rel=\"stylesheet\" href=\"/static/styles.css\" />
  </head>
  <body>
    <main class=\"page\">
      <section class=\"hero\">
        <div>
          <p class=\"eyebrow\">AI Morning Briefing</p>
          <h1>어제 16시 이후 리포트, 공시, 뉴스를<br />읽지 않은 것만 골라 봅니다.</h1>
          <p class=\"lead\">
            시장 리포트와 공식 공시, 주요 뉴스를 한곳에 모으되 길게 쌓이지 않도록 전환형 대시보드로 정리했습니다.
          </p>
        </div>
        <form id=\"run-form\" class=\"run-form\">
          <button type=\"submit\">지금 수집 실행</button>
          <p id=\"status-text\">서버가 시작되면 즉시 수집하고, 이후 3시간마다 자동으로 갱신합니다.</p>
          <div class=\"progress-panel\" id=\"progress-panel\">
            <div class=\"progress-bar\">
              <div class=\"progress-fill\" id=\"progress-fill\"></div>
            </div>
            <p class=\"progress-meta\" id=\"progress-meta\">진행 대기 중</p>
            <p class=\"progress-title\" id=\"progress-title\"></p>
          </div>
          <div class=\"hero-stats\">
            <div class=\"stat-box\">
              <strong>{len(reports)}</strong>
              <span>리포트</span>
            </div>
            <div class=\"stat-box\">
              <strong>{len(disclosures)}</strong>
              <span>공시</span>
            </div>
            <div class=\"stat-box\">
              <strong>{len(news_items)}</strong>
              <span>뉴스</span>
            </div>
          </div>
        </form>
      </section>
      {spotlight_cards}
      <section class=\"section-block content-switcher\">
        <div class=\"section-head section-head-tabs\">
          <div>
            <p class=\"eyebrow\">View</p>
            <h2>탭으로 전환해 읽기</h2>
          </div>
          <p class=\"section-copy\">전체 흐름을 먼저 보고, 리포트나 공시만 따로 좁혀 읽을 수 있습니다.</p>
        </div>
        <div class=\"tab-bar\" role=\"tablist\" aria-label=\"자료 보기 전환\">
          <button type=\"button\" class=\"tab-button is-active\" data-tab-target=\"all-panel\" id=\"all-tab\" role=\"tab\" aria-selected=\"true\" aria-controls=\"all-panel\">
            전체
            <span>{len(rows)}</span>
          </button>
          <button type=\"button\" class=\"tab-button\" data-tab-target=\"reports-panel\" id=\"reports-tab\" role=\"tab\" aria-selected=\"false\" aria-controls=\"reports-panel\">
            리포트
            <span>{len(reports)}</span>
          </button>
          <button type=\"button\" class=\"tab-button\" data-tab-target=\"disclosures-panel\" id=\"disclosures-tab\" role=\"tab\" aria-selected=\"false\" aria-controls=\"disclosures-panel\">
            공시
            <span>{len(disclosures)}</span>
          </button>
          <button type=\"button\" class=\"tab-button\" data-tab-target=\"news-panel\" id=\"news-tab\" role=\"tab\" aria-selected=\"false\" aria-controls=\"news-panel\">
            뉴스
            <span>{len(news_items)}</span>
          </button>
        </div>
        <div class=\"panel-meta\">
          <p id=\"panel-description\">리포트와 공시를 시간순으로 함께 보여줍니다.</p>
          <button type=\"button\" class=\"slideshow-toggle-button\" id=\"slideshow-toggle\" aria-pressed=\"false\">슬라이드쇼 시작</button>
        </div>
        <div class=\"tab-panel is-active\" id=\"all-panel\" role=\"tabpanel\" aria-labelledby=\"all-tab\">
          <div class=\"grid\">
            {all_cards}
          </div>
        </div>
        <div class=\"tab-panel\" id=\"reports-panel\" role=\"tabpanel\" aria-labelledby=\"reports-tab\" hidden>
          <div class=\"grid\">
            {report_cards}
          </div>
        </div>
        <div class=\"tab-panel\" id=\"disclosures-panel\" role=\"tabpanel\" aria-labelledby=\"disclosures-tab\" hidden>
          <div class=\"grid\">
            {disclosure_cards}
          </div>
        </div>
        <div class=\"tab-panel\" id=\"news-panel\" role=\"tabpanel\" aria-labelledby=\"news-tab\" hidden>
          <div class=\"grid\">
            {news_cards}
          </div>
        </div>
      </section>
    </main>
    {script}
  </body>
</html>"""


def is_disclosure(item: dict[str, str]) -> bool:
    return "공시" in item.get("source_type", "")


def is_news(item: dict[str, str]) -> bool:
    return item.get("source_type", "") == "뉴스"


def is_report(item: dict[str, str]) -> bool:
    return not is_disclosure(item) and not is_news(item)


def get_news_category(item: dict[str, str]) -> str:
    source_name = item.get("source_name", "")
    if "네이버 뉴스 " in source_name:
        return source_name.removeprefix("네이버 뉴스 ").strip() or "뉴스"
    if "클리앙" in source_name or "다모앙" in source_name:
        return "커뮤니티"
    tags = [part.strip() for part in item.get("tags", "").split(",") if part.strip()]
    return tags[0] if tags else "기타"


def compact_summary(summary: str, *, max_length: int = 140) -> str:
    for line in summary.splitlines():
        text = line.strip()
        if not text:
            continue
        for prefix in ("핵심:", "영향:", "체크포인트:", "비고:"):
            if text.startswith(prefix):
                text = text.removeprefix(prefix).strip()
                break
        if text:
            return text[:max_length].rstrip() + ("..." if len(text) > max_length else "")
    flat = " ".join(summary.split())
    return flat[:max_length].rstrip() + ("..." if len(flat) > max_length else "")


def canonical_item_key(item: dict[str, str]) -> str:
    source_name = item.get("source_name", "").strip().lower()
    source_type = item.get("source_type", "").strip().lower()
    published_at = item.get("published_at", "").strip()
    url = (item.get("url") or "").strip()
    if url:
        parsed = urlparse(url)
        host = parsed.netloc.lower().replace("www.", "")
        path = parsed.path.rstrip("/")
        query = "&".join(
            f"{key}={value}"
            for key, values in sorted(parse_qs(parsed.query).items())
            if not key.lower().startswith("utm_")
            for value in values
        )
        url_key = f"{host}{path}?{query}" if query else f"{host}{path}"
        return f"{source_type}|{source_name}|{url_key}|{published_at}"
    normalized_title = re.sub(r"\s+", " ", item.get("title", "").strip().lower())
    return f"{source_type}|{source_name}|{normalized_title}|{published_at}"


def render_spotlight_section(
    reports: list[dict[str, str]],
    disclosures: list[dict[str, str]],
    news_items: list[dict[str, str]],
) -> str:
    spotlight_items: list[tuple[str, str, dict[str, str]]] = []
    if reports:
        spotlight_items.append(("리포트", get_report_category(reports[0]), reports[0]))
    if disclosures:
        spotlight_items.append(("공시", get_disclosure_category(disclosures[0]), disclosures[0]))
    if news_items:
        spotlight_items.append(("뉴스", get_news_category(news_items[0]), news_items[0]))
    if not spotlight_items:
        return ""
    cards = []
    for label, category, item in spotlight_items:
        item_key = html.escape(canonical_item_key(item))
        spotlight_attrs = f' data-item-key="{item_key}"'
        cards.append(
            f"""
          <article class=\"spotlight-card\"{spotlight_attrs}>
            <div class=\"spotlight-meta\">
              <span class=\"spotlight-pill\">{html.escape(label)}</span>
              <span class=\"spotlight-category\">{html.escape(category)}</span>
            </div>
            <h3><a href=\"{html.escape(item.get('url') or '#')}\" target=\"_blank\" rel=\"noreferrer\">{html.escape(item['title'])}</a></h3>
            <p class=\"spotlight-summary\">{html.escape(compact_summary(item.get('summary', '')))}</p>
            <p class=\"spotlight-source\">{html.escape(item.get('source_name', ''))}</p>
          </article>
        """
        )
    return f"""
      <section class=\"section-block spotlight-section\">
        <div class=\"section-head\">
          <div>
            <p class=\"eyebrow\">Spotlight</p>
            <h2>오늘 주목할 항목</h2>
          </div>
          <p class=\"section-copy\">리포트, 공시, 뉴스에서 가장 먼저 볼 항목만 한 번 더 짧게 압축했습니다.</p>
        </div>
        <div class=\"spotlight-grid\">
          {''.join(cards)}
        </div>
      </section>
    """


def get_report_category(item: dict[str, str]) -> str:
    source_name = item.get("source_name", "")
    if "종목분석" in source_name:
        return "기업"
    if "산업분석" in source_name:
        return "산업"
    if "시황정보" in source_name:
        return "시황"
    if "투자전략" in source_name:
        return "전략"
    if "경제분석" in source_name:
        return "경제"
    if "한경 컨센서스" in source_name:
        tags = [part.strip() for part in item.get("tags", "").split(",") if part.strip()]
        return tags[0] if tags else "컨센서스"
    return "기타 리포트"


def get_disclosure_category(item: dict[str, str]) -> str:
    source_name = item.get("source_name", "")
    tags = [part.strip() for part in item.get("tags", "").split(",") if part.strip()]
    if "OpenDART" in source_name:
        return "DART 공시"
    if "KIND" in source_name:
        if len(tags) >= 2 and tags[1]:
            return f"KIND {tags[1]}"
        return "KIND 공시"
    if "EDGAR" in source_name or item.get("source_type") == "미국 공시":
        return "미국 공시"
    return "기타 공시"


def render_grouped_sections(
    items: list[dict[str, str]],
    *,
    empty_message: str,
    category_order: list[str],
    category_getter,
    block_class: str,
) -> str:
    if not items:
        return render_empty(empty_message)
    grouped: dict[str, list[dict[str, str]]] = {}
    for item in items:
        grouped.setdefault(category_getter(item), []).append(item)
    sections: list[str] = []
    for category in category_order:
        category_items = grouped.pop(category, [])
        if not category_items:
            continue
        cards = "\n".join(render_card(item) for item in category_items)
        sections.append(
            f"""
          <section class=\"{block_class}\">
            <div class=\"category-head\">
              <h3>{html.escape(category)}</h3>
              <span>{len(category_items)}건</span>
            </div>
            <div class=\"grid\">
              {cards}
            </div>
          </section>
        """
        )
    for category, category_items in grouped.items():
        cards = "\n".join(render_card(item) for item in category_items)
        sections.append(
            f"""
          <section class=\"{block_class}\">
            <div class=\"category-head\">
              <h3>{html.escape(category)}</h3>
              <span>{len(category_items)}건</span>
            </div>
            <div class=\"grid\">
              {cards}
            </div>
          </section>
        """
        )
    return "\n".join(sections)


def render_report_sections(items: list[dict[str, str]]) -> str:
    return render_grouped_sections(
        items,
        empty_message="리포트가 아직 없습니다.",
        category_order=["기업", "산업", "시황", "전략", "경제", "컨센서스", "기타 리포트"],
        category_getter=get_report_category,
        block_class="category-block report-category-block",
    )


def render_disclosure_sections(items: list[dict[str, str]]) -> str:
    return render_grouped_sections(
        items,
        empty_message="공시가 아직 없습니다.",
        category_order=["DART 공시", "KIND 공시", "미국 공시", "기타 공시"],
        category_getter=get_disclosure_category,
        block_class="category-block disclosure-category-block",
    )


def render_news_sections(items: list[dict[str, str]]) -> str:
    if not items:
        return render_empty("뉴스가 아직 없습니다.")
    category_order = ["경제", "사회", "생활/문화", "세계", "IT/과학", "커뮤니티", "기타"]
    grouped: dict[str, list[dict[str, str]]] = {}
    for item in items:
        grouped.setdefault(get_news_category(item), []).append(item)
    ordered_categories = [category for category in category_order if grouped.get(category)]
    ordered_categories.extend(category for category in grouped if category not in ordered_categories)
    first_category = ordered_categories[0]
    buttons = []
    panels = []
    for category in ordered_categories:
        category_items = grouped[category]
        category_id = category.replace("/", "-").replace(" ", "-")
        cards = "\n".join(render_card(item) for item in category_items)
        is_active = category == first_category
        buttons.append(
            f"""
            <button
              type=\"button\"
              class=\"news-filter-button{' is-active' if is_active else ''}\"
              data-news-target=\"news-category-{html.escape(category_id)}\"
              aria-pressed=\"{str(is_active).lower()}\"
            >
              {html.escape(category)}
              <span>{len(category_items)}</span>
            </button>
            """
        )
        panels.append(
            f"""
          <section class=\"category-block news-category-block{' is-active' if is_active else ''}\" id=\"news-category-{html.escape(category_id)}\" {'hidden' if not is_active else ''}>
            <div class=\"category-head\">
              <h3>{html.escape(category)}</h3>
              <span>{len(category_items)}건</span>
            </div>
            <div class=\"grid\">
              {cards}
            </div>
            <div class=\"news-category-empty\" hidden>
              읽지 않은 뉴스가 없습니다.
            </div>
          </section>
        """
        )
    return f"""
      <div class=\"news-filter-bar\" role=\"tablist\" aria-label=\"뉴스 카테고리 전환\">
        {''.join(buttons)}
      </div>
      {''.join(panels)}
    """


def render_empty(message: str) -> str:
    return f"""
      <div class=\"empty-state\">
        <p>{html.escape(message)}</p>
        <p>상단의 수집 버튼으로 최신 자료를 다시 불러올 수 있습니다.</p>
      </div>
    """


def render_card(item: dict[str, str]) -> str:
    if is_disclosure(item):
        card_class = "card is-disclosure"
    elif is_news(item):
        card_class = "card is-news"
    else:
        card_class = "card is-report"
    title = html.escape(item["title"])
    source_name = html.escape(item["source_name"])
    source_type = html.escape(item["source_type"])
    summary = "<br />".join(html.escape(line) for line in item["summary"].splitlines())
    tags = html.escape(item.get("tags", ""))
    link = item.get("url") or "#"
    published_at = html.escape(item["published_at"])
    item_id = html.escape(str(item.get("id", "")))
    item_key = html.escape(canonical_item_key(item))
    item_kind = "news" if is_news(item) else "disclosure" if is_disclosure(item) else "report"
    item_attrs = f' data-item-id="{item_id}" data-item-key="{item_key}" data-item-kind="{item_kind}"'
    item_nav_buttons = """
            <button type="button" class="item-nav-button" data-item-nav="previous" aria-label="이전 항목으로 이동">이전</button>
            <button type="button" class="item-nav-button" data-item-nav="next" aria-label="다음 항목으로 이동">이후</button>
        """
    hide_button = """
        <button type="button" class="item-hide-button" aria-label="읽은 항목 숨기기">읽음</button>
    """
    return f"""
      <article class=\"{card_class}\"{item_attrs}>
        <div class=\"meta-row\">
          <span class=\"pill\">{source_type}</span>
          <span class=\"meta\">{source_name}</span>
          <span class=\"meta\">{published_at}</span>
          <span class=\"meta-actions\">
            {item_nav_buttons}
            {hide_button}
          </span>
        </div>
        <h3><a href=\"{html.escape(link)}\" target=\"_blank\" rel=\"noreferrer\">{title}</a></h3>
        <p class=\"summary\">{summary}</p>
        <p class=\"tags\">{tags}</p>
      </article>
    """


def get_job_state() -> dict[str, object]:
    with JOB_LOCK:
        return dict(JOB_STATE)


def update_job_state(payload: dict[str, object]) -> None:
    with JOB_LOCK:
        JOB_STATE.update(payload)


def start_background_run(*, trigger: str = "manual") -> bool:
    with JOB_LOCK:
        if JOB_STATE.get("running"):
            return False
        JOB_STATE.update(
            {
                "running": True,
                "phase": "starting",
                "message": "작업을 시작합니다.",
                "fetched": 0,
                "stored": 0,
                "total_items": 0,
                "current_index": 0,
                "current_title": "",
                "current_source": "",
                "errors": 0,
                "mode": "",
                "trigger": trigger,
                "last_result": None,
            }
        )

    def worker() -> None:
        try:
            result = run_pipeline(progress_callback=update_job_state)
            update_job_state(
                {
                    "running": False,
                    "phase": "completed",
                    "message": f"완료: {result.stored}건 저장",
                    "fetched": result.fetched,
                    "stored": result.stored,
                    "total_items": result.fetched,
                    "current_index": result.fetched,
                    "errors": len(result.errors),
                    "mode": result.mode,
                    "last_result": pipeline_result_to_dict(result),
                }
            )
        except Exception as exc:  # pragma: no cover - server safety
            update_job_state(
                {
                    "running": False,
                    "phase": "failed",
                    "message": f"실패: {exc}",
                    "errors": int(JOB_STATE.get("errors", 0)) + 1,
                }
            )

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    return True


def start_auto_collection_scheduler() -> None:
    def scheduler() -> None:
        while True:
            start_background_run(trigger="scheduled")
            time.sleep(AUTO_RUN_INTERVAL_SECONDS)

    thread = threading.Thread(target=scheduler, daemon=True)
    thread.start()


def main() -> None:
    init_db()
    start_auto_collection_scheduler()
    server = ReusableThreadingHTTPServer(("127.0.0.1", 8000), DashboardHandler)
    print("Dashboard running on http://127.0.0.1:8000")
    server.serve_forever()


if __name__ == "__main__":
    main()
