// Live log stream for the Execution Inspection tab. Plain JS (no Alpine) because the
// hot path here is "append one DOM node per incoming SSE event", which Alpine's
// reactive re-diffing isn't a good fit for. Container/level checkboxes never
// re-open the stream -- everything running is always sent, and the checkboxes just
// show/hide already-rendered entries via a `hidden` toggle.
//
// Re-runs on every htmx swap into #tab-content (htmx executes <script> tags found in
// swapped content), which is what we want: a fresh EventSource per tab visit, closed
// via htmx:beforeCleanupElement when the tab is navigated away from.
(function () {
  var root = document.getElementById("live-logs");
  if (!root) return;

  var pane = document.getElementById("live-log-pane");
  var statusEl = document.getElementById("live-logs-status");
  var clearBtn = document.getElementById("live-logs-clear");
  var autoscrollBox = document.getElementById("live-logs-autoscroll");
  var containerFieldset = document.getElementById("live-container-filter");
  if (!pane || !statusEl || !containerFieldset) return;

  var MAX_LINES = 500;
  // Must match _NO_TASK_LABEL in routes/execution_inspection.py: the bucket for lines
  // that aren't attributable to a scheduled task (non-workflow containers, and
  // workflow's own startup/idle chatter before its first task starts).
  var NO_TASK_LABEL = "Other";

  function containerBoxes() {
    return containerFieldset.querySelectorAll(".live-container-toggle");
  }

  function levelBoxes() {
    return root.querySelectorAll(".live-level-toggle");
  }

  function taskBoxes() {
    return root.querySelectorAll(".live-task-toggle");
  }

  function hiddenSet(boxes) {
    var hidden = {};
    boxes.forEach(function (box) {
      if (!box.checked) hidden[box.value] = true;
    });
    return hidden;
  }

  function updateVisibility() {
    var hiddenContainers = hiddenSet(containerBoxes());
    var hiddenLevels = hiddenSet(levelBoxes());
    var hiddenTasks = hiddenSet(taskBoxes());
    pane.querySelectorAll(".log-entry").forEach(function (entry) {
      entry.hidden = !!hiddenContainers[entry.dataset.container] ||
        !!hiddenLevels[entry.dataset.level] ||
        !!hiddenTasks[entry.dataset.task];
    });
  }

  function appendEntry(data) {
    var taskLabel = data.task || NO_TASK_LABEL;

    var el = document.createElement("div");
    el.className = "log-entry";
    el.dataset.container = data.container;
    el.dataset.level = data.level;
    el.dataset.time = data.time || "";
    el.dataset.task = taskLabel;

    var levelSpan = document.createElement("span");
    levelSpan.className = "log-level log-level-" + String(data.level).toLowerCase();
    levelSpan.textContent = (data.time ? data.time + " " : "") + data.level;

    var containerSpan = document.createElement("span");
    containerSpan.className = "log-event";
    containerSpan.textContent = "[" + data.container + "]";

    var taskSpan = document.createElement("span");
    taskSpan.className = "log-task";
    taskSpan.textContent = "(" + taskLabel + ")";

    var messageSpan = document.createElement("span");
    messageSpan.className = "log-message";
    messageSpan.textContent = data.message;

    el.appendChild(levelSpan);
    el.appendChild(containerSpan);
    el.appendChild(taskSpan);
    el.appendChild(messageSpan);

    el.hidden = !!hiddenSet(containerBoxes())[data.container] ||
      !!hiddenSet(levelBoxes())[data.level] ||
      !!hiddenSet(taskBoxes())[taskLabel];

    insertInTimeOrder(el);
    while (pane.children.length > MAX_LINES) {
      pane.removeChild(pane.firstChild);
    }
    if (!autoscrollBox || autoscrollBox.checked) {
      pane.scrollTop = pane.scrollHeight;
    }
  }

  // Five containers' `docker logs -f` subprocesses each deliver their own lines in
  // order, but arrive at the browser interleaved by process/network timing, not by
  // the line's own timestamp -- most noticeably in the initial --tail backlog burst,
  // where a quiet container's few old lines can arrive after another container's
  // fresh ones. Insert by timestamp (a zero-padded "YYYY-MM-DD HH:MM:SS" string, so
  // plain `>` comparison sorts correctly) so the pane reads chronologically and
  // autoscroll-to-bottom always lands on the truly newest line, not just the
  // most-recently-arrived one. Lines without a parsed timestamp go straight to the end.
  function insertInTimeOrder(el) {
    var timeStr = el.dataset.time;
    if (!timeStr) {
      pane.appendChild(el);
      return;
    }
    var children = pane.children;
    var i = children.length - 1;
    while (i >= 0 && children[i].dataset.time > timeStr) {
      i--;
    }
    if (i === children.length - 1) {
      pane.appendChild(el);
    } else {
      pane.insertBefore(el, children[i + 1]);
    }
  }

  function addContainerCheckbox(name) {
    var label = document.createElement("label");
    label.className = "level-checkbox";
    var input = document.createElement("input");
    input.type = "checkbox";
    input.className = "live-container-toggle";
    input.value = name;
    input.checked = true;
    input.addEventListener("change", updateVisibility);
    label.appendChild(input);
    label.appendChild(document.createTextNode(" " + name));
    containerFieldset.appendChild(label);
  }

  containerBoxes().forEach(function (box) { box.addEventListener("change", updateVisibility); });
  levelBoxes().forEach(function (box) { box.addEventListener("change", updateVisibility); });
  taskBoxes().forEach(function (box) { box.addEventListener("change", updateVisibility); });
  if (clearBtn) clearBtn.addEventListener("click", function () { pane.innerHTML = ""; });

  if (!window.EventSource) {
    statusEl.textContent = "Live streaming isn't supported in this browser.";
    return;
  }

  var es = new EventSource(root.dataset.streamUrl);

  es.addEventListener("open", function () { statusEl.textContent = "Live"; });

  es.addEventListener("init", function (evt) {
    try {
      var payload = JSON.parse(evt.data);
      var known = {};
      containerBoxes().forEach(function (box) { known[box.value] = true; });
      (payload.containers || []).forEach(function (name) {
        if (!known[name]) addContainerCheckbox(name);
      });
    } catch (err) {
      // Malformed init payload -- checkbox list just stays as server-rendered.
    }
  });

  es.addEventListener("log", function (evt) {
    try {
      appendEntry(JSON.parse(evt.data));
    } catch (err) {
      // Skip unparseable line rather than breaking the whole stream.
    }
  });

  es.addEventListener("error", function (evt) {
    if (evt.data) {
      try {
        statusEl.textContent = JSON.parse(evt.data).message || "Stream error.";
        return;
      } catch (err) {
        // Fall through to generic message below.
      }
    }
    statusEl.textContent = "Disconnected -- retrying...";
  });

  root.addEventListener("htmx:beforeCleanupElement", function () { es.close(); });
  window.addEventListener("beforeunload", function () { es.close(); });
})();
