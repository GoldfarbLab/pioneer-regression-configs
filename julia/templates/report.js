function parseCellValue(text) {
  const trimmed = text.trim();
  if (!trimmed || trimmed === "NA") return NaN;
  const cleaned = trimmed.replace(/[%+,]/g, "");
  const value = parseFloat(cleaned);
  return Number.isNaN(value) ? NaN : value;
}
function sortTable(table, columnIndex, direction) {
  const tbody = table.tBodies[0];
  if (!tbody) return;
  const rows = Array.from(tbody.rows);
  rows.sort((rowA, rowB) => {
    const cellA = rowA.cells[columnIndex];
    const cellB = rowB.cells[columnIndex];
    const textA = cellA ? cellA.textContent : "";
    const textB = cellB ? cellB.textContent : "";
    const numA = parseCellValue(textA);
    const numB = parseCellValue(textB);
    let result = 0;
    if (!Number.isNaN(numA) && !Number.isNaN(numB)) {
      result = numA - numB;
    } else if (!Number.isNaN(numA)) {
      result = 1;
    } else if (!Number.isNaN(numB)) {
      result = -1;
    } else {
      result = textA.localeCompare(textB);
    }
    return direction === "asc" ? result : -result;
  });
  rows.forEach(row => tbody.appendChild(row));
  table.dataset.sortIndex = columnIndex;
  table.dataset.sortDirection = direction;
}
function setupSortableTables() {
  const tables = document.querySelectorAll("table.metrics-table");
  tables.forEach(table => {
    const headers = table.querySelectorAll("thead th");
    headers.forEach((header, index) => {
      header.classList.add("sortable");
      header.addEventListener("click", () => {
        const currentIndex = parseInt(table.dataset.sortIndex || "-1", 10);
        const currentDirection = table.dataset.sortDirection || "desc";
        const nextDirection = currentIndex === index && currentDirection === "desc" ? "asc" : "desc";
        sortTable(table, index, nextDirection);
      });
    });
    const initial = table.dataset.initialSort;
    if (initial !== undefined && initial !== "") {
      sortTable(table, parseInt(initial, 10), "desc");
    }
  });
}
function setupTableFilters() {
  const inputs = document.querySelectorAll(".table-filter");
  inputs.forEach(input => {
    const tableId = input.dataset.tableId;
    if (!tableId) return;
    const table = document.getElementById(tableId);
    if (!table) return;
    input.addEventListener("input", () => {
      const query = input.value.trim().toLowerCase();
      const rows = Array.from(table.tBodies[0].rows);
      rows.forEach(row => {
        const text = row.textContent.toLowerCase();
        const dataset = (row.dataset.dataset || "").toLowerCase();
        const search = (row.dataset.search || "").toLowerCase();
        const haystack = `${text} ${dataset} ${search}`;
        row.style.display = !query || haystack.includes(query) ? "" : "none";
      });
    });
  });
}
function collectSeries(table, mode) {
  const headers = Array.from(table.querySelectorAll("thead th")).map(th => th.textContent.trim());
  const versions = (table.dataset.versions || "").split("|").filter(Boolean);
  const rows = Array.from(table.tBodies[0].rows);
  if (!rows.length) return [];
  const datasetValues = rows.map(row => row.dataset.dataset || (row.cells[0] ? row.cells[0].textContent.trim() : ""));
  const searchValues = rows.map(row => row.dataset.search || "");
  const group = (table.dataset.metricGroup || "").toLowerCase();
  const speciesIndex = headers.indexOf("Species");
  const conditionIndex = headers.indexOf("Condition");
  const speciesValues = speciesIndex >= 0 ? rows.map(row => {
    const cell = row.cells[speciesIndex];
    return cell ? cell.textContent.trim() : "";
  }) : [];
  const conditionValues = conditionIndex >= 0 ? rows.map(row => {
    const cell = row.cells[conditionIndex];
    return cell ? cell.textContent.trim() : "";
  }) : [];
  const usesCompositeXAxis = (group === "fold_change" || group === "fold_change_variance") && speciesIndex >= 0 && conditionIndex >= 0;
  const xValues = usesCompositeXAxis ? rows.map((row, idx) => {
    const dataset = datasetValues[idx];
    const species = speciesValues[idx];
    const condition = conditionValues[idx];
    const parts = [];
    if (dataset) parts.push(dataset);
    if (species) parts.push(species);
    if (condition) parts.push(condition);
    return parts.join(" / ") || dataset || "";
  }) : datasetValues;
  const shouldSplitBySpecies = false;
  const columnIndexes = [];
  const labels = [];
  if (mode === "raw") {
    versions.forEach(version => {
      const idx = headers.indexOf(version);
      if (idx >= 0) {
        columnIndexes.push(idx);
        labels.push(version);
      }
    });
  } else if (mode === "delta") {
    headers.forEach((header, idx) => {
      if (header.startsWith("Δ ")) {
        columnIndexes.push(idx);
        labels.push(header);
      }
    });
  } else if (mode === "percent_delta") {
    headers.forEach((header, idx) => {
      if (header.startsWith("% ")) {
        columnIndexes.push(idx);
        labels.push(header);
      }
    });
  }
  if (!columnIndexes.length) return [];
  if (!shouldSplitBySpecies) {
    return labels.map((label, seriesIdx) => {
      const columnIndex = columnIndexes[seriesIdx];
      const yValues = rows.map(row => {
        const cell = row.cells[columnIndex];
        const value = cell ? parseCellValue(cell.textContent) : NaN;
        return Number.isNaN(value) ? null : value;
      });
      const hoverText = rows.map((row, idx) => {
        const dataset = xValues[idx];
        const search = searchValues[idx];
        return search ? `${dataset}<br>${search}` : dataset;
      });
      return {
        name: label,
        x: xValues,
        y: yValues,
        text: hoverText,
        hovertemplate: "%{text}<br>%{y}<extra>%{name}</extra>",
        mode: "lines+markers"
      };
    });
  }
  const seenSpecies = new Set();
  const speciesList = [];
  speciesValues.forEach(value => {
    if (seenSpecies.has(value)) return;
    seenSpecies.add(value);
    speciesList.push(value);
  });
  const series = [];
  labels.forEach((label, seriesIdx) => {
    const columnIndex = columnIndexes[seriesIdx];
    speciesList.forEach(species => {
      const x = [];
      const y = [];
      const hoverText = [];
      rows.forEach((row, idx) => {
        if (speciesValues[idx] !== species) return;
        x.push(xValues[idx]);
        const cell = row.cells[columnIndex];
        const value = cell ? parseCellValue(cell.textContent) : NaN;
        y.push(Number.isNaN(value) ? null : value);
        const dataset = xValues[idx];
        const search = searchValues[idx];
        hoverText.push(search ? `${dataset}<br>${search}` : dataset);
      });
      const hasValue = y.some(value => value !== null && value !== undefined);
      if (!hasValue) return;
      const speciesLabel = species || "Unknown";
      series.push({
        name: `${label} · ${speciesLabel}`,
        x,
        y,
        text: hoverText,
        hovertemplate: "%{text}<br>%{y}<extra>%{name}</extra>",
        mode: "lines+markers"
      });
    });
  });
  return series;
}
function choosePrimarySeries(series) {
  if (!series.length) return null;
  const byName = needle => series.find(trace => (trace.name || "").toLowerCase().includes(needle));
  const currentVsDevelop = byName("current vs develop");
  if (currentVsDevelop) return currentVsDevelop;
  const currentExact = series.find(trace => (trace.name || "").trim().toLowerCase() === "current");
  if (currentExact) return currentExact;
  return series[0];
}
function sortSeriesByY(series) {
  const primary = choosePrimarySeries(series);
  if (!primary) return series;
  const indices = primary.y.map((value, idx) => ({
    idx,
    value: value === null || value === undefined || Number.isNaN(value) ? -Infinity : value
  }));
  indices.sort((a, b) => b.value - a.value);
  const order = indices.map(entry => entry.idx);
  return series.map(trace => {
    return {
      ...trace,
      x: order.map(idx => trace.x[idx]),
      y: order.map(idx => trace.y[idx]),
      text: trace.text ? order.map(idx => trace.text[idx]) : trace.text
    };
  });
}
function setupTableCharts() {
  if (typeof Plotly === "undefined") return;
  const chartContainers = document.querySelectorAll(".table-chart");
  chartContainers.forEach(container => {
    const tableId = container.dataset.tableId;
    const mode = container.dataset.chartMode || "";
    if (!tableId || !mode) return;
    const table = document.getElementById(tableId);
    if (!table) return;
    const series = sortSeriesByY(collectSeries(table, mode));
    if (!series.length) {
      container.style.display = "none";
      return;
    }
    const showZeroLine = mode !== "raw";
    const group = (table.dataset.metricGroup || "").toLowerCase();
    const xAxisTitle = (group === "fold_change" || group === "fold_change_variance") ? "Dataset / Species / Condition" : "Dataset";
    const layout = {
      margin: { l: 50, r: 20, t: 10, b: 40 },
      height: 520,
      xaxis: { title: xAxisTitle, automargin: true },
      yaxis: { title: mode === "raw" ? "Value" : (mode === "percent_delta" ? "Δ %" : "Δ"), automargin: true },
      legend: { orientation: "h", x: 1, y: 1, xanchor: "right", yanchor: "top" },
      shapes: showZeroLine ? [{ type: "line", xref: "paper", x0: 0, x1: 1, y0: 0, y1: 0, line: { color: "#666", width: 1, dash: "dash" } }] : []
    };
    Plotly.newPlot(container, series, layout, { displaylogo: false, responsive: true });
  });
}
function applyValueCues() {
  const tables = document.querySelectorAll("table.metrics-table");
  tables.forEach(table => {
    const group = (table.dataset.metricGroup || "").toLowerCase();
    const headers = Array.from(table.querySelectorAll("thead th")).map(th => th.textContent.trim());
    const rows = Array.from(table.tBodies[0]?.rows || []);
    rows.forEach(row => {
      Array.from(row.cells).forEach((cell, idx) => {
        if (!cell.classList.contains("numeric")) return;
        const header = headers[idx] || "";
        const value = parseCellValue(cell.textContent);
        if (Number.isNaN(value)) return;
        const isDelta = header.startsWith("Δ ");
        const isPercent = header.startsWith("% ");
        const isRaw = !isDelta && !isPercent;
        let classification = null;
        if (group === "identification" && (isDelta || isPercent)) {
          if (value > 0) classification = "good";
          else if (value < 0) classification = "bad";
        } else if (group === "runtime" && (isDelta || isPercent)) {
          if (value < 0) classification = "good";
          else if (value > 0) classification = "bad";
        } else if (group === "cv" && isDelta) {
          if (value < 0) classification = "good";
          else if (value > 0) classification = "bad";
        } else if (group === "fold_change_variance" && isDelta) {
          if (value < 0) classification = "good";
          else if (value > 0) classification = "bad";
        } else if (group === "entrapment" && isRaw) {
          if (value < 0) classification = "good";
          else if (value > 0) classification = "bad";
        } else if (group === "ftr" && isRaw) {
          if (value < 0.01) classification = "good";
          else if (value > 0.01) classification = "bad";
        }
        if (!classification) return;
        cell.classList.remove("metric-good", "metric-bad");
        cell.classList.add(classification === "good" ? "metric-good" : "metric-bad");
      });
    });
  });
}
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", () => {
    setupSortableTables();
    setupTableFilters();
    setupTableCharts();
    applyValueCues();
  });
} else {
  setupSortableTables();
  setupTableFilters();
  setupTableCharts();
  applyValueCues();
}
