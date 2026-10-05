(() => {
  const failed = row => row.status != null && Number(row.status) !== 0;
  const slow = row => Number(row.slow_wr_count) > 0;
  const knownWorker = worker => worker && worker !== '未明确' && worker !== 'unknown';
  const percentile = (values, fraction) => {
    if (!values.length) return null;
    const ordered = [...values].sort((left, right) => left - right);
    return ordered[Math.min(ordered.length - 1, Math.ceil(ordered.length * fraction) - 1)];
  };
  const chartWindow = (buckets, mobileCount = 12, desktopCount = 50) => {
    const visible = window.innerWidth < 700 ? mobileCount : desktopCount;
    if (buckets.length <= visible) return [];
    const peak = buckets.reduce((best, bucket, index) =>
      bucket.anomaly_count > (buckets[best]?.anomaly_count || 0) ? index : best, 0);
    const start = Math.max(0, Math.min(peak - Math.floor(visible / 2), buckets.length - visible));
    const range = {start: 100 * start / buckets.length, end: 100 * (start + visible) / buckets.length};
    return [{type: 'inside', ...range}, {type: 'slider', height: 18, bottom: 14, ...range}];
  };
  const workerSummary = rows => {
    const workers = new Map();
    for (const row of rows) {
      const worker = knownWorker(row.worker) ? row.worker : '未明确';
      const stats = workers.get(worker) || {
        worker, traces: 0, anomalies: 0, errors: 0, slow_wr_count: 0, client_ms: [],
      };
      stats.traces++;
      stats.anomalies += Number(failed(row) || slow(row));
      stats.errors += Number(failed(row));
      stats.slow_wr_count += Number(row.slow_wr_count) || 0;
      if (Number.isFinite(row.client_ms)) stats.client_ms.push(row.client_ms);
      workers.set(worker, stats);
    }
    return [...workers.values()].sort((left, right) =>
      right.anomalies - left.anomalies || right.traces - left.traces ||
      left.worker.localeCompare(right.worker));
  };
  const secondsFor = rows => {
    const buckets = new Map();
    let missingTime = 0;
    for (const row of rows) {
      const second = String(row.timestamp || '').slice(0, 19);
      if (!/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d$/.test(second)) {
        missingTime++;
        continue;
      }
      const bucket = buckets.get(second) || {
        second, trace_count: 0, anomaly_count: 0, error_count: 0,
        slow_trace_count: 0, slow_wr_count: 0, dual_chip_count: 0, client_ms: [],
      };
      bucket.trace_count++;
      bucket.anomaly_count += Number(failed(row) || slow(row));
      bucket.error_count += Number(failed(row));
      bucket.slow_trace_count += Number(slow(row));
      bucket.slow_wr_count += Number(row.slow_wr_count) || 0;
      bucket.dual_chip_count += Number(row.chip_mode === '双 chip');
      if (Number.isFinite(row.client_ms)) bucket.client_ms.push(row.client_ms);
      buckets.set(second, bucket);
    }
    return {
      buckets: [...buckets.values()].sort((left, right) => left.second.localeCompare(right.second))
        .map(({client_ms, ...bucket}) => ({...bucket, client_p90_ms: percentile(client_ms, .9)})),
      missingTime,
    };
  };
  const drawTime = (id, buckets) => {
    const dataZoom = chartWindow(buckets);
    chart(id, {
      tooltip: {trigger: 'axis'}, legend: {top: 4},
      grid: {left: 58, right: 58, top: 76, bottom: dataZoom.length ? 100 : 74},
      dataZoom,
      xAxis: {type: 'category', data: buckets.map(bucket => bucket.second.slice(11)),
        axisLabel: {rotate: 35, hideOverlap: true}},
      yAxis: [{type: 'value', name: 'Trace / WR 数', min: 0, minInterval: 1},
        {type: 'value', name: 'Client P90 ms', min: 0}],
      series: [
        {name: 'Trace', type: 'line', showSymbol: false, data: buckets.map(bucket => bucket.trace_count),
          itemStyle: {color: '#2563eb'}},
        {name: '失败 Trace', type: 'bar', barMaxWidth: 24,
          data: buckets.map(bucket => bucket.error_count), itemStyle: {color: '#d94352'}},
        {name: '慢 WR 事件', type: 'bar', barMaxWidth: 24,
          data: buckets.map(bucket => bucket.slow_wr_count), itemStyle: {color: '#e99b24'}},
        {name: 'Client P90 ms', type: 'line', yAxisIndex: 1, connectNulls: false,
          data: buckets.map(bucket => bucket.client_p90_ms), itemStyle: {color: '#7c5ce7'}},
      ],
      ...(buckets.length ? {} : {graphic: {type: 'text', left: 'center', top: 'middle',
        style: {text: '未观测到可绘制的时间戳', fill: '#68738a'}}}),
    });
  };
  const drawWorkers = (id, workers) => {
    const visible = window.innerWidth < 700 ? 6 : 20;
    const dataZoom = workers.length > visible
      ? [{type: 'inside', start: 0, end: 100 * visible / workers.length},
        {type: 'slider', height: 18, bottom: 14, start: 0, end: 100 * visible / workers.length}]
      : [];
    chart(id, {
      tooltip: {trigger: 'axis'}, legend: {top: 4},
      grid: {left: 58, right: 58, top: 76, bottom: dataZoom.length ? 125 : 100},
      dataZoom,
      xAxis: {type: 'category', data: workers.map(item => item.worker),
        axisLabel: {rotate: 35, hideOverlap: true, width: 115, overflow: 'truncate'}},
      yAxis: [{type: 'value', name: 'Trace / WR 数', min: 0, minInterval: 1},
        {type: 'value', name: 'Client P90 ms', min: 0}],
      series: [
        {name: 'Trace', type: 'line', showSymbol: false, data: workers.map(item => item.traces),
          itemStyle: {color: '#2563eb'}},
        {name: '异常 Trace', type: 'bar', barMaxWidth: 24,
          data: workers.map(item => item.anomalies), itemStyle: {color: '#d94352'}},
        {name: '慢 WR 事件', type: 'bar', barMaxWidth: 24,
          data: workers.map(item => item.slow_wr_count), itemStyle: {color: '#e99b24'}},
        {name: 'Client P90 ms', type: 'line', yAxisIndex: 1, connectNulls: false,
          data: workers.map(item => percentile(item.client_ms, .9)), itemStyle: {color: '#7c5ce7'}},
      ],
      ...(workers.length ? {} : {graphic: {type: 'text', left: 'center', top: 'middle',
        style: {text: '未观测到关联 Worker', fill: '#68738a'}}}),
    });
  };

  function renderOperation(operation, prefix, label) {
    const rows = DATA.traces.filter(row => row.operation === operation);
    const workers = workerSummary(rows);
    const ranked = workers.filter(item => knownWorker(item.worker));
    const knownCount = ranked.reduce((total, item) => total + item.traces, 0);
    const {buckets: allBuckets, missingTime} = secondsFor(rows);
    let selectedBuckets = [];
    ReportRegistry.bindSource(`numa_${prefix}`, () => ({traces: rows, time_buckets: allBuckets}));
    ReportRegistry.bindSource(`numa_${prefix}_worker_time`, () => ({time_buckets: selectedBuckets}));
    $(prefix + '-worker-scope').textContent = `${label} ${rows.length} 条唯一 Trace；关联 Worker ${knownCount}，未关联 ${rows.length - knownCount}；有时间戳 ${rows.length - missingTime}，缺失 ${missingTime}。`;
    drawWorkers(prefix + '-worker-chart', workers);
    drawTime(prefix + '-time-chart', allBuckets);

    const topChoice = $(prefix + '-worker-time-top');
    const workerChoice = $(prefix + '-worker-time-choice');
    const scope = $(prefix + '-worker-time-scope');
    function renderSelected() {
      const worker = workerChoice.value;
      const stats = ranked.find(item => item.worker === worker);
      const {buckets, missingTime: missing} = secondsFor(rows.filter(row => row.worker === worker));
      selectedBuckets = buckets;
      scope.textContent = stats
        ? `${label} · ${worker} · 异常唯一 Trace ${stats.anomalies} / ${stats.traces}；有时间戳 ${stats.traces - missing}，缺失 ${missing}。${chartWindow(buckets).length ? '默认定位异常峰值附近，可缩放查看全程。' : ''}`
        : `${label}没有可归属的 Worker；未关联 ${rows.length - knownCount} 条。`;
      drawTime(prefix + '-worker-time-chart', buckets);
    }
    function updateTop() {
      const limit = Number(topChoice.value);
      const candidates = limit ? ranked.slice(0, limit) : ranked;
      const previous = workerChoice.value;
      workerChoice.replaceChildren(...candidates.map(stats => {
        const option = document.createElement('option');
        option.value = stats.worker;
        option.textContent = `${stats.worker} · ${stats.anomalies} 异常`;
        return option;
      }));
      workerChoice.disabled = candidates.length === 0;
      if (candidates.some(stats => stats.worker === previous)) workerChoice.value = previous;
      renderSelected();
    }
    topChoice.addEventListener('change', updateTop);
    workerChoice.addEventListener('change', renderSelected);
    updateTop();
  }

  const unclassified = DATA.traces.filter(row => row.operation !== 'GET' && row.operation !== 'PUT').length;
  $('operation-scope').textContent = `按已识别操作拆分；操作未明确 ${unclassified} 条，未混入读取或写入。Worker 为 Trace 关联标识，不代表执行耗时归属。`;
  renderOperation('GET', 'read', '读取');
  renderOperation('PUT', 'write', '写入');
})();
