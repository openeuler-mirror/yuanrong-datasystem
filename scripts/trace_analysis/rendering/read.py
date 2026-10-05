"""Render the read bottleneck model without parsing or attributing logs."""

from __future__ import annotations

import html
import json
import re

from ..resources import asset_path, echarts_path
from .registry import embed_registry
from .template import replace_tokens


CORRELATION_STYLE = r'''
.panel,.hero,.problem-grid>*{min-width:0;overflow-wrap:anywhere}
.chart{min-width:0;max-width:100%}
.correlation-grid{display:grid;grid-template-columns:minmax(0,1fr);gap:14px}
.correlation-grid>div{min-width:0;border:1px solid var(--line);border-radius:9px;padding:12px}
.correlation-summary{display:grid;grid-template-columns:repeat(4,minmax(140px,1fr));gap:10px;margin:12px 0}
.correlation-summary .metric{background:#f9fbfe}
table{width:100%;table-layout:fixed}
.table-wrap{max-height:560px;overflow-y:auto;overflow-x:hidden}
.worker-table-wrap{overflow-y:auto;overflow-x:hidden}
.panel th,.panel td{overflow-wrap:anywhere;word-break:break-word}
#traces .table-wrap,#write-analysis .worker-table-wrap{max-height:none;overflow:visible}.trace-log-group pre,.trace-log-group details pre{max-height:none;overflow:visible}.trace-slow td{background:#fff7ed}.trace-hot td{background:#fee2e2}.trace-failed td{background:#fff1f2}
.panel code{white-space:normal;word-break:break-all}
.panel .badge{white-space:normal}
.worker-name{max-width:none;overflow:visible;text-overflow:clip;white-space:normal}
.conclusion-cell{min-width:0}
.nowrap{white-space:normal}
.controls>*{max-width:100%}
#trace-table th:nth-child(1){width:8%}#trace-table th:nth-child(2){width:7%}#trace-table th:nth-child(3){width:12%}#trace-table th:nth-child(4){width:7%}#trace-table th:nth-child(5){width:9%}#trace-table th:nth-child(6){width:12%}#trace-table th:nth-child(7),#trace-table th:nth-child(8),#trace-table th:nth-child(9),#trace-table th:nth-child(10){width:7%}#trace-table th:nth-child(11),#trace-table th:nth-child(12){width:6%}#trace-table th:nth-child(13){width:5%}
#non-transport-table th:nth-child(1){width:7%}#non-transport-table th:nth-child(2){width:6%}#non-transport-table th:nth-child(3){width:11%}#non-transport-table th:nth-child(4){width:15%}#non-transport-table th:nth-child(5){width:10%}#non-transport-table th:nth-child(6),#non-transport-table th:nth-child(7),#non-transport-table th:nth-child(8),#non-transport-table th:nth-child(9),#non-transport-table th:nth-child(10){width:6%}#non-transport-table th:nth-child(11){width:21%}
#worker-correlation-table th:nth-child(1){width:10%}#worker-correlation-table th:nth-child(2){width:7%}#worker-correlation-table th:nth-child(3){width:13%}#worker-correlation-table th:nth-child(4){width:6%}#worker-correlation-table th:nth-child(5){width:14%}#worker-correlation-table th:nth-child(6){width:7%}#worker-correlation-table th:nth-child(7){width:6%}#worker-correlation-table th:nth-child(8),#worker-correlation-table th:nth-child(9){width:7%}#worker-correlation-table th:nth-child(10){width:23%}
#direct-worker-table th:first-child,#urma-source-table th:first-child{width:52%}#direct-worker-table th:not(:first-child),#urma-source-table th:not(:first-child){width:12%}
#write-trace-table th:nth-child(1){width:11%}#write-trace-table th:nth-child(2){width:12%}#write-trace-table th:nth-child(3){width:8%}#write-trace-table th:nth-child(4){width:16%}#write-trace-table th:nth-child(5),#write-trace-table th:nth-child(6),#write-trace-table th:nth-child(7){width:9%}#write-trace-table th:nth-child(8){width:15%}#write-trace-table th:nth-child(9){width:11%}
.time-segment-controls{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}
.time-segment-button{height:34px;border:1px solid #cfd8e6;border-radius:999px;background:#fff;padding:0 14px;cursor:pointer;color:var(--ink)}
.time-segment-button:hover{border-color:var(--blue);color:var(--blue)}
.time-segment-button.active{border-color:var(--blue);background:var(--blue);color:#fff;font-weight:700}
.time-segment-scope{min-height:24px;color:var(--muted);font-size:12px;line-height:1.6}
@media(max-width:1050px){
  .correlation-summary{grid-template-columns:repeat(3,1fr)}
  .controls input{min-width:0;flex:1 1 220px}
  #trace-table th:nth-child(5),#trace-table td:nth-child(5){display:none}
  #non-transport-table th:nth-child(4),#non-transport-table td:nth-child(4),#non-transport-table th:nth-child(5),#non-transport-table td:nth-child(5),#non-transport-table th:nth-child(7),#non-transport-table td:nth-child(7),#non-transport-table th:nth-child(8),#non-transport-table td:nth-child(8),#non-transport-table th:nth-child(10),#non-transport-table td:nth-child(10){display:none}
  #worker-correlation-table th:nth-child(2),#worker-correlation-table td:nth-child(2),#worker-correlation-table th:nth-child(8),#worker-correlation-table td:nth-child(8),#worker-correlation-table th:nth-child(9),#worker-correlation-table td:nth-child(9){display:none}
  #write-trace-table th:nth-child(1),#write-trace-table td:nth-child(1),#write-trace-table th:nth-child(5),#write-trace-table td:nth-child(5),#write-trace-table th:nth-child(7),#write-trace-table td:nth-child(7){display:none}
  .urma-request-table th:nth-child(n+6):nth-child(-n+12),.urma-request-table td:nth-child(n+6):nth-child(-n+12),.urma-request-table th:nth-child(14),.urma-request-table td:nth-child(14),.urma-request-table th:nth-child(15),.urma-request-table td:nth-child(15),.urma-request-table th:nth-child(16),.urma-request-table td:nth-child(16),.urma-request-table th:nth-child(17),.urma-request-table td:nth-child(17){display:none}
}
@media(max-width:650px){
  .correlation-summary{grid-template-columns:repeat(2,1fr)}
  .panel table{font-size:10px}.panel th,.panel td{padding:6px 3px}
  #trace-table th:nth-child(1),#trace-table td:nth-child(1),#trace-table th:nth-child(4),#trace-table td:nth-child(4),#trace-table th:nth-child(5),#trace-table td:nth-child(5),#trace-table th:nth-child(16),#trace-table td:nth-child(16){display:none}
  #worker-correlation-table th:nth-child(3),#worker-correlation-table td:nth-child(3),#worker-correlation-table th:nth-child(7),#worker-correlation-table td:nth-child(7){display:none}
  #write-trace-table th:nth-child(9),#write-trace-table td:nth-child(9){display:none}
  #urma-time-table th:nth-child(2),#urma-time-table td:nth-child(2),#urma-time-table th:nth-child(3),#urma-time-table td:nth-child(3),#urma-time-table th:nth-child(6),#urma-time-table td:nth-child(6),#urma-time-table th:nth-child(7),#urma-time-table td:nth-child(7){display:none}
  #urma-edge-table th:nth-child(2),#urma-edge-table td:nth-child(2),#urma-edge-table th:nth-child(3),#urma-edge-table td:nth-child(3),#urma-edge-table th:nth-child(6),#urma-edge-table td:nth-child(6),#urma-edge-table th:nth-child(7),#urma-edge-table td:nth-child(7){display:none}
  #direct-worker-table th:nth-child(3),#direct-worker-table td:nth-child(3),#direct-worker-table th:nth-child(4),#direct-worker-table td:nth-child(4),#direct-worker-table th:nth-child(5),#direct-worker-table td:nth-child(5){display:none}
  #urma-source-table th:nth-child(3),#urma-source-table td:nth-child(3){display:none}
}
'''

WRITE_SECTION = r'''
<section class="panel" id="write-analysis">
<h2>9. 写入瓶颈分析</h2>
<div class="notice"><b>独立口径：</b>写入不复用读取 QueryAndGet/Get 阶段。以 Client Set 总窗口为边界，分别展示 Create RPC、MemoryCopy、Client→Worker URMA通信、URMA调度/线程开销、Publish RPC、Worker Publish/元数据、其他调度、RPC网络和RPC框架。RPC timing 不闭合时不强拆 handler/网络/框架。</div>
<div id="write-summary" class="finding-grid"></div>
<h3 class="chart-title">图 9-1 写入 TopN 互斥阶段</h3>
<div id="write-timeline-chart" class="chart" style="height:430px"></div>
<h3>表 9-1 写入 Trace</h3>
<div class="worker-table-wrap write-trace-wrap"><table id="write-trace-table"><thead><tr><th>时间</th><th>Trace</th><th>总时延</th><th>主问题</th><th>Create RPC</th><th>写入数据</th><th>URMA通信/调度</th><th>Publish RPC</th><th>Worker Publish/元数据</th><th>RPC网络/框架</th></tr></thead><tbody></tbody></table></div>
<div id="write-trace-pager" class="pager"></div>
</section>
'''

WRITE_SCRIPT = r'''
const WRITE_STAGE_COLORS={'Create RPC其他':'#2563eb','写入MemoryCopy':'#0ea5a4','写入URMA通信':'#f59e0b','写入URMA调度/线程开销':'#c026d3','Publish RPC其他':'#7c3aed','Worker Publish/元数据':'#16a34a','其他调度/线程开销':'#9333ea','RPC网络相关':'#2563eb','RPC框架':'#64748b','未解释残差':'#9aa4b2'};

function renderWriteAnalysis(){
  const summary=$('write-summary'),body=$('write-trace-table').querySelector('tbody'),pager=$('write-trace-pager');
  if(!WRITE_ROWS.length){summary.innerHTML='<div class="empty">本批未采集 Client 写入 Trace</div>';body.innerHTML='<tr><td colspan="10" class="empty">0条/未采集</td></tr>';pager.innerHTML='';correlationChart('write-timeline-chart',false,{});return}
  const problems=Object.entries(WRITE_AGG.problem_counts||{}).sort((a,b)=>b[1]-a[1]),top=problems[0]||['未解释残差',0];
  summary.innerHTML=`<div class="finding-card"><b>写入 Trace</b><br>${WRITE_AGG.trace_count}条，失败 ${WRITE_AGG.failed_count}条；Client p90 ${fmt(WRITE_AGG.latency.p90)}，max ${fmt(WRITE_AGG.latency.max)}。</div><div class="finding-card"><b>最多主问题</b><br>${esc(top[0])} ${top[1]}条。主问题按每条 Trace 最大互斥阶段计数。</div><div class="finding-card"><b>URMA / RPC 口径</b><br>URMA通信与明确的 URMA 调度分开；wait→poll/completion wait 不整体算调度。Create/Publish RPC其他在完整 trailer 缺失时保持未细分。</div>`;
  const labels=WRITE_ROWS.map((row,index)=>`${String(index+1).padStart(3,'0')} ${row.timestamp.slice(11,19)}`),stages=Object.keys(WRITE_STAGE_COLORS),series=stages.map(name=>({name,type:'bar',stack:'write',barMaxWidth:12,data:WRITE_ROWS.map(row=>row.write_breakdown_ms[name]),itemStyle:{color:WRITE_STAGE_COLORS[name]}}));
  const chart=chartAt('write-timeline-chart');chart.setOption({animation:false,legend:{top:0,data:stages},grid:{left:48,right:20,top:68,bottom:72},tooltip:{trigger:'axis',axisPointer:{type:'shadow'},formatter:params=>{const row=WRITE_ROWS[params[0]?.dataIndex||0];return `<b>${esc(row.trace_id)}</b><br>Client ${fmt(row.client_ms)}<br>Create ${fmt(row.create_rpc_ms)} / Publish ${fmt(row.publish_rpc_ms)}<br>${stages.map(name=>`${esc(TraceCharts.label(name))}: ${fmt(row.write_breakdown_ms[name])}`).join('<br>')}`}},xAxis:{type:'category',data:labels,axisLabel:{interval:9,rotate:35,fontSize:10}},yAxis:{type:'value',name:'耗时 (ms)'},dataZoom:[{type:'inside',start:0,end:100},{type:'slider',height:18,bottom:10,start:0,end:100}],series});
  const selected=WRITE_ROWS;
  body.innerHTML=selected.map(row=>`<tr class="${row.failed?'trace-failed ':''}${row.client_ms>=20?'trace-hot':row.client_ms>=5?'trace-slow':''}"><td class="nowrap">${esc(row.timestamp.slice(11,23))}</td><td><code>${esc(row.trace_id)}</code></td><td>${latencyValue(row.client_ms)}</td><td><span class="badge" style="background:${WRITE_STAGE_COLORS[row.write_primary_stage]}20;color:${WRITE_STAGE_COLORS[row.write_primary_stage]}">${esc(row.write_primary_stage)}</span></td><td>${latencyValue(row.create_rpc_ms)}</td><td>${latencyValue(row.write_data_ms)}<div class="caption">${esc(row.write_data_basis)}</div></td><td>${latencyValue(row.write_breakdown_ms['写入URMA通信'])} / ${latencyValue(row.write_breakdown_ms['写入URMA调度/线程开销'])}</td><td>${latencyValue(row.publish_rpc_ms)}</td><td>${latencyValue(row.write_breakdown_ms['Worker Publish/元数据'])}</td><td>${latencyValue(row.write_breakdown_ms['RPC网络相关'])} / ${latencyValue(row.write_breakdown_ms['RPC框架'])}</td></tr>`).join('');
  pager.innerHTML=`<span>${WRITE_ROWS.length}条 · 全部展开</span>`;
}
'''


CORRELATION_SECTION = r'''
<section class="panel" id="query-meta-analysis">
<h2>5. QueryMeta 根因分析</h2>
<p class="caption">本章包含 QueryAndGet 父窗口证据；不等同于独立 QueryMeta RPC。父子耗时不相加，缺失日志标为未观测。</p>
<div id="query-meta-summary" class="query-meta-kpis"></div>
<p id="query-meta-boundary" class="caption"></p>
<div class="correlation-grid">
<div><h3>图 5-1 QueryAndGet 互斥细类</h3><div id="query-meta-detail-chart" class="chart"></div></div>
<div><h3>图 5-2 QueryMeta 时间分布</h3><div id="query-meta-time-chart" class="chart"></div></div>
<div><h3>图 5-3 QueryMeta 发起节点分布</h3><div id="query-meta-worker-chart" class="chart"></div></div>
<div><h3>图 5-4 Meta Owner 目标分布</h3><div id="query-meta-target-chart" class="chart"></div></div>
</div>
<details class="analysis-details"><summary>QueryAndGet 超时流程定界</summary>
<div id="query-meta-timeout-flow" class="finding-grid"></div>
</details>
<details class="analysis-details"><summary>统计口径与归因规则</summary>
<div class="notice"><b>定界口径：</b>当前源码为 <code>WorkerOCService.QueryAndGet</code>（分析器同时兼容历史 <code>MasterOCService.QueryAndGet</code>）。它不只查元数据：携带 <code>data_request</code> 时，metadata-affine Worker 还会准备本地数据响应，可通过 UB/URMA 内联返回数据。Worker 日志明确 <code>inlineHits &gt; 0</code>、<code>transport: UB</code>，且 URMA source Worker、Trace 和 attempt 时间窗唯一匹配时，逻辑 URMA Write 关键路径会从 QueryAndGet 父窗口剝离；同 Worker 的 QueryAndGet 父窗口内若唯一匹配到带 <code>elapsedMs</code> 的 <code>URMA_WAIT_TIMEOUT</code>，Stacked Bars 进一步拆为 QueryMeta/QueryAndGet 独占与 <b>URMA超时等待窗口</b>。该窗口是等待到超时的证据，不冒充完成态 WR 耗时。WR 分片不求和。失败且只有 <code>cntl_error_code=1008</code> 时，只能确认 Client 等待到截止点；缺少 server trailer 时，Worker 执行、响应发送、RPC residual 与 Client 截止观察仍未闭合。</div>
</details>
</section>
<section class="panel" id="worker-correlation">
<h2>6. 同 Worker 时间关联分析</h2>
<div class="notice"><b>口径：</b>本章始终使用全量 TopN，不受总览五段筛选影响。按日志本地时间每秒汇聚所选 Worker 的事件，p90 从桶内原始事件重算；跨 Worker 时钟未校准，仅作时间分布展示。关联判断仍限定同一 Worker。RPC按日志所在调用端归属；Worker处理、localRead和metadata来自Worker阶段日志，未推断缺失的对端耗时。RPC、UB、元数据、数据访问是四个独立观察维度，不把父子窗口相加；<code>URMA_ELAPSED_TOTAL &gt; 1.5ms</code> 才是慢 WR，<code>transferPath: UB</code> 本身不是 UB 耗时证据。同期出现只表示伴随关系，不证明因果。</div>
<div class="controls">
<select id="correlation-worker-filter"><option value="">全部有证据组件 / Worker</option></select>
<select id="correlation-category-filter"><option value="">全部类别</option><option value="query_meta">QueryMeta</option><option value="remote_get">RemoteGet</option><option value="urma_wr">URMA WR</option><option value="rpc">RPC</option><option value="local_processing">本地处理</option><option value="query_local_read">Worker localRead</option><option value="query_metadata">Worker metadata</option></select>
<select id="correlation-status-filter"><option value="all">全部状态（含未观测状态）</option><option value="problem">失败/慢事件</option><option value="failed">仅失败</option><option value="slow">仅慢事件</option><option value="normal">仅正常</option></select>
<select id="correlation-relation-filter"><option value="">全部关联</option><option value="direct_same_trace">同Trace直接证据</option><option value="concurrent_companion">同Worker同期伴随</option><option value="no_companion_evidence">无伴随证据</option></select>
<select id="correlation-latency-band-filter"><option value="">全部Client时延</option><option value="2-3">2–3ms</option><option value="3-4">3–4ms</option><option value="4-5">4–5ms</option><option value="5-6">5–6ms</option><option value="6-7">6–7ms</option><option value="7-10">7–10ms</option><option value="10-20">10–20ms</option><option value="20+">≥20ms</option></select>
<input id="correlation-time-start" type="datetime-local" step="0.001" title="Worker本地开始时间">
<input id="correlation-time-end" type="datetime-local" step="0.001" title="Worker本地结束时间">
<button id="correlation-reset-filter">清空筛选</button>
</div>
<div id="worker-correlation-summary" class="correlation-summary"></div>
<div class="correlation-grid">
<div><h3>图 6-1 RPC 通信残差 / 服务端 / 排队</h3><div class="caption">“RPC网络”桶沿用历史字段名，实际表示 bRPC 未被服务端执行和排队解释的通信残差（物理网络 + RPC framework），不能单独证明物理网络慢。</div><div id="worker-correlation-chart-rpc" class="chart"></div></div>
<div><h3>图 6-2 UB WR / completion wait / Inflight</h3><div id="worker-correlation-chart-ub" class="chart"></div></div>
<div><h3>图 6-3 QueryMeta / Worker metadata</h3><div id="worker-correlation-chart-metadata" class="chart"></div></div>
<div><h3>图 6-4 数据访问 LocalRead / RemoteGet</h3><div id="worker-correlation-chart-data" class="chart"></div></div>
</div>
<div class="worker-section"><h3>表 6-1 关联事件明细</h3><div class="table-wrap"><table id="worker-correlation-table"><thead><tr><th>Worker本地时间</th><th>Trace</th><th>Worker</th><th>维度</th><th>事件</th><th>耗时</th><th>失败</th><th>±1s慢WR</th><th>±1s RPC失败</th><th>关联判断</th></tr></thead><tbody></tbody></table></div><div id="worker-correlation-pager" class="pager"></div></div>
</section>
'''


CORRELATION_SCRIPT = asset_path("read_correlation.js").read_text(encoding='utf-8')


QUERY_BREAKDOWN_SCRIPT = (
    asset_path("read_rpc.js").read_text(encoding="utf-8")
    + "\n"
    + asset_path("read_query_breakdown.js").read_text(encoding="utf-8")
)


HTML_TEMPLATE = asset_path("read.html").read_text(encoding="utf-8")


def _render_dashboard_html(
    rows: list[dict],
    aggregate_data: dict,
    title: str,
    metadata: dict,
    write_rows: list[dict],
    write_aggregate: dict,
    *,
    scope_aggregates: dict,
    topology: dict,
    template: str = HTML_TEMPLATE,
    contract_error: type[ValueError] = ValueError,
) -> str:
    for slot in ("__REPORT_INIT__", "__CORRELATION_SCRIPT__", "__REPORT_RUNTIME__",
                 "__ROWS__", "__READ_SCOPES__", "__ECHARTS_SOURCE__", "__QUERY_BREAKDOWN_SCRIPT__"):
        if template.count(slot) != 1:
            raise contract_error(f"read template requires exactly one {slot}")
    view_rows = [{key: value for key, value in row.items() if key != "evidence_facts"} for row in rows]
    rows_json = json.dumps(view_rows, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    scope_json = json.dumps(scope_aggregates, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    aggregate_json = json.dumps(aggregate_data, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    write_rows_json = json.dumps(write_rows, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    write_aggregate_json = json.dumps(write_aggregate, ensure_ascii=False, separators=(",", ":")).replace(
        "<", "\\u003c"
    )
    library_path = echarts_path()
    echarts_source = library_path.read_text(encoding="utf-8")
    chart_support = asset_path("charts.js").read_text(encoding="utf-8")
    chart_support += "\n" + asset_path("chapter_navigation.js").read_text(
        encoding="utf-8"
    )
    chart_support += '\n' + asset_path("log_fields.js").read_text(encoding='utf-8')
    chart_support += "\n" + asset_path("read_trace_stages.js").read_text(
        encoding="utf-8"
    )
    echarts_source += "</script><script>" + chart_support
    shared_style = (
        asset_path("shared.css").read_text(encoding="utf-8")
        + "\n"
        + asset_path("read.css").read_text(encoding="utf-8")
    )
    echarts_source += "</script><style>" + shared_style + "</style><script>"
    safe_title = html.escape(title)
    code_ref = html.escape(str(metadata.get("code_ref") or "未记录"))
    current_source_ref = html.escape(str(metadata.get("current_source_ref") or "未提供"))
    case = html.escape(str(metadata.get("case") or "未命名"))
    scenario = html.escape(str(metadata.get("scenario") or "未记录"))
    deadline = float(aggregate_data.get("deadline_ms", 20.0))
    deadline_label = "参考阈值" if aggregate_data.get("deadline_is_reference") else "deadline"
    non_transport_count = int(aggregate_data.get("non_transport_analysis", {}).get("trace_count", 0))
    topology_kind = topology["kind"]
    topology_label = html.escape(str(topology["label"]))
    topology_path = html.escape(str(topology["path"]))
    raw_archives = metadata.get("raw_input_archives") or []
    raw_archive_html = ""
    if raw_archives:
        archive_items = "".join(
            '<li><a href="{path}" download>{name}</a> · {size} bytes · SHA256 <code>{sha}</code></li>'.format(
                path=html.escape(str(item.get("download_path") or ""), quote=True),
                name=html.escape(str(item.get("name") or "")),
                size=int(item.get("size_bytes", 0) or 0),
                sha=html.escape(str(item.get("sha256") or "未记录")),
            )
            for item in raw_archives
        )
        raw_archive_html = (
            '<section class="panel" id="raw-archives"><h2>下载原始 Trace 数据包</h2>'
            '<div class="notice"><b>归档合同：</b>以下文件是 ds-trace-triage 保留的输入原包副本；'
            '专项分享目录再次复制原文件，不从截断后的 evidence 反向拼包。</div>'
            f'<ul>{archive_items}</ul></section>'
        )
    if topology_kind == "client_direct":
        topology_detail = (
            "<code>Get</code> 进入 Client 侧 <code>GetFromTransportLayer</code>；Client 先向 Meta Owner 查询对象位置，"
            "再通过 TCP/URMA 直接访问 Data Worker。<code>BatchGetObjectRemote</code>、"
            "<code>WorkerWorkerOCService</code> 和 <code>RemotePull</code> 是服务/日志命名，单独出现时不代表 Worker→Worker。"
            "本模式下 RPC 网络是 Client↔Data Worker，URMA 是 Data Worker→Client。"
        )
    elif topology_kind == "legacy_worker_pull":
        topology_detail = (
            "该页面按历史运行日志中的调用方、目标地址、<code>WorkerWorkerOCService</code>、"
            "<code>RemotePull</code> 与 <code>worker-&gt;worker</code> 明确信息解释方向："
            "BatchGet 为 Worker→Data Worker，URMA 为 Data Worker→请求 Worker。"
            "这是采集版本的运行时语义；当前源码可能已经改为 Client 直达数据面。"
        )
    elif topology_kind == "bound_worker":
        topology_detail = (
            "Client 通过绑定 Worker 访问；只有 Trace 同时提供调用方、目标 Data Worker 和远端请求证据时，"
            "才把 BatchGet/RemotePull 判定为 Worker→Worker，不能仅凭服务名推断。"
        )
    else:
        topology_detail = (
            "使用者未提供 <code>local_cache</code> 模式。本页保留中性 BatchGet/Data Worker 口径，"
            "不根据 <code>WorkerWorkerOCService</code>、<code>BatchGetObjectRemote</code> 或 "
            "<code>RemotePull</code> 单独推断 Worker→Worker。"
        )
    source_logic_html = (
        '<section class="panel" id="source-logic"><h2>附录 9. 源码与访问拓扑</h2>'
        f'<div class="notice"><b>{topology_label}：</b><code>{topology_path}</code>。{topology_detail}</div>'
        f'<div class="caption">triage code ref：<code>{code_ref}</code>；当前源码校正 ref：'
        f'<code>{current_source_ref}</code>。源码级结论需对照该 ref 的实际源码；'
        "CodeGraph 仅用于定位。字段缺失保持为观测盲区，不推断网络、CPU、锁或线程调度。</div></section>"
    )
    template = (
        template.replace("SAME 3x105 QPS · Top100 关键瓶颈", safe_title)
        .replace(
            "100 个唯一 GET trace · triage 数据 ref d897aee1 · 代码逻辑校正 main/master@77fb2d9a",
            f"{len(rows)} 个唯一 Trace · case {case} · scenario {scenario} · triage ref {code_ref}",
        )
        .replace(
            '<section class="panel" id="source-logic"><h2>附录 9. 源码与访问拓扑</h2><div '
            'class="notice"><b><code>enableLocalCache=false</code> 读取主链：</b><code>Get</code> 进入 '
            '<code>GetFromTransportLayer</code>；<code>BuildTransportReadRequest</code> 先按 hash '
            'ring 选择 metadata owner，获得对象位置后，<code>ReplicaReader::ReadReplicaOnce</code> 直接向该 Data '
            'Worker 执行读取。因此这不是“Client→入口 Worker→Data Worker”的固定代理链。Trace 中出现不同的 DS_POSIX_GET '
            'Worker 与 URMA 日志 Worker 时，页面仅作“直连请求目标”和“URMA 供数端”的证据分组。</div><div '
            'class="caption">代码逻辑校正基线：main/master@77fb2d9a46f7ba9b658f4e1f6eba74c22206f9fe；triage '
            '数据记录的 code ref 为 d897aee13b7f20b58a60f81e1b31e094964c996d。CodeGraph '
            '仅用于定位，结论已对照当前实际源码。</div></section>',
            raw_archive_html,
        )
        .replace("Top100 诊断", "TopN 诊断")
        .replace("2. Top100 时间序列", "2. TopN 时间序列")
        .replace("表 7-1 Top100", "表 8-1 TopN")
        .replace("Top100 中", f"Top{len(rows)} 中")
        .replace("合计 100 条", f"合计 {len(rows)} 条")
        .replace("的 40 条 Trace", f"的 {non_transport_count} 条 Trace")
        .replace(
            '<a class="sub" href="#trace-log-panel">日志框 7-3 Trace 证据日志</a></aside>',
            '<a class="sub" href="#trace-log-panel">日志框 7-3 Trace 证据日志</a>'
            '<a href="#write-analysis">9. 写入瓶颈分析</a></aside>',
        )
        .replace('data-deadline-ms="20"', f'data-deadline-ms="{deadline:g}"')
        .replace("20ms deadline", f"{deadline:g}ms {deadline_label}")
        .replace("20ms 超时", "失败")
        .replace("远端供数非URMA", "远端供数处理")
        .replace("远端供数非 URMA", "远端供数处理")
        .replace("远端供数端非URMA处理", "远端供数端处理")
        .replace("截止点观测盲区", "Client/Worker观测未闭合")
        .replace("截止点观测空窗", "Client/Worker观测空窗")
        .replace("非 RPC / 非 UB 深挖", "非 RPC 主导深挖")
        .replace("Trace 原始日志", "Trace 证据日志")
        .replace("完整原始行按需展开", "triage 保留的证据行按需展开")
        .replace("展开全部 ${lines.length} 行原始日志", "展开 triage 保留的 ${lines.length} 行证据")
        .replace("下载全量 TopN", "下载 TopN 证据")
        .replace("Top100 全量 Trace", "TopN triage 证据")
        .replace("${latencyValue(row.urma_ms)}", "${row.urma_observed?latencyValue(row.urma_ms):'—'}")
        .replace(
            "${Number(row.data_rpc_e2e_ms).toFixed(6)} / "
            "${Number(row.data_rpc_network_ms).toFixed(6)} / "
            "${Number(row.data_rpc_server_ms).toFixed(6)} ms",
            "${row.data_rpc_e2e_ms===null?'未观测':"
            "Number(row.data_rpc_e2e_ms).toFixed(6)+' ms'} / "
            "${row.data_rpc_network_ms===null?'未观测':"
            "Number(row.data_rpc_network_ms).toFixed(6)+' ms'} / "
            "${row.data_rpc_server_ms===null?'未观测':"
            "Number(row.data_rpc_server_ms).toFixed(6)+' ms'}",
        )
        .replace(
            "`--- 原始日志 (${row.evidence.length}行) ---`,...row.evidence",
            "`证据保留: ${row.evidence.length}行；截断: ${row.dropped_evidence}行`,"
            "`--- triage保留证据 ---`,...row.evidence",
        )
        .replace("yAxis:20", "yAxis:DEADLINE_MS")
        .replace("const PAGE_SIZE=4", f"const DEADLINE_MS={deadline:g};const PAGE_SIZE=4")
        .replace("same-3x105qps-8mb", "datasystem-bottleneck")
        .replace(
            "'datasystem-bottleneck-top100-all-100'",
            "`datasystem-bottleneck-top${ROWS.length}-all-${ROWS.length}`",
        )
        .replace("`Top100 当前筛选 ${filtered.length} 条`", "`Top${ROWS.length} 当前筛选 ${filtered.length} 条`")
        .replace("生成自 SAME 3x105 QPS Top100 离线分析页", "生成自 ds-trace-triage 后置关键瓶颈分析页")
        .replace(
            "<b>口径：</b><code>local cache=false</code> 下没有固定“入口 Worker”层。页面将",
            "<b>口径：</b>页面不假设固定“入口 Worker”层；将",
        )
        .replace(
            '<a href="#non-transport-analysis">4. 非 RPC 主导深挖</a>',
            '<a href="#query-meta-analysis">5. QueryMeta 根因分析</a><a class="sub" '
            'href="#query-meta-detail-chart">图 5-1 互斥细类</a><a class="sub" '
            'href="#query-meta-time-chart">图 5-2 时间</a><a class="sub" '
            'href="#query-meta-worker-chart">图 5-3 发起节点</a><a class="sub" '
            'href="#query-meta-target-chart">图 5-4 Meta Owner</a><a href="#worker-correlation">6. '
            '同 Worker 时间关联</a><a class="sub" href="#worker-correlation-chart-rpc">图 6-1 RPC</a><a '
            'class="sub" href="#worker-correlation-chart-ub">图 6-2 UB</a><a class="sub" '
            'href="#worker-correlation-chart-metadata">图 6-3 元数据</a><a class="sub" '
            'href="#worker-correlation-chart-data">图 6-4 数据访问</a><a class="sub" '
            'href="#worker-correlation-table">表 6-1 关键事件</a><a href="#non-transport-analysis">5. 非'
            ' RPC 主导深挖</a>',
        )
        .replace(
            '<a href="#source-logic">7. 最新代码逻辑</a>',
            '<a href="#raw-archives">7. 原始数据包</a>'
            '<a href="#source-logic">8. 最新代码逻辑</a>',
        )
        .replace(
            '<a class="sub" href="#non-transport-count-chart">图 4-1 精细分类</a>'
            '<a class="sub" href="#non-transport-time-chart">图 4-2 时间分布</a>'
            '<a class="sub" href="#non-transport-worker-chart">图 4-3 Worker 分布</a>'
            '<a class="sub" href="#non-transport-table">表 4-1 逐 Trace 结论</a>'
            '<a href="#workers">5. Data Worker 分析</a><a href="#source-logic">6. 最新代码逻辑</a>'
            '<a href="#traces">7. Trace 查看</a><a class="sub" href="#trace-table">表 8-1 TopN</a>'
            '<a class="sub" href="#trace-detail-panel">表 7-2 Trace 阶段明细</a>'
            '<a class="sub" href="#trace-log-panel">日志框 7-3 Trace 证据日志</a>',
            '<a class="sub" href="#non-transport-count-chart">图 5-1 精细分类</a>'
            '<a class="sub" href="#non-transport-time-chart">图 5-2 时间分布</a>'
            '<a class="sub" href="#non-transport-worker-chart">图 5-3 Worker 分布</a>'
            '<a class="sub" href="#non-transport-table">表 5-1 逐 Trace 结论</a>'
            '<a href="#workers">5. Data Worker 分析</a><a href="#traces">6. Trace 查看</a>'
            '<a href="#source-logic">附录 9. 源码与访问拓扑</a><a class="sub" href="#trace-table">表 8-1 TopN</a>'
            '<a class="sub" href="#trace-detail-panel">表 8-2 Trace 阶段明细</a>'
            '<a class="sub" href="#trace-log-panel">日志框 8-3 Trace 证据日志</a>',
        )
        .replace(
            '<section class="panel" id="workers">',
            CORRELATION_SECTION + '<section class="panel" id="workers">',
        )
        .replace(
            '<div class="problem-grid"><div><h3>图 4-1 精细分类</h3>',
            '<div class="problem-grid"><div><h3>图 5-1 精细分类</h3>',
        )
        .replace('<div><h3>图 4-2 时间分布</h3>', '<div><h3>图 5-2 时间分布</h3>')
        .replace(
            '<div class="worker-section"><h3>图 4-3 Worker 分布</h3>',
            '<div class="worker-section"><h3>图 5-3 Worker 分布</h3>',
        )
        .replace(
            '<div class="worker-section"><h3>表 4-1 逐 Trace 结论</h3>',
            '<div class="worker-section"><h3>表 5-1 逐 Trace 结论</h3>',
        )
        .replace(
            '<section class="panel" id="workers"><h2>Data Worker 粒度分析</h2>',
            '<section class="panel" id="workers"><h2>7. Data Worker 粒度分析</h2>',
        )
        .replace(
            '<section class="panel" id="source-logic">'
            '<h2>最新 main/master 代码逻辑校正</h2>',
            '<section class="panel" id="source-logic">'
            '<h2>7. 最新 main/master 代码逻辑校正</h2>',
        )
        .replace(
            '<section class="panel" id="traces"><h2>按分类查看 Trace</h2>',
            '<section class="panel" id="traces"><h2>8. Trace 查看</h2><h3>表 8-1 TopN</h3>',
        )
        .replace(
            '<section class="panel" id="trace-detail-panel"><h2>Trace 阶段明细</h2>',
            '<section class="panel" id="trace-detail-panel"><h2>表 8-2 Trace 阶段明细</h2>',
        )
        .replace(
            '<section class="panel" id="trace-log-panel"><h2>Trace 原始日志</h2>',
            '<section class="panel" id="trace-log-panel"><h2>日志框 8-3 Trace 原始日志</h2>',
        )
        .replace(
            '<button id="non-transport-reset-filter">清空筛选</button>',
            '<button id="non-transport-reset-filter">清空筛选</button>'
            '<button id="download-non-transport-category">下载当前精细分类</button>',
        )
        .replace(
            '<div id="non-transport-summary" class="finding-grid"></div>',
            '<div id="non-transport-conclusions"></div>'
            '<h3 style="margin-top:18px">五类分布与治理方向</h3>'
            '<div id="non-transport-summary" class="finding-grid"></div>',
        )
        .replace(
            '<button id="reset-filter">清空筛选</button>',
            '<button id="reset-filter">清空筛选</button>'
            '<button id="download-filtered-traces">下载当前筛选 Trace</button>'
            '<button id="download-all-traces">下载 TopN 证据</button>',
        )
        .replace(
            '<div id="trace-detail"></div>',
            '<div class="controls"><button id="download-selected-trace">'
            '下载当前单条 Trace</button></div><div id="trace-detail"></div>',
        )
        .replace('</main></div><div id="tooltip"', WRITE_SECTION + '</main></div><div id="tooltip"')
        .replace(
            'const AGG=__AGG__;',
            'const AGG=__AGG__;const WRITE_ROWS=__WRITE_ROWS__;const WRITE_AGG=__WRITE_AGG__;',
        )
        .replace(
            '</style>',
            CORRELATION_STYLE
            + '.chart-title{text-align:center}'
            'th.sortable-header{cursor:pointer;user-select:none;white-space:nowrap}'
            'th.sortable-header:hover{color:var(--blue);background:#eef5ff}'
            'th.sortable-header:focus{outline:2px solid var(--blue);outline-offset:-2px}'
            '</style>',
        )
        .replace(
            "const rows=nonTransportFiltered.slice(",
            "const rows=sortRows('non-transport-table',nonTransportFiltered).slice(",
        )
        .replace("const slice=filtered.slice(", "const slice=sortRows('trace-table',filtered)")
        .replace(
            '<a href="#timeline">2. TopN 时间序列</a>',
            '<a class="sub" href="#time-segments">图 1-5 Client总时延五档</a>'
            '<a href="#timeline">2. TopN 时间序列</a>',
        )
        .replace(
            'renderUrmaAnalysis();renderWorkers();',
            'renderUrmaAnalysis();renderQueryMetaAnalysis();renderWorkerCorrelation();renderWriteA'
            'nalysis();renderWorkers();',
        )
        .replace(
            'renderTable();renderDetail();initScrollSpy();',
            "renderTable();renderDetail();bindSortableHeaders('trace-table',()=>{page=1},renderTab"
            "le);bindSortableHeaders('urma-time-table',()=>{urmaTimePage=1},renderUrmaTimeTable);b"
            "indSortableHeaders('urma-edge-table',()=>{urmaEdgePage=1},renderUrmaEdgeTable);bindSo"
            "rtableHeaders('direct-worker-table',()=>{workerPages.direct=1},renderWorkerTables);bi"
            "ndSortableHeaders('urma-source-table',()=>{workerPages.urma=1},renderWorkerTables);bi"
            "ndSortableHeaders('worker-correlation-table',()=>{correlationPage=1},renderWorkerCorr"
            'elationTable);initScrollSpy();',
        )
    )
    template = template.replace('</main></div><div id="tooltip"', source_logic_html + '</main></div><div id="tooltip"')
    if topology_kind == "client_direct":
        template = template.replace(
            "需要结合源→目标边继续定位", "需要结合 Data Worker→Client 请求继续定位"
        ).replace("源 Worker 与源→目标边", "Data Worker→Client URMA").replace(
            "源→目标", "Data Worker→接收端"
        )
    elif topology_kind == "unknown":
        template = template.replace("源 Worker 与源→目标边", "URMA 发送端与接收端（拓扑未确认）").replace(
            "源→目标", "发送端→接收端"
        )
    runtime = asset_path("report_runtime.js").read_text(encoding="utf-8")
    runtime += "\n" + asset_path("report_navigation.js").read_text(encoding="utf-8")
    for asset in ("trace_visuals.js", "trace_evidence_logs.js", "bottleneck_timeline.js"):
        runtime += "\n" + asset_path(asset).read_text(encoding="utf-8")
    init_script = asset_path("read_init.js").read_text(encoding="utf-8")
    injections = {
        "__REPORT_RUNTIME__": runtime,
        "__REPORT_INIT__": init_script,
        "__CORRELATION_SCRIPT__": 'let correlationWorker="";let correlationPage=1;\n' + CORRELATION_SCRIPT,
        "__ROWS__": rows_json,
        "__READ_SCOPES__": scope_json,
        "__QUERY_BREAKDOWN_SCRIPT__": QUERY_BREAKDOWN_SCRIPT,
        "__AGG__": aggregate_json,
        "__WRITE_ROWS__": write_rows_json,
        "__WRITE_AGG__": write_aggregate_json,
        "__ECHARTS_SOURCE__": echarts_source,
    }
    return replace_tokens(
        template,
        '__(?:ROWS|AGG|READ_SCOPES|WRITE_ROWS|WRITE_AGG|ECHARTS_SOURCE|QUERY_BREAKDOWN_SCRIPT|'
        'REPORT_RUNTIME|REPORT_INIT|CORRELATION_SCRIPT)__',
        injections,
    )


def render_html(
    analysis: dict, title: str, *, scope_aggregates: dict, topology: dict,
    template: str = HTML_TEMPLATE, contract_error: type[ValueError] = ValueError, view_top: int = 0,
) -> str:
    """Render one self-contained report from a precomputed analysis model."""

    if type(view_top) is not int or view_top not in (0, 100, 1000):
        raise ValueError("read view_top must be 0, 100 or 1000")
    rows = sorted(analysis["traces"], key=lambda row: (row["timestamp"], row["trace_id"]))
    output = _render_dashboard_html(
        rows, analysis["aggregate"], title, analysis["metadata"], [], {},
        scope_aggregates=scope_aggregates, topology=topology,
        template=template, contract_error=contract_error,
    )
    output = re.sub(r'<section class="panel" id="write-analysis">.*?</section>', '', output, flags=re.S)
    output = re.sub(r'<section class="panel" id="non-transport-analysis">.*?</section>', '', output, flags=re.S)
    output = re.sub(r'<a[^>]*href="#non-transport-[^"]*"[^>]*>.*?</a>', '', output)
    output = output.replace('renderNonTransportAnalysis();', '')
    output = re.sub(
        r"\$\('download-non-transport-category'\)\.onclick=.*?;\$\('download-selected-trace'\)",
        "$('download-selected-trace')",
        output,
        flags=re.S,
    )
    output = output.replace('<a href="#write-analysis">9. 写入瓶颈分析</a>', '')
    output = output.replace(WRITE_SCRIPT, '').replace('renderWriteAnalysis();', '')
    output = output.replace('<option>SET</option>', '')
    output = output.replace("Top100 时间序列", "TopN 时间序列").replace(
        "图 2-1 Stacked Bars", "图 2-1 TopN 时间序列 Stacked Bars"
    )
    output = output.replace('data-read-top="0"', f'data-read-top="{view_top}"', 1)
    return embed_registry(output, "read")
