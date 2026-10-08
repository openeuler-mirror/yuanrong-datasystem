"""Error charts retain unclassified failures and explain empty selections."""
from trace_test_loader import REPO_ROOT
from pathlib import Path
import subprocess


def test_error_panel_scopes_and_empty_state():
    template = (REPO_ROOT / 'scripts/trace_analysis/assets/read/read.html').read_text()
    start = template.index('function renderErrorAnalysis()')
    renderer = template[start:template.index('\nfunction ', start + 10)]
    script = """
const assert=require('assert');
let rows=[{failed:true},{error_family:'RPC截止超时',error_subcategory:'RPC deadline'}];
const options={},nodes={},scopeRows=()=>rows;
const chartAt=id=>({setOption:o=>options[id]=o});
const $=id=>nodes[id]||(nodes[id]={});
""" + renderer + """
renderErrorAnalysis();
assert.deepEqual(options['error-subcategory-chart'].series[0].data,[1,1]);
assert(options['error-subcategory-chart'].xAxis.data.includes('未细分'));
rows=[];renderErrorAnalysis();
assert(options['error-subcategory-chart'].title.text.includes('未观测'));
assert.equal(options['error-chain-chart'].series.length,0);
rows=[{error_family:'URMA超时',error_pending_wrs:2}];renderErrorAnalysis();
assert.deepEqual(options['error-chain-chart'].series[0].data,[1]);
"""
    subprocess.run(['node', '-e', script], check=True)
