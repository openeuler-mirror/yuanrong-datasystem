from trace_test_loader import REPO_ROOT
import subprocess
from pathlib import Path


def test_wr_thread_windows_preserve_overlap_and_evidence_limits():
    asset = REPO_ROOT / 'scripts/trace_analysis/assets/read/read_trace_stages.js'
    subprocess.run(['node', '-e', r'''
const assert=require('assert');eval(require('fs').readFileSync(process.argv[1],'utf8'));
const t={post:338160214419,wait:338160214430,poll_begin:338160214865,sleep_start:338160214807,sleep_end:338160214865,poll_end:338160214867,notify:338160214871,awake:338160214879,observed:338160214879,waited_for_notification:1,event_processing_and_wait_latency_valid:0};
const model=readWrTimelineModel({trace_us:t});
const span=name=>model.intervals.find(x=>x.name===name);
assert.equal(span('轮询线程 sleep').duration_ms,0.058);
assert.equal(span('通知→唤醒').duration_ms,0.008);
assert.equal(span('提交→通知').duration_ms,0.452);
assert.equal(span('调用方等待').duration_ms,0.449);
assert(model.notes.some(x=>x.includes('标志无效')));
assert(model.notes.some(x=>x.includes('不能把 post→poll')));
assert.equal(readWrTimelineModel({}).intervals.length,0);
const invalid=readWrTimelineModel({trace_us:{post:10,sleep_start:30,sleep_end:20,pre_completed_before_wait:1,woken_by_previous_event:1}});
assert(invalid.missing.some(x=>x.includes('时序无效')));
assert(invalid.notes.some(x=>x.includes('前一事件')));
''', str(asset)], check=True)
