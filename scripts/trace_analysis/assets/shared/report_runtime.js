var ReportDiagnostics = (() => {
  const errors = [];
  function failure(stage, error) {
    errors.push({stage, message: String(error?.message || error)});
    let node = document.getElementById('report-render-failures');
    if (!node) {
      node = document.createElement('div');
      node.id = 'report-render-failures';
      node.setAttribute('role', 'alert');
      node.style.cssText = 'padding:14px;margin:14px;border:2px solid #b42318;background:white;color:#b42318;overflow-wrap:anywhere';
      (document.querySelector('main') || document.body).prepend(node);
    }
    node.textContent = '报告渲染不完整，请勿视为无样本：' + errors.map(x => x.stage + '：' + x.message).join('；');
  }
  function run(stage, render) {
    try { render(); } catch (error) { failure(stage, error); }
  }
  function audit() {
    if(typeof ReportRegistry!=='undefined'){
      const result=ReportRegistry.audit();
      return {...result,valid:result.valid&&!errors.length,errors:[...errors,...result.errors],charts:result.components.filter(item=>item.kind==='chart')};
    }
    const charts = [...document.querySelectorAll('.chart')].map(node => {
      const instance = window.echarts?.getInstanceByDom(node);
      let state;
      if (instance) {
        const series = instance.getOption().series || [];
        state = series.some(s => (s.data || []).length > 0) ? 'rendered' : 'empty';
      } else if (node.dataset.renderState === 'error') {
        state = 'error';
      } else if (/请选择/.test(node.textContent)) {
        state = 'awaiting_selection';
      } else {
        state = node.querySelector('.empty') || /未观测|没有|无数据|无样本/.test(node.textContent) ? 'empty' : 'error';
      }
      node.dataset.renderState = state;
      if (state === 'error') {
        node.textContent = '图表未完成渲染（不是无数据），请查看页面错误提示。';
      }
      return {id:node.id, state};
    });
    return {valid:!errors.length && charts.every(c => c.state !== 'error'), errors:[...errors], charts};
  }
  window.addEventListener('error', event => failure('javascript', event.message || '资源加载失败'), true);
  window.addEventListener('unhandledrejection', event => failure('promise', event.reason));
  window.addEventListener('load', () => {
    const result = audit();
    if (result.charts.some(c => c.state === 'error')) failure('chart-audit', '有图表未生成，详见各图状态');
  });
  return {run, audit};
})();
