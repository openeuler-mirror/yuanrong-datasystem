(() => {
  function install() {
    for (const nav of document.querySelectorAll('body > nav')) {
      const entries = [...nav.querySelectorAll('a[href^="#"]')].flatMap(link => {
        const href = link.getAttribute('href');
        if (!href.startsWith('#')) return [];
        let id;
        try { id = decodeURIComponent(href.slice(1)); } catch { return []; }
        const section = id && document.getElementById(id);
        return section ? [{link, section}] : [];
      });
      if (!entries.length) continue;
      let pending = false;
      function update() {
        pending = false;
        let current = entries[0], nearest = -Infinity;
        for (const entry of entries) {
          const top = entry.section.getBoundingClientRect().top;
          if (top <= 96 && top >= nearest) { current = entry; nearest = top; }
        }
        if (scrollY > 0 && scrollY + innerHeight >= document.documentElement.scrollHeight - 2) {
          current = entries[entries.length - 1];
        }
        for (const {link} of entries) {
          const active = link === current.link;
          link.classList.toggle('active', active);
          if (active) link.setAttribute('aria-current', 'location');
          else link.removeAttribute('aria-current');
        }
      }
      function schedule() {
        if (pending) return;
        pending = true;
        requestAnimationFrame(update);
      }
      addEventListener('scroll', schedule, {passive:true});
      addEventListener('resize', schedule);
      addEventListener('hashchange', schedule);
      addEventListener('load', schedule);
      if (typeof ResizeObserver !== 'undefined') new ResizeObserver(schedule).observe(document.documentElement);
      update();
    }
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', install, {once:true});
  else install();
})();
