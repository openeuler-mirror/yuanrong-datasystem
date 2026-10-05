
(() => {
  if(typeof document==='undefined')return;
  function installChapterDisclosure(){
    for(const nav of document.querySelectorAll('#nav,#write-nav,#overview-nav,body>nav')){
      const groups=[];let current;
      for(const link of [...nav.querySelectorAll('a[href^="#"]')]){
        const target=document.getElementById(link.hash.slice(1));if(!target)continue;
        if(!link.classList.contains('sub')&&/^(?:\d+\.\s|附录)/.test(link.textContent.trim())){
          current={link,target,children:[],button:null};groups.push(current);
        }else if(current)current.children.push(link);
      }
      function expand(group,open){for(const link of group.children)link.hidden=!open;if(group.button){group.button.setAttribute('aria-expanded',String(open));group.button.textContent=open?'▾':'▸';}}
      for(const [index,group] of groups.entries()){
        if(!group.children.length)continue;
        const header=document.createElement('div');header.className='chapter-heading';group.link.before(header);header.append(group.link);
        const button=document.createElement('button');button.type='button';button.className='chapter-toggle';button.dataset.navToggle=nav.id+'-'+index;button.setAttribute('aria-label','展开或收起 '+group.link.textContent.trim());header.append(button);group.button=button;
        for(const link of group.children){link.dataset.autoSub='true';link.dataset.navGroup=button.dataset.navToggle;}
        button.onclick=()=>expand(group,button.getAttribute('aria-expanded')!=='true');
        expand(group,false);
      }
      if(!groups.length)continue;
      let active=null,scheduled=false;
      function update(){scheduled=false;const bar=document.getElementById('report-switcher'),top=(bar?.getBoundingClientRect().bottom||0)+40;let next=groups[0];for(const group of groups)if(group.target.getBoundingClientRect().top<=top)next=group;if(next!==active){active=next;const before=active.target.getBoundingClientRect().top;groups.forEach(g=>{expand(g,g===active);g.link.classList.toggle('active',g===active);if(g===active)g.link.setAttribute('aria-current','location');else g.link.removeAttribute('aria-current')});const shift=active.target.getBoundingClientRect().top-before;if(shift)window.scrollBy({top:shift,behavior:'instant'});}}
      const schedule=()=>{if(!scheduled){scheduled=true;requestAnimationFrame(update)}};
      function followHash(){
        let id;try{id=decodeURIComponent(location.hash.slice(1))}catch{return}
        const target=id&&document.getElementById(id);if(!target)return;
        const group=[...groups].reverse().find(g=>g.target===target||g.target.contains(target));
        if(group){active=group;groups.forEach(g=>{expand(g,g===group);g.link.classList.toggle('active',g===group);if(g===group)g.link.setAttribute('aria-current','location');else g.link.removeAttribute('aria-current')});requestAnimationFrame(()=>window.dispatchEvent(new Event('resize')))}
      }
      addEventListener('hashchange',followHash);
      followHash();
      let hasScrolled=false;
      addEventListener('scroll',()=>{hasScrolled=true;schedule()},{passive:true});
      addEventListener('resize',()=>{if(hasScrolled)schedule()});
    }
  }
  if(document.readyState==='complete')installChapterDisclosure();else addEventListener('load',installChapterDisclosure,{once:true});
})();
