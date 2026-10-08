from trace_test_loader import REPO_ROOT
import subprocess
from pathlib import Path


def test_scroll_navigation_tracks_sections_without_changing_other_links():
    asset = REPO_ROOT / 'scripts/trace_analysis/assets/shared/navigation.js'
    subprocess.run(['node', '-e', r'''
const assert=require('assert'),fs=require('fs');
let top=0;const handlers={};global.scrollY=0;global.innerHeight=600;
global.addEventListener=(name,fn)=>handlers[name]=fn;
global.requestAnimationFrame=fn=>fn();
const make=href=>({href,active:false,attrs:{},getAttribute:()=>href,
 classList:{toggle(name,on){links.find(l=>l.classList===this).active=on;}},
 setAttribute(k,v){this.attrs[k]=v;},removeAttribute(k){delete this.attrs[k];}});
const links=['#first','#second','#last','#missing','other.html'].map(make);
const sections={first:0,second:800,last:1800};
global.document={readyState:'complete',documentElement:{scrollHeight:2200},
 querySelectorAll:()=>[{querySelectorAll:()=>links}],
 getElementById:id=>id in sections?{getBoundingClientRect:()=>({top:sections[id]-top})}:null};
eval(fs.readFileSync(process.argv[1],'utf8'));
assert(links[0].active);assert.equal(links[0].attrs['aria-current'],'location');
top=850;global.scrollY=top;handlers.scroll();
assert(links[1].active&&!links[0].active);assert(!links[0].attrs['aria-current']);
top=1600;global.scrollY=top;handlers.scroll();assert(links[2].active);
top=0;global.scrollY=0;handlers.hashchange();assert(links[0].active);
assert(!links[3].active&&!links[4].active);
''', str(asset)], check=True)
