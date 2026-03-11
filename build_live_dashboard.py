"""
Generates dashboard/index.html from RespireLYF_unified_dashboard.html.
Keeps all HTML/CSS identical; replaces the <script> section with
Supabase-powered data loading (with static fallback).

Run once:  python build_live_dashboard.py
"""
import os

BASE = os.path.dirname(__file__)
SRC  = os.path.join(BASE, "RespireLYF_unified_dashboard.html")
DEST = os.path.join(BASE, "dashboard", "index.html")

with open(SRC, encoding="utf-8") as f:
    html = f.read()

# Cut at the opening <script> tag that contains the data section
CUT = "\n<script>\n// ─── DATA"
cut_idx = html.find(CUT)
if cut_idx == -1:
    raise ValueError("Could not find data script block — check source file")

html_head = html[:cut_idx]  # everything up to and including </div><!-- /page -->

NEW_SCRIPT = r"""
<script>
// ══════════════════════════════════════════════════════════
//  SUPABASE CONFIG — fill in after creating your free project
//  at supabase.com → Project Settings → API
// ══════════════════════════════════════════════════════════
const SUPABASE_URL  = 'https://jxanvdhpqzxehupxkmee.supabase.co';
const SUPABASE_ANON = 'sb_publishable_Y1PLmqV5ZTcOcPwm2xub8Q_3C5uLnj-';

// ── Static fallback — used until Supabase is configured ───
// These are the original verified data points (Feb 6 – Mar 7)
const STATIC_FL   = ['Feb 6','Feb 7','Feb 8','Feb 9','Feb 10','Feb 11','Feb 12','Feb 13','Feb 14','Feb 15','Feb 16','Feb 17','Feb 18','Feb 19','Feb 20','Feb 21','Feb 22','Feb 23','Feb 24','Feb 25','Feb 26','Feb 27','Feb 28','Mar 1','Mar 2','Mar 3','Mar 4','Mar 5','Mar 6','Mar 7'];
const STATIC_IMP  = [3,6,2,8,6,2425,821,19,80,116,1399,466,29,25,72,74,35,33,32,22,21,25,19,9,34,16,13,9,28,48];
const STATIC_PV   = [0,2,0,2,2,10,4,2,8,4,10,8,2,2,24,37,1,1,0,5,3,1,2,0,4,1,1,5,17,19];
const STATIC_DL   = [0,0,0,0,1,4,2,2,1,0,1,1,2,3,3,0,0,1,0,0,1,0,0,1,1,1,0,2,1,1];
const STATIC_SESS = [0,0,0,0,1,2,12,0,0,0,0,3,25,6,10,0,0,0,0,0,0,0,0,0,19,0,0,0,0,5];
const STATIC_YTVW = [7,4,16,31,8,31,37,34,7,17,40,51,55,40,127,56,51,11,12,7,5,14,5,1,11,18,10,9,8,15];
const STATIC_ACTD = [0,0,0,0,1,1,2,0,0,0,0,1,1,2,2,0,0,0,0,0,0,0,0,0,1,0,0,0,0,1];
const STATIC_KWDATA = [
  {kw:'vitals app',imp:917,taps:3,inst:0,spend:2.21,cpt:.74},
  {kw:'ai',imp:660,taps:3,inst:0,spend:2.65,cpt:.88},
  {kw:'health app',imp:491,taps:3,inst:0,spend:2.38,cpt:.79},
  {kw:'health insights',imp:280,taps:2,inst:1,spend:1.27,cpt:.64},
  {kw:'health monitoring',imp:235,taps:3,inst:1,spend:2.63,cpt:.88},
  {kw:'apple watch',imp:220,taps:2,inst:0,spend:1.87,cpt:.93},
  {kw:'tracking app',imp:206,taps:3,inst:0,spend:4.20,cpt:1.40},
  {kw:'pollen',imp:66,taps:1,inst:0,spend:1.67,cpt:.67},
  {kw:'peak flow',imp:45,taps:1,inst:1,spend:.05,cpt:.05},
  {kw:'asthma tracker',imp:48,taps:6,inst:0,spend:7.41,cpt:1.23},
  {kw:'respiratory rate',imp:8,taps:1,inst:0,spend:.97,cpt:.97},
  {kw:'symptom tracker',imp:7,taps:1,inst:0,spend:.92,cpt:.92},
  {kw:'copd tracker',imp:43,taps:0,inst:0,spend:0,cpt:0},
  {kw:'breathing tracker',imp:16,taps:0,inst:0,spend:0,cpt:0},
];
const STATIC_VIDS = [
  {t:'Want to MAXIMIZE Your Respire LYF Experience',v:108,imp:180,ctr:14.44,pub:'Feb 9'},
  {t:'Why Everything You Know About HYDRATION Is WRONG',v:77,imp:4856,ctr:0.97,pub:'Feb 16'},
  {t:"You Won't Believe the DIFFERENCE: Dry vs Wet Coughs",v:42,imp:474,ctr:3.80,pub:'Feb 17'},
  {t:"My Asthma Was Under Control — Why Coughing at 3 AM?",v:38,imp:571,ctr:2.28,pub:'Feb 16'},
  {t:'Millions do not realize their lungs are screaming for help',v:31,imp:96,ctr:8.33,pub:'Jan 17'},
  {t:'Asthma & COPD Management Feels Like the Wrong Map',v:29,imp:275,ctr:5.09,pub:'Feb 8'},
  {t:'7 Days to EASIER BREATHING with This Asthma App Trick',v:26,imp:293,ctr:0.68,pub:'Feb 19'},
  {t:'Asthma & COPD Just Got a MAJOR Upgrade',v:18,imp:498,ctr:2.01,pub:'Feb 20'},
  {t:'I Took Supplements for Asthma & COPD — 30 Days Results',v:24,imp:1106,ctr:0.81,pub:'Feb 21'},
  {t:'Chronic Stress Is SLOWLY KILLING Your Lungs!',v:22,imp:1795,ctr:0.72,pub:'Feb 16'},
];

// ── Supabase helpers ──────────────────────────────────────
const _H = { headers: { apikey: SUPABASE_ANON, Authorization: `Bearer ${SUPABASE_ANON}` } };
const _sb = (path) => fetch(`${SUPABASE_URL}/rest/v1/${path}`, _H).then(r => r.json());

function _fmtDate(iso) {
  // "2026-02-06" → "Feb 6"
  const d = new Date(iso + 'T00:00:00');
  return d.toLocaleDateString('en-US', { month:'short', day:'numeric' });
}

async function _loadFromSupabase() {
  try {
    const [appStore, kwds, ytDaily, ytVideos, events] = await Promise.all([
      _sb('app_store_daily?order=date.asc&limit=90'),
      _sb('apple_ads_keywords?order=impressions.desc&limit=50'),
      _sb('youtube_channel_daily?order=date.asc&limit=90'),
      _sb('youtube_videos?order=views.desc&limit=10'),
      _sb('firebase_events?order=date.asc'),
    ]);

    if (!Array.isArray(appStore) || appStore.length === 0) return null;

    // Build date-indexed maps
    const sessMap  = {};
    const actDMap  = {};
    for (const e of events) {
      if (e.event_name === 'session_start') sessMap[e.date]  = (sessMap[e.date]  || 0) + e.event_count;
      if (e.event_name === 'app_open')      actDMap[e.date]  = e.unique_users || 0;
    }
    const ytMap = {};
    for (const r of ytDaily) ytMap[r.date] = r;

    // Align all arrays to appStore dates
    const FL   = appStore.map(r => _fmtDate(r.date));
    const imp  = appStore.map(r => r.impressions  || 0);
    const pv   = appStore.map(r => r.page_views   || 0);
    const dl   = appStore.map(r => r.downloads    || 0);
    const sess = appStore.map(r => sessMap[r.date] || 0);
    const actD = appStore.map(r => actDMap[r.date] || 0);
    const ytVw = appStore.map(r => ytMap[r.date]?.views || 0);

    // Keywords
    const kwData = kwds.map(k => ({
      kw:   k.keyword,
      imp:  k.impressions,
      taps: k.taps,
      inst: k.installs,
      spend: parseFloat(k.spend),
      cpt:   parseFloat(k.cpt),
    }));

    // Videos
    const vids = ytVideos.map(v => ({
      t:   v.title,
      v:   v.views,
      imp: v.impressions,
      ctr: v.ctr,
      pub: v.published_at ? _fmtDate(v.published_at.slice(0, 10)) : '—',
    }));

    // Update the "Last updated" chip
    const lastDate = appStore[appStore.length - 1]?.date;
    if (lastDate) {
      const el = document.querySelector('.hlast');
      if (el) el.textContent = 'Live data · Last updated: ' + _fmtDate(lastDate);
    }

    return { FL, imp, pv, dl, sess, actD, ytVw, kwData, vids };
  } catch(e) {
    console.warn('[dashboard] Supabase load failed, using static data:', e.message);
    return null;
  }
}

// ── Chart defaults ────────────────────────────────────────
Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.font.size = 11;
Chart.defaults.color = '#5c6e8a';
Chart.defaults.plugins.legend.display = false;

function mkLine(id, labels, datasets, opts={}) {
  const ctx = document.getElementById(id);
  if(!ctx) return;
  return new Chart(ctx, {
    type:'line',
    data:{labels, datasets},
    options:{
      responsive:true, maintainAspectRatio:true,
      interaction:{mode:'index',intersect:false},
      plugins:{legend:{display:datasets.length>1,position:'top',labels:{boxWidth:10,padding:12,font:{size:10}}}},
      scales:{
        x:{grid:{color:'#f0f3fa'},ticks:{maxTicksLimit:10,font:{size:10}}},
        y:{grid:{color:'#f0f3fa'},ticks:{font:{size:10}},...(opts.yAxis||{})}
      },
      elements:{line:{tension:.35},point:{radius:2.5,hoverRadius:5}},
      ...opts.extra
    }
  });
}

function mkBar(id, labels, datasets, opts={}) {
  const ctx = document.getElementById(id);
  if(!ctx) return;
  return new Chart(ctx, {
    type:'bar',
    data:{labels, datasets},
    options:{
      responsive:true, maintainAspectRatio:true,
      interaction:{mode:'index',intersect:false},
      plugins:{legend:{display:datasets.length>1,position:'top',labels:{boxWidth:10,padding:12,font:{size:10}}}},
      scales:{
        x:{grid:{display:false},ticks:{maxTicksLimit:12,font:{size:10}},...(opts.xAxis||{})},
        y:{grid:{color:'#f0f3fa'},ticks:{font:{size:10}},...(opts.yAxis||{})}
      },
      ...opts.extra
    }
  });
}

function mkDonut(id, labels, data, colors) {
  const ctx = document.getElementById(id);
  if(!ctx) return;
  return new Chart(ctx, {
    type:'doughnut',
    data:{labels, datasets:[{data, backgroundColor:colors, borderWidth:2, borderColor:'#fff'}]},
    options:{
      responsive:true, maintainAspectRatio:true,
      plugins:{legend:{display:true,position:'bottom',labels:{boxWidth:10,padding:10,font:{size:10}}}},
      cutout:'62%'
    }
  });
}

// ── Render all charts with given data ─────────────────────
function renderDashboard({ FL, imp, pv, dl, sess, actD, ytVw, kwData, vids }) {

  // ─── MASTER CHART ───────────────────────────────────────
  mkLine('masterChart', FL, [
    {label:'App Store Impr ÷50', data:imp.map(v=>+(v/50).toFixed(1)), borderColor:'#1d55d9', backgroundColor:'rgba(29,85,217,.08)', fill:true, borderWidth:2},
    {label:'Downloads',          data:dl,   borderColor:'#0a8a5f', backgroundColor:'rgba(10,138,95,.1)', fill:true,  borderWidth:2},
    {label:'Sessions',           data:sess, borderColor:'#6830d8', backgroundColor:'rgba(104,48,216,.08)',fill:true, borderWidth:2},
    {label:'YouTube Views',      data:ytVw, borderColor:'#d42b2b', backgroundColor:'rgba(212,43,43,.07)', fill:true, borderWidth:2},
  ]);

  // ─── PAID SPEND DONUT ───────────────────────────────────
  mkDonut('paidSpendDonut',
    ['Apple Ads $29.60','YouTube $8.41','Meta $0','Google $0'],
    [29.60, 8.41, 0.01, 0.01],
    ['#1d55d9','#d42b2b','#6830d8','#1a73e8']
  );

  // ─── APPLE ADS WEEK DETAIL ──────────────────────────────
  mkBar('appleWeekDetail',
    ['Feb 10','Feb 11','Feb 12','Feb 13','Feb 14','Feb 15','Feb 16'],
    [
      {label:'Impressions', data:[6,2425,821,19,80,116,1399], backgroundColor:'rgba(29,85,217,.75)', yAxisID:'y'},
      {label:'Downloads',   data:[1,4,2,2,1,0,1],             backgroundColor:'rgba(10,138,95,.85)',  yAxisID:'y1'},
    ],
    {extra:{scales:{
      x:{grid:{display:false}},
      y:{position:'left', grid:{color:'#f0f3fa'}, title:{display:true,text:'Impressions',font:{size:10}}},
      y1:{position:'right', grid:{drawOnChartArea:false}, title:{display:true,text:'Downloads',font:{size:10}}, max:6}
    }}}
  );

  // ─── KW IMP CHART ───────────────────────────────────────
  const top10kw = kwData.slice(0,10);
  mkBar('kwImpChart',
    top10kw.map(k=>k.kw),
    [
      {label:'Impressions', data:top10kw.map(k=>k.imp), backgroundColor:'rgba(29,85,217,.6)'},
      {label:'Taps',        data:top10kw.map(k=>k.taps*20), backgroundColor:'rgba(10,138,95,.7)'},
    ],
    {xAxis:{ticks:{font:{size:9}}}}
  );

  // ─── YT RETENTION ───────────────────────────────────────
  {
    const ctx = document.getElementById('ytRetention');
    if(ctx) new Chart(ctx, {
      type:'bar',
      data:{
        labels:['Start (0s)','30 seconds','1 minute','1:31','End (1:31)'],
        datasets:[{data:[100,17.8,9.6,8.2,8.2], backgroundColor:['#d42b2b','#e84b4b','#f06060','#f47070','#f8a0a0'], borderRadius:4}]
      },
      options:{
        indexAxis:'y', responsive:true, maintainAspectRatio:true,
        plugins:{legend:{display:false}},
        scales:{
          x:{max:100, ticks:{callback:v=>v+'%', font:{size:10}}, grid:{color:'#f0f3fa'}},
          y:{grid:{display:false}, ticks:{font:{size:10}}}
        }
      }
    });
  }

  // ─── YT AGE CHART ───────────────────────────────────────
  mkBar('ytAgeChart',
    ['18–24','25–34','35–44','45–54','55–64','65+'],
    [{data:[1066,1649,1172,698,415,324], backgroundColor:['#d42b2b','#e84b4b','#c83030','#b02020','#981818','#801010'], borderRadius:4}]
  );

  // ─── YT VIEWS LINE ──────────────────────────────────────
  mkLine('ytViewsChart', FL,
    [{label:'YouTube Views', data:ytVw, borderColor:'#d42b2b', backgroundColor:'rgba(212,43,43,.1)', fill:true, borderWidth:2}]
  );

  // ─── APP STORE IMPRESSIONS SPLIT ────────────────────────
  const totalImp = imp.reduce((a,b)=>a+b,0);
  const paidImprArr = imp.map((v,i)=>{
    const paid=[0,0,0,0,6,2425,821,19,80,116,1399,466,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0];
    return paid[i]||0;
  });
  const paidTotal = paidImprArr.reduce((a,b)=>a+b,0);
  const orgTotal  = Math.max(0, totalImp - paidTotal);
  mkDonut('appStoreImprSplit',
    [`Apple Ads ${paidTotal.toLocaleString()}`, `Organic ${orgTotal.toLocaleString()}`],
    [paidTotal, orgTotal],
    ['#1d55d9','#0a8a5f']
  );

  // ─── DOWNLOAD ATTRIBUTION DONUT ─────────────────────────
  const totalDl = dl.reduce((a,b)=>a+b,0);
  const paidDl  = 3;
  const ytDl    = 1;
  const orgDl   = Math.max(0, totalDl - paidDl - ytDl);
  mkDonut('dlAttrDonut',
    [`Organic ~${orgDl}`, `Apple Ads ${paidDl}`, `YouTube ~${ytDl}`],
    [orgDl, paidDl, ytDl],
    ['#0a8a5f','#1d55d9','#d42b2b']
  );

  // ─── APP STORE IMPRESSIONS STACKED ──────────────────────
  const orgImprArr = imp.map((v,i)=>Math.max(0,v-paidImprArr[i]));
  mkBar('appStoreImprDay', FL,
    [
      {label:'Paid (Apple Ads)', data:paidImprArr, backgroundColor:'rgba(29,85,217,.75)', stack:'s'},
      {label:'Organic',          data:orgImprArr,  backgroundColor:'rgba(10,138,95,.65)', stack:'s'},
    ],
    {extra:{scales:{x:{stacked:true,grid:{display:false},ticks:{maxTicksLimit:10,font:{size:10}}}, y:{stacked:true,grid:{color:'#f0f3fa'}}}}}
  );

  // ─── DOWNLOAD DAY CHART ─────────────────────────────────
  mkBar('dlDayChart', FL,
    [{data:dl, backgroundColor:'rgba(10,138,95,.75)', borderRadius:3}]
  );

  // ─── PV vs DL ───────────────────────────────────────────
  mkBar('pvDlChart', FL,
    [
      {label:'Page Views', data:pv, backgroundColor:'rgba(29,85,217,.6)', borderRadius:3},
      {label:'Downloads',  data:dl, backgroundColor:'rgba(10,138,95,.75)', borderRadius:3},
    ]
  );

  // ─── SESSIONS CHART ─────────────────────────────────────
  mkBar('sessionsChart', FL,
    [{data:sess, backgroundColor:'rgba(104,48,216,.7)', borderRadius:3}]
  );

  // ─── DEVICES CHART ──────────────────────────────────────
  mkLine('devicesChart', FL,
    [{label:'Active Devices', data:actD, borderColor:'#6830d8', backgroundColor:'rgba(104,48,216,.1)', fill:true, borderWidth:2, stepped:true}]
  );

  // ─── WEEKLY GROUPED BAR ─────────────────────────────────
  {
    const weeks=['W1 Feb 6-9','W2 Feb 10-16','W3 Feb 17-23','W4 Feb 24-Mar 2','W5 Mar 3-7'];
    const wDl  =[0,11,7,4,7];
    const wSess=[0,17,9,19,24];
    mkBar('weeklyChart', weeks,
      [
        {label:'Downloads', data:wDl,   backgroundColor:'rgba(10,138,95,.75)', borderRadius:3},
        {label:'Sessions',  data:wSess, backgroundColor:'rgba(104,48,216,.65)', borderRadius:3},
      ]
    );
  }

  // ─── FUNNEL CHART ───────────────────────────────────────
  {
    const ctx = document.getElementById('funnelChart');
    if(ctx) new Chart(ctx, {
      type:'bar',
      data:{
        labels:['Ad Impressions','App Store Impr','Page Views','Downloads','Sessions','Active D7+'],
        datasets:[{
          data:[33624, 5895, 177, 29, 83, 2],
          backgroundColor:['#1d55d9','#3b6fef','#5c8caa','#0a8a5f','#6830d8','#94a3b8'],
          borderRadius:5
        }]
      },
      options:{
        indexAxis:'y', responsive:true, maintainAspectRatio:true,
        plugins:{legend:{display:false}, tooltip:{callbacks:{label:c=>' '+c.raw.toLocaleString()}}},
        scales:{
          x:{type:'logarithmic', grid:{color:'#f0f3fa'}, ticks:{font:{size:9}, callback:v=>v>=1000?Math.round(v/1000)+'k':v}},
          y:{grid:{display:false}, ticks:{font:{size:10}}}
        }
      }
    });
  }

  // ─── KEYWORD TABLE ──────────────────────────────────────
  {
    const tbody = document.getElementById('kwBody');
    if(tbody && kwData.length) {
      tbody.innerHTML = '';
      const maxImp = Math.max(...kwData.map(k=>k.imp));
      kwData.forEach(k=>{
        const pct = (k.imp/maxImp*100).toFixed(1);
        const result = k.inst>0
          ? `<span class="pill pG">✓ Install</span>`
          : k.taps>0
            ? `<span class="pill po">Taps only</span>`
            : `<span class="pill px">No taps</span>`;
        const cptStr   = k.cpt===0   ? '<span class="dt">—</span>' : `$${k.cpt.toFixed(2)}`;
        const spendStr = k.spend===0 ? '<span class="dt">—</span>' : `$${k.spend.toFixed(2)}`;
        tbody.innerHTML += `<tr class="${k.inst>0?'ra':''}">
          <td><strong>${k.kw}</strong></td>
          <td><div class="kwbar"><div class="kwtrk"><div class="kwfil" style="width:${pct}%;background:var(--apple)"></div></div><span style="font-size:10px;color:var(--t2);font-weight:600;min-width:30px">${k.imp}</span></div></td>
          <td class="num">${k.taps}</td>
          <td class="num ${k.inst>0?'gt':''}">${k.inst}</td>
          <td class="num">${spendStr}</td>
          <td class="num">${cptStr}</td>
          <td>${result}</td>
        </tr>`;
      });
    }
  }

  // ─── VIDEO TABLE ────────────────────────────────────────
  {
    const tbody = document.getElementById('vidBody');
    if(tbody && vids.length) {
      tbody.innerHTML = '';
      vids.forEach((v,i)=>{
        const ctrCls = v.ctr>=5?'gt':v.ctr>=2?'ot':'rt';
        tbody.innerHTML += `<tr class="${i%2===0?'ra':''}">
          <td style="max-width:280px"><div style="font-size:11px;font-weight:500;color:var(--t1);line-height:1.4">${v.t}</div></td>
          <td class="num">${v.v}</td>
          <td class="num">${(v.imp||0).toLocaleString()}</td>
          <td class="num ${ctrCls}">${v.ctr}%</td>
          <td style="font-size:11px;color:var(--t3)">${v.pub}</td>
        </tr>`;
      });
    }
  }
}

// ─── Sticky nav ──────────────────────────────────────────
{
  const secs = ['s1','s2','s3','s4','s5','s6','s7','s8'];
  const links = document.querySelectorAll('.hn');
  const obs = new IntersectionObserver(entries=>{
    entries.forEach(e=>{
      if(e.isIntersecting){
        links.forEach(l=>l.classList.remove('act'));
        const link = document.querySelector(`.hn[href="#${e.target.id}"]`);
        if(link) link.classList.add('act');
      }
    });
  },{threshold:0.2,rootMargin:'-80px 0px -60% 0px'});
  secs.forEach(id=>{
    const el=document.getElementById(id);
    if(el) obs.observe(el);
  });
}

// ─── Boot ────────────────────────────────────────────────
async function boot() {
  const isConfigured = SUPABASE_URL.startsWith('https://') && SUPABASE_ANON.length > 10;
  let data = null;

  if (isConfigured) {
    data = await _loadFromSupabase();
  }

  // Fall back to static data if Supabase not configured or returned empty
  if (!data) {
    data = {
      FL:     STATIC_FL,
      imp:    STATIC_IMP,
      pv:     STATIC_PV,
      dl:     STATIC_DL,
      sess:   STATIC_SESS,
      actD:   STATIC_ACTD,
      ytVw:   STATIC_YTVW,
      kwData: STATIC_KWDATA,
      vids:   STATIC_VIDS,
    };
  }

  renderDashboard(data);
}

boot();
</script>

</body>
</html>"""

with open(DEST, "w", encoding="utf-8") as f:
    f.write(html_head + NEW_SCRIPT)

print(f"Built: {DEST}")
print(f"  HTML head: {len(html_head):,} chars")
print(f"  Total:     {len(html_head) + len(NEW_SCRIPT):,} chars")
