import React,{useEffect,useState} from 'react';
import {createRoot} from 'react-dom/client';
import './style.css';

const API=import.meta.env.VITE_API_URL||'http://localhost:3000/api';
// Service addresses shown on the login page (host-side ports from docker-compose.yml).
const SERVICES=[
  {name:'API',value:API.replace(/\/api$/,''),href:true,usedFor:'Fastify REST API, JWT auth, outbox publisher, payment-timeout worker'},
  {name:'PostgreSQL',value:import.meta.env.VITE_POSTGRES||'localhost:5433 (user/db/password: booking)',href:false,usedFor:'Source of truth: users, hotels, rooms, bookings, audit log, outbox events'},
  {name:'Redis',value:import.meta.env.VITE_REDIS||'redis://localhost:6379',href:false,usedFor:'Room availability per night (availability:<roomId>:<date>) and the atomic Lua booking lock'},
  {name:'Kafka broker',value:import.meta.env.VITE_KAFKA_BROKER||'kafka:9092',href:false,usedFor:'Booking events published from the outbox (address inside the compose network)'},
  {name:'Kafka UI',value:import.meta.env.VITE_KAFKA_UI_URL||'http://localhost:8080',href:true,usedFor:'Browse topics and messages'},
];
async function api(path:string, opts:any={}) {
  const token=localStorage.getItem('token');
  const headers:any={...(opts.body?{'Content-Type':'application/json'}:{}),...(token?{Authorization:`Bearer ${token}`}:{})};
  const r=await fetch(API+path,{...opts,headers});
  const data=await r.json(); if(!r.ok) throw new Error(data.error||'Request failed'); return data;
}

const isoDate=(d:Date)=>d.toISOString().slice(0,10);
const plusDays=(iso:string,n:number)=>{const t=new Date(iso+'T00:00:00Z');t.setUTCDate(t.getUTCDate()+n);return isoDate(t)};
const today=isoDate(new Date());
const fmt=(iso:string)=>new Date(iso+'T00:00:00Z').toLocaleDateString(undefined,{day:'numeric',month:'short',year:'numeric',timeZone:'UTC'});
const baht=(n:any)=>'฿'+Math.round(Number(n)).toLocaleString();
const STATUS_LABEL:Record<string,string>={PENDING:'Awaiting payment',CONFIRMED:'Confirmed',CANCELLED:'Cancelled',PAYMENT_TIMEOUT:'Payment timed out',EXPIRED:'Expired'};
const secondsLeft=(b:any)=>Math.max(0,Math.ceil((Date.parse(b.expiresAt)-Date.now())/1000));
const daysBetween=(a:string,b:string)=>Math.round((Date.parse(b+'T00:00:00Z')-Date.parse(a+'T00:00:00Z'))/86400000);
/** Where a confirmed stay is relative to today, worded like a booking site's trip list. */
function stayPhase(b:any){
  if(b.status!=='CONFIRMED')return null;
  if(today<b.checkIn){const n=daysBetween(today,b.checkIn);return{key:'upcoming',label:'Upcoming',hint:n===1?'Check-in tomorrow':`Check-in in ${n} days`}}
  if(today<b.checkOut){const n=daysBetween(today,b.checkOut);return{key:'current',label:'Staying now',hint:n===1?'Check-out tomorrow':`Check-out in ${n} days`}}
  const n=daysBetween(b.checkOut,today);return{key:'completed',label:'Completed',hint:n===0?'Checked out today':n===1?'Checked out yesterday':`Checked out ${n} days ago`}
}

// Seeded accounts. Admin comes from database/init.sql; the rest are created by "Generate Sample Data".
const ACCOUNTS=[
  {label:'Admin',email:'admin@example.com',password:'admin123'},
  {label:'Customer',email:'customer@example.com',password:'customer123'},
  {label:'Customer 2',email:'customer2@example.com',password:'customer123'},
  {label:'Seller',email:'seller@example.com',password:'seller123'},
  {label:'Seller 2',email:'seller2@example.com',password:'seller123'},
];

const num=(n:any)=>Number(n).toLocaleString();
const ms=(n:any)=>`${Number(n).toLocaleString(undefined,{maximumFractionDigits:1})} ms`;

function Tile({label,value,hint}:{label:string,value:any,hint?:string}){
  return <div className="tile"><div className="tile-label">{label}</div><div className="tile-value">{value}</div>{hint&&<div className="tile-hint">{hint}</div>}</div>;
}

/** Stacked bars: booked vs free rooms per day. Inline SVG, no library. */
function OccupancyChart({perDay,inventory}:{perDay:any[],inventory:number}){
  const W=640,H=220,padL=36,padB=34,padT=10,gap=2,n=perDay.length;
  const plotW=W-padL-8,plotH=H-padT-padB,bw=Math.min(56,(plotW/n)-10);
  const max=Math.max(1,inventory);
  const y=(v:number)=>padT+plotH-(v/max)*plotH;
  const ticks=[0,0.25,0.5,0.75,1].map(f=>Math.round(max*f));
  return <figure className="chart">
    <figcaption><b>Rooms per night</b> · booked vs free, next {n} days · inventory {inventory}</figcaption>
    <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label="Booked and free rooms per day">
      {ticks.map(t=><g key={t}><line x1={padL} x2={W-8} y1={y(t)} y2={y(t)} className="grid"/><text x={padL-6} y={y(t)+4} textAnchor="end" className="axis">{t}</text></g>)}
      {perDay.map((d,i)=>{const x=padL+(plotW/n)*i+((plotW/n)-bw)/2;const yb=y(d.booked),yf=y(d.booked+d.free);const bookedH=Math.max(0,padT+plotH-yb);
        return <g key={d.day}>
          <title>{`${fmt(d.day)}\nBooked: ${d.booked} (${d.locked} locked, ${d.arrivals} arrivals)\nFree: ${d.free}`}</title>
          <rect x={x} y={yf} width={bw} height={Math.max(0,yb-yf-(bookedH?gap:0))} rx={4} className="bar-free"/>
          {bookedH>0&&<rect x={x} y={yb} width={bw} height={bookedH} rx={bookedH>4?4:0} className="bar-booked"/>}
          {d.booked>0&&<text x={x+bw/2} y={Math.max(padT+10,yb-4)} textAnchor="middle" className="label">{d.booked}</text>}
          <text x={x+bw/2} y={H-padB+16} textAnchor="middle" className="axis">{new Date(d.day+'T00:00:00Z').toLocaleDateString(undefined,{weekday:'short',timeZone:'UTC'})}</text>
          <text x={x+bw/2} y={H-padB+29} textAnchor="middle" className="axis muted">{new Date(d.day+'T00:00:00Z').toLocaleDateString(undefined,{day:'numeric',month:'short',timeZone:'UTC'})}</text>
        </g>})}
    </svg>
    <div className="legend"><span><i className="sw booked"/>Booked (confirmed + locked)</span><span><i className="sw free"/>Free</span></div>
  </figure>;
}

/** Horizontal bars: occupancy % per hotel for the window. */
function HotelBars({hotels}:{hotels:any[]}){
  const rowH=26,W=640,labelW=170,valW=120,H=hotels.length*rowH+8,plotW=W-labelW-valW;
  return <figure className="chart">
    <figcaption><b>Occupancy by hotel</b> · booked room-nights as % of capacity</figcaption>
    <svg viewBox={`0 0 ${W} ${Math.max(H,30)}`} role="img" aria-label="Occupancy per hotel">
      {hotels.map((h,i)=>{const y=4+i*rowH;const w=Math.max(0,plotW*h.occupancyPct/100);
        return <g key={h.id}><title>{`${h.name} (${h.city})\n${h.bookedRoomNights} of ${h.capacityRoomNights} room-nights booked · ${h.bookings} bookings · ${baht(h.revenue)}`}</title>
          <text x={labelW-8} y={y+rowH/2+4} textAnchor="end" className="axis">{h.name}</text>
          <rect x={labelW} y={y+5} width={plotW} height={rowH-10} rx={4} className="bar-free"/>
          {w>0&&<rect x={labelW} y={y+5} width={w} height={rowH-10} rx={4} className="bar-booked"/>}
          <text x={labelW+plotW+8} y={y+rowH/2+4} className="label">{h.occupancyPct}% · {h.freeRoomNights} free</text>
        </g>})}
    </svg>
  </figure>;
}

function SellerDashboard({days,setDays,onMessage}:{days:number,setDays:(n:number)=>void,onMessage:(m:string)=>void}){
  const [data,setData]=useState<any>(null); const [table,setTable]=useState(false);
  async function load(){try{setData(await api(`/seller/dashboard?days=${days}`))}catch(e:any){onMessage(e.message)}}
  useEffect(()=>{load()},[days]);
  if(!data) return <section className="card"><h2>Dashboard</h2><p>Loading…</p></section>;
  const t=data.tiles;
  return <section className="card">
    <div className="row between"><h2>Dashboard · {fmt(data.from)} → {fmt(data.to)}</h2>
      <div className="row"><label>Range<select value={days} onChange={e=>setDays(Number(e.target.value))}><option value={7}>Next 7 days</option><option value={14}>Next 14 days</option><option value={30}>Next 30 days</option></select></label><button onClick={load}>Refresh</button><button onClick={()=>setTable(!table)}>{table?'Charts':'Table view'}</button></div></div>
    <div className="tiles">
      <Tile label="Hotels" value={t.hotels} hint={`${t.roomTypes} room types`}/>
      <Tile label="Rooms (inventory)" value={t.inventory} hint={`${num(t.capacityRoomNights)} room-nights capacity`}/>
      <Tile label="Bookings" value={t.bookings} hint={`overlapping the ${days}-day window`}/>
      <Tile label="Occupancy" value={`${t.occupancyPct}%`} hint={`${num(t.bookedRoomNights)} room-nights booked`}/>
      <Tile label="Free room-nights" value={num(t.freeRoomNights)} hint="capacity minus booked"/>
      <Tile label="Revenue" value={baht(t.revenue)} hint="confirmed stays checking in this window"/>
      <Tile label="Arrivals today" value={t.arrivalsToday}/>
      <Tile label="Locked now" value={t.lockedNow} hint="pending payment"/>
    </div>
    {!table?<><OccupancyChart perDay={data.perDay} inventory={t.inventory}/>{data.hotels.length>0&&<HotelBars hotels={data.hotels}/>}</>:
    <div className="tablewrap">
      <table><thead><tr><th>Day</th><th>Booked</th><th>Locked</th><th>Free</th><th>Arrivals</th></tr></thead><tbody>{data.perDay.map((d:any)=><tr key={d.day}><td>{fmt(d.day)}</td><td>{d.booked}</td><td>{d.locked}</td><td>{d.free}</td><td>{d.arrivals}</td></tr>)}</tbody></table>
      <table><thead><tr><th>Hotel</th><th>City</th><th>Rooms</th><th>Bookings</th><th>Booked nights</th><th>Free nights</th><th>Occupancy</th><th>Revenue</th></tr></thead><tbody>{data.hotels.map((h:any)=><tr key={h.id}><td>{h.name}</td><td>{h.city}</td><td>{h.inventory}</td><td>{h.bookings}</td><td>{h.bookedRoomNights}</td><td>{h.freeRoomNights}</td><td>{h.occupancyPct}%</td><td>{baht(h.revenue)}</td></tr>)}</tbody></table>
    </div>}
  </section>;
}

function SimulationPanel({onMessage,hotels,rooms,hotelId,roomId,setHotelId,setRoomId}:{onMessage:(m:string)=>void,hotels:any[],rooms:any[],hotelId:string,roomId:string,setHotelId:(v:string)=>void,setRoomId:(v:string)=>void}){
  const [mode,setMode]=useState<'random-week'|'same-room-fallback'>('random-week');
  const [customers,setCustomers]=useState(100); const [payPct,setPayPct]=useState(50); const [latePct,setLatePct]=useState(30); const [windowS,setWindowS]=useState(5);
  const [runs,setRuns]=useState<any[]>([]); const [run,setRun]=useState<any>(null); const [busy,setBusy]=useState(false); const [showLog,setShowLog]=useState(false); const [allRooms,setAllRooms]=useState(false);
  async function loadRuns(){try{setRuns(await api('/admin/simulation'))}catch(e:any){onMessage(e.message)}}
  async function loadRun(id:string){try{const r=await api(`/admin/simulation/${id}`);setRun(r);return r}catch(e:any){onMessage(e.message)}}
  async function start(){if(payPct+latePct>100){onMessage('Pay % + late % cannot exceed 100');return}setBusy(true);setShowLog(false);
    if(mode==='same-room-fallback'&&!roomId){onMessage('Select the target hotel and room first');setBusy(false);return}
    try{const x=await api('/admin/simulation',{method:'POST',body:JSON.stringify({mode,customers,payRatio:payPct/100,lateRatio:latePct/100,windowSeconds:windowS,roomId})});onMessage(`Simulation started: ${x.params.customers} customers`);
      let r=await loadRun(x.runId);while(r&&r.status==='RUNNING'){await new Promise(res=>setTimeout(res,1000));r=await loadRun(x.runId)}
      onMessage(r?.status==='DONE'?'Simulation finished':'Simulation failed: '+(r?.report?.error||''));loadRuns()}
    catch(e:any){onMessage(e.message)}finally{setBusy(false)}}
  async function cancelAll(){if(!run)return;if(!confirm(`Cancel every active booking made by the ${run.customers} customers of this run?`))return;
    try{const x=await api(`/admin/simulation/${run.id}/cancel-all`,{method:'POST'});onMessage(x.message);loadRun(run.id);loadRuns()}catch(e:any){onMessage(e.message)}}
  useEffect(()=>{loadRuns()},[]);
  const r=run?.report||{};const c=r.counts||{};
  return <>
    <h3>Load simulation: {customers} customers book at once</h3>
    <div className="row">
      <label>Scenario<select value={mode} onChange={e=>setMode(e.target.value as any)}>
        <option value="random-week">Random rooms this week · pay / late / abandon mix</option>
        <option value="same-room-fallback">Same room, 7 nights · fallback to other room, then other hotel</option></select></label>
      <label>Customers<input type="number" min={1} max={500} value={customers} onChange={e=>setCustomers(Number(e.target.value))}/></label>
      {mode==='random-week'&&<>
        <label>Pay immediately %<input type="number" min={0} max={100} value={payPct} onChange={e=>setPayPct(Number(e.target.value))}/></label>
        <label>Late (expire, then rebook) %<input type="number" min={0} max={100} value={latePct} onChange={e=>setLatePct(Number(e.target.value))}/></label>
        <label>Payment window (s)<input type="number" min={3} max={60} value={windowS} onChange={e=>setWindowS(Number(e.target.value))}/></label></>}
      {mode==='same-room-fallback'&&<>
        <label>Target hotel<select value={hotelId} onChange={e=>setHotelId(e.target.value)}>{hotels.map(h=><option key={h.id} value={h.id}>{h.name} ({h.city})</option>)}</select></label>
        <label>Target room<select value={roomId} onChange={e=>setRoomId(e.target.value)}>{rooms.map(r=><option key={r.id} value={r.id}>{r.name} · {r.total_rooms} rooms · {baht(r.price)}/night</option>)}</select></label></>}
      <button onClick={start} disabled={busy}>{busy?'Running…':'Run simulation'}</button>
      {runs.length>0&&<label>Previous runs<select value={run?.id||''} onChange={e=>e.target.value&&loadRun(e.target.value)}><option value="">Select…</option>{runs.map(x=><option key={x.id} value={x.id}>{new Date(x.createdAt).toLocaleTimeString()} · {x.customers} customers · {x.status} · {x.activeBookings} active</option>)}</select></label>}
    </div>
    {mode==='random-week'?<small>Each customer books a random room for random nights within the coming week, all in the same instant. {payPct}% pay at once, {latePct}% let the {windowS}s lock expire and then try to rebook the same room, the rest abandon. Customers rejected as sold out retry once on another room. Every step is timed.</small>
    :<small>All {customers} customers try to book the <b>same room for 7 nights</b> starting today, in the same instant. A customer who is rejected tries the other room types of that hotel, one by one; if the whole hotel is full they try up to 3 other hotels. Whoever gets a room pays immediately. The report shows how the crowd cascades through the inventory.</small>}
    {run&&run.status==='RUNNING'&&<p className="lockinfo">Running… {r.phase||'booking burst'}</p>}
    {run&&run.status==='FAILED'&&<p className="notice">Failed: {r.error}</p>}
    {run&&run.status==='DONE'&&<div className="report">
      <div className="row between"><h4>Report · {new Date(run.createdAt).toLocaleString()} · {run.customers} customers</h4>
        <div className="row"><button className="danger" onClick={cancelAll} disabled={!run.activeBookings}>Cancel all {run.activeBookings} active bookings of these customers</button><button onClick={()=>loadRun(run.id)}>Refresh</button></div></div>
      {r.mode==='same-room-fallback'&&r.target&&<p><small>Target: <b>{r.target.hotel} / {r.target.room}</b> ({r.target.city}), {r.target.totalRooms} rooms, {fmt(r.target.checkIn)} → {fmt(r.target.checkOut)} ({r.target.nights} nights). Same hotel has {r.target.sameHotelOtherRooms} other room types with {r.target.sameHotelOtherCapacity} rooms; {r.target.otherHotels} other hotels available.</small></p>}
      <div className="tiles">
        <Tile label="Total time" value={`${(r.durationMs/1000).toFixed(1)} s`} hint={r.mode==='same-room-fallback'?'incl. outbox drain':'incl. waiting for locks to expire'}/>
        <Tile label="Booking burst" value={ms(r.burstMs)} hint={`${r.throughputPerSec} ${r.mode==='same-room-fallback'?'attempts':'bookings'}/s`}/>
        {r.mode==='same-room-fallback'?<>
          <Tile label="Got target room" value={r.tiers.targetRoom} hint={`of ${r.target?.totalRooms} rooms`}/>
          <Tile label="Other room, same hotel" value={r.tiers.sameHotelOtherRoom} hint="fallback 1"/>
          <Tile label="Other hotel" value={r.tiers.otherHotel} hint="fallback 2"/>
          <Tile label="No room" value={r.tiers.noRoom} hint="gave up"/>
          <Tile label="Attempts" value={r.totalAttempts} hint={`${r.avgAttemptsPerCustomer} per customer`}/>
        </>:<>
          <Tile label="Locked" value={c.locked} hint={`${c.soldOut} sold out at first try`}/>
          <Tile label="Paid" value={c.paid} hint={`${c.payFailed} payments failed`}/>
          <Tile label="Timed out" value={c.timedOut} hint={`${c.late} late + ${c.abandoned} abandoned`}/>
          <Tile label="Rebooked" value={c.rebooked} hint={`${c.rebookSoldOut} lost the room`}/>
        </>}
        <Tile label="Active now" value={run.activeBookings} hint="confirmed + pending"/>
        <Tile label="Errors" value={c.errors}/>
      </div>
      {r.mode==='same-room-fallback'&&<>
        <h4>Cascade</h4>
        <div className="tablewrap"><table><thead><tr><th>Tier</th><th>Attempts</th><th>Customers who ended here</th><th>Share</th></tr></thead><tbody>
          {[['targetRoom','1 · Target room'],['sameHotelOtherRoom','2 · Other room type, same hotel'],['otherHotel','3 · Other hotel'],['noRoom','No room']].map(([k,l])=><tr key={k}><td>{l}</td><td>{r.attemptsPerTier[k]??'—'}</td><td>{r.tiers[k]}</td><td><div className="bar"><i style={{width:`${100*r.tiers[k]/run.customers}%`}}/></div>{Math.round(100*r.tiers[k]/run.customers)}%</td></tr>)}
        </tbody></table></div>
        <h4>Where the customers ended up <small>({(r.roomsWon||[]).length} rooms)</small></h4>
        <div className="tablewrap"><table><thead><tr><th>Room</th><th>Customers</th></tr></thead><tbody>{(r.roomsWon||[]).slice(0,allRooms?undefined:12).map((x:any)=><tr key={x.room}><td>{x.room}</td><td>{x.count}</td></tr>)}</tbody></table></div>
        {(r.roomsWon||[]).length>12&&<button onClick={()=>setAllRooms(!allRooms)}>{allRooms?'Show top 12':`Show all ${(r.roomsWon||[]).length} rooms`}</button>}
      </>}
      <h4>Bottlenecks (slowest step first, p95)</h4>
      <div className="tablewrap"><table><thead><tr><th>Step</th><th>p95</th><th>Why</th></tr></thead><tbody>
        {(r.bottlenecks||[]).map((b:any)=><tr key={b.step} className={b.p95Ms>1000?'warn':''}><td>{b.step}</td><td>{ms(b.p95Ms)}</td><td><small>{b.note}</small></td></tr>)}
      </tbody></table></div>
      <p><small>{r.mode!=='same-room-fallback'&&<>Inside one booking request, Redis took {r.redisShareOfBookPct}% and PostgreSQL {r.pgShareOfBookPct}% of the time. </>}Outbox: {r.outbox?.published}/{r.outbox?.events} events published, drained in {((r.outbox?.drainMs||0)/1000).toFixed(1)} s, avg lag {ms(r.outbox?.avgMs||0)}.</small></p>
      <h4>Timings per operation</h4>
      <div className="tablewrap"><table><thead><tr><th>Operation</th><th>Count</th><th>Avg</th><th>p50</th><th>p95</th><th>Max</th></tr></thead><tbody>
        {Object.entries(r.timings||{}).filter(([,v]:any)=>v.count).map(([k,v]:any)=><tr key={k}><td>{k}</td><td>{v.count}</td><td>{ms(v.avgMs)}</td><td>{ms(v.p50Ms)}</td><td>{ms(v.p95Ms)}</td><td>{ms(v.maxMs)}</td></tr>)}
      </tbody></table></div>
      <h4>What happened to the customers</h4>
      <div className="tablewrap"><table><thead><tr><th>Outcome</th><th>Customers</th></tr></thead><tbody>
        {(r.outcomes||[]).map((o:any)=><tr key={o.outcome}><td>{o.outcome}</td><td>{o.count}</td></tr>)}
        <tr><td><b>Final booking statuses</b></td><td>{Object.entries(r.finalStatuses||{}).map(([k,v]:any)=>`${STATUS_LABEL[k]||k}: ${v}`).join(' · ')}</td></tr>
      </tbody></table></div>
      <button onClick={()=>setShowLog(!showLog)}>{showLog?'Hide':'Show'} timeline</button>
      {showLog&&<pre className="log">{(r.log||[]).join('\n')}</pre>}
    </div>}
  </>;
}

function App(){
  const [user,setUser]=useState<any>(JSON.parse(localStorage.getItem('user')||'null'));
  const [hotels,setHotels]=useState<any[]>([]); const [selected,setSelected]=useState<any>(null);
  const [email,setEmail]=useState('customer@example.com'); const [password,setPassword]=useState('customer123');
  const [city,setCity]=useState('');
  const [checkIn,setCheckIn]=useState(today); const [checkOut,setCheckOut]=useState(plusDays(today,1));
  const [view,setView]=useState<'search'|'bookings'|'myhotels'|'dashboard'>(user?.role==='SELLER'?'dashboard':'search');
  const [dashDays,setDashDays]=useState(7);
  const [myHotels,setMyHotels]=useState<any[]>([]); const [hotelBookings,setHotelBookings]=useState<any>(null);
  const [bookings,setBookings]=useState<any[]>([]);
  const [customers,setCustomers]=useState<any[]>([]); const [customerId,setCustomerId]=useState('');
  const [adminHotels,setAdminHotels]=useState<any[]>([]); const [adminHotelId,setAdminHotelId]=useState(''); const [adminRooms,setAdminRooms]=useState<any[]>([]); const [adminRoomId,setAdminRoomId]=useState('');
  const [confirmNow,setConfirmNow]=useState(false); const [race,setRace]=useState<any>(null); const [allCustomerCount,setAllCustomerCount]=useState(0);
  const [message,setMessage]=useState('');
  const [tick,setTick]=useState(0); // re-renders once a second while a payment countdown is running
  const [accounts,setAccounts]=useState<any[]>(ACCOUNTS); const [accountFilter,setAccountFilter]=useState('');
  useEffect(()=>{if(!user)api('/auth/demo-accounts').then((rows:any[])=>{if(rows.length)setAccounts(rows.map(r=>({label:r.name,email:r.email,password:r.password,role:r.role})))}).catch(()=>{})},[user]);

  const nights=Math.max(0,Math.round((Date.parse(checkOut)-Date.parse(checkIn))/86400000));
  const rangeOk=checkIn>=today&&checkOut>checkIn;
  const range=`checkIn=${checkIn}&checkOut=${checkOut}`;

  async function login(e=email,p=password){try{const x=await api('/auth/login',{method:'POST',body:JSON.stringify({email:e,password:p})});localStorage.setItem('token',x.token);localStorage.setItem('user',JSON.stringify(x.user));setUser(x.user);setView(x.user.role==='SELLER'?'dashboard':'search');setMessage('Logged in')}catch(err:any){setMessage(err.message+(e.includes('example.com')&&e!=='admin@example.com'?' (login as Admin and Generate Sample Data first)':''))}}
  function quickLogin(a:{email:string,password:string}){setEmail(a.email);setPassword(a.password);login(a.email,a.password)}
  async function search(){const x=await api('/hotels?city='+encodeURIComponent(city));setHotels(x.items)}
  async function openHotel(id:string){if(!rangeOk){setMessage('Choose a valid date range first');return}try{setSelected(await api(`/hotels/${id}?${range}`))}catch(e:any){setMessage(e.message)}}
  async function book(roomId:string){try{const x=await api('/bookings',{method:'POST',body:JSON.stringify({roomId,checkIn,checkOut})});setMessage(`Room locked for you for ${x.paymentWindowSeconds}s: ${x.nights} night(s), ${baht(x.totalPrice)}. Press Pay in My bookings to confirm.`);setSelected(null);setView('bookings');loadBookings()}catch(e:any){setMessage(e.message)}}
  async function pay(id:string){try{await api(`/bookings/${id}/pay`,{method:'POST'});setMessage('Payment received. Booking confirmed.');loadBookings()}catch(e:any){setMessage(e.message);loadBookings()}}
  async function loadBookings(){try{setBookings(await api('/bookings/me'))}catch(e:any){setMessage(e.message)}}
  async function cancel(id:string){if(!confirm('Cancel this booking?'))return;try{const x=await api(`/bookings/${id}/cancel`,{method:'POST'});setMessage(`Booking cancelled. Rooms available again for these dates: ${x.remaining}`);loadBookings()}catch(e:any){setMessage(e.message)}}
  async function loadMyHotels(){try{setMyHotels(await api('/seller/hotels'))}catch(e:any){setMessage(e.message)}}
  async function openHotelBookings(h:any){try{setHotelBookings({hotel:h,rows:await api(`/seller/hotels/${h.id}/bookings`)})}catch(e:any){setMessage(e.message)}}
  async function loadAdminHotels(){try{const hs=(await api('/hotels?limit=100')).items;setAdminHotels(hs);if(!hs.find((h:any)=>h.id===adminHotelId))setAdminHotelId(hs[0]?.id||'')}catch(e:any){setMessage(e.message)}}
  async function loadAdminRooms(hid:string){if(!hid){setAdminRooms([]);setAdminRoomId('');return}try{const h=await api(`/hotels/${hid}?${range}`);setAdminRooms(h.rooms);if(!h.rooms.find((r:any)=>r.id===adminRoomId))setAdminRoomId(h.rooms[0]?.id||'')}catch(e:any){setMessage(e.message)}}
  async function raceBooking(){if(!adminRoomId){setMessage('Select a hotel and room first');return}if(!rangeOk){setMessage('Choose a valid date range first');return}try{const x=await api('/admin/concurrent-booking',{method:'POST',body:JSON.stringify({roomId:adminRoomId,checkIn,checkOut,confirm:confirmNow})});setRace(x);setMessage(x.message);loadAdminRooms(adminHotelId)}catch(e:any){setMessage(e.message)}}
  async function loadCustomers(){try{const all=(await api('/admin/users')).filter((u:any)=>u.role==='CUSTOMER');setAllCustomerCount(all.length);const us=all.filter((u:any)=>!u.email.startsWith('sim-'));setCustomers(us);if(!us.find((u:any)=>u.id===customerId))setCustomerId(us[0]?.id||'')}catch(e:any){setMessage(e.message)}}
  async function sample(){try{setMessage((await api('/admin/sample-data',{method:'POST'})).message);search();loadCustomers();loadAdminHotels()}catch(e:any){setMessage(e.message)}}
  async function delSample(){try{setMessage((await api('/admin/sample-data',{method:'DELETE'})).message);search();loadCustomers();loadAdminHotels();setRace(null)}catch(e:any){setMessage(e.message)}}
  async function sampleBookings(){if(!customerId){setMessage('Select a customer first');return}try{setMessage((await api('/admin/sample-bookings',{method:'POST',body:JSON.stringify({userId:customerId})})).message)}catch(e:any){setMessage(e.message)}}
  function onCheckIn(v:string){setCheckIn(v);if(checkOut<=v)setCheckOut(plusDays(v,1))}
  // Reload the hotel list whenever the logged-in user changes: the API scopes it (sellers see only their hotels).
  useEffect(()=>{search();setSelected(null)},[user?.id]);
  useEffect(()=>{if(view==='bookings'&&user)loadBookings();if(view==='myhotels'&&user)loadMyHotels()},[view]);
  const pending=bookings.filter(b=>b.status==='PENDING');
  useEffect(()=>{ // countdown; when a lock runs out, reload so the server's PAYMENT_TIMEOUT status shows up
    if(view!=='bookings'||pending.length===0)return;
    const t=setInterval(()=>{setTick(x=>x+1);if(pending.some(b=>secondsLeft(b)===0))loadBookings()},1000);
    return()=>clearInterval(t)},[view,pending.map(b=>b.id).join()]);
  useEffect(()=>{if(user?.role==='ADMIN'){loadCustomers();loadAdminHotels()}},[user]);
  useEffect(()=>{if(user?.role==='ADMIN')loadAdminRooms(adminHotelId)},[adminHotelId,checkIn,checkOut]);

  if(!user) return <main><h1>Hotel Booking Lab</h1><div className="card"><h2>Login</h2>
    {/* Demo credentials only: the secret field is a masked text input (not type="password") so Chrome's
        password manager does not offer to save it or warn that "admin123" appears in a data breach. */}
    <input value={email} onChange={e=>setEmail(e.target.value)} placeholder="email" name="demo-email" autoComplete="off" spellCheck={false} data-lpignore="true" data-1p-ignore/>
    <input value={password} onChange={e=>setPassword(e.target.value)} onKeyDown={e=>{if(e.key==='Enter')login()}} placeholder="password" name="demo-secret" className="secret" type="text" autoComplete="off" spellCheck={false} data-lpignore="true" data-1p-ignore/>
    <button onClick={()=>login()}>Login</button>
    <p className="message">{message}</p>
    {(()=>{const q=accountFilter.trim().toLowerCase();const list=accounts.filter(a=>!q||a.email.toLowerCase().includes(q)||(a.label||'').toLowerCase().includes(q)||(a.role||'').toLowerCase().includes(q));
      const counts=accounts.reduce((m:any,a)=>{m[a.role||'?']=(m[a.role||'?']||0)+1;return m},{});
      return <>
      <div className="row between"><h3>All accounts <small>({accounts.length}: {Object.entries(counts).map(([k,v])=>`${v} ${k.toLowerCase()}`).join(', ')}) · click one to log in</small></h3>
        <input value={accountFilter} onChange={e=>setAccountFilter(e.target.value)} placeholder="Filter by email, name or role" autoComplete="off"/></div>
      <div className="accounts-scroll"><table className="accounts"><thead><tr><th>Role</th><th>Name</th><th>Email</th><th>Password</th></tr></thead><tbody>
        {list.map(a=><tr key={a.email} onClick={()=>quickLogin(a)} title="Click to log in"><td><span className={`role ${a.role}`}>{a.role||a.label}</span></td><td>{a.label}</td><td>{a.email}</td><td><code>{a.password}</code></td></tr>)}
        {list.length===0&&<tr><td colSpan={4}>No account matches "{accountFilter}"</td></tr>}
      </tbody></table></div></>})()}
    <p><small>Only Admin exists on a fresh database. Log in as Admin and press "Generate Sample Data" to create sellers and customers; load simulations add customers named "Sim …" (password sim123).</small></p>
    <h3>Services</h3>
    <div className="tablewrap"><table className="services"><thead><tr><th>Service</th><th>Address</th><th>Used for</th></tr></thead><tbody>
      {SERVICES.map(sv=><tr key={sv.name}><td>{sv.name}</td><td>{sv.href?<a href={sv.value} target="_blank" rel="noreferrer">{sv.value}</a>:<code>{sv.value}</code>}</td><td><small>{sv.usedFor}</small></td></tr>)}
    </tbody></table></div>
    <p><small>Peek at Redis: <code>podman exec -it hotel-booking-lab_redis_1 redis-cli KEYS 'availability:*'</code></small></p>
    <p><small>Kafka topics to watch in Kafka UI: booking.created, booking.confirmed, booking.payment_timeout, booking.cancelled, room.availability.changed.</small></p>
  </div></main>;

  return <main>
    <header><h1>Hotel Booking Lab</h1><span>{user.name} · {user.role}</span>
      <nav>
        <button className={view==='search'?'active':''} onClick={()=>setView('search')}>Search</button>
        {user.role==='CUSTOMER'&&<button className={view==='bookings'?'active':''} onClick={()=>setView('bookings')}>My bookings</button>}
        {user.role==='SELLER'&&<button className={view==='dashboard'?'active':''} onClick={()=>setView('dashboard')}>Dashboard</button>}
        {user.role==='SELLER'&&<button className={view==='myhotels'?'active':''} onClick={()=>setView('myhotels')}>My hotels</button>}
        <button onClick={()=>{localStorage.clear();location.reload()}}>Logout</button>
      </nav>
    </header>
    <p className="message">{message}</p>
    {user.role==='ADMIN'&&<section className="card admin"><h2>Admin</h2>
      <div className="row"><button onClick={sample}>Generate Sample Data</button><button onClick={delSample}>Delete Sample Data</button>
        <button onClick={async()=>{try{setMessage((await api('/admin/rebuild-availability',{method:'POST'})).message)}catch(e:any){setMessage(e.message)}}} title="Recompute Redis per-night counters from PostgreSQL bookings (also runs at API startup)">Rebuild Redis availability</button></div>
      <div className="row">
        <select value={customerId} onChange={e=>setCustomerId(e.target.value)} disabled={!customers.length}>
          {customers.length===0&&<option value="">No customers yet</option>}
          {customers.map(c=><option key={c.id} value={c.id}>{c.name} ({c.email})</option>)}
        </select>
        <button onClick={sampleBookings} disabled={!customerId}>Create Sample Bookings for selected customer</button>
      </div>
      <small>Creates 3 future bookings for that customer only. Two of them overlap so the double-booking notice can be seen in their "My bookings".</small>
      <h3>Concurrency demo: all customers book one room at the same time</h3>
      <div className="row">
        <select value={adminHotelId} onChange={e=>setAdminHotelId(e.target.value)} disabled={!adminHotels.length}>
          {adminHotels.length===0&&<option value="">No hotels yet</option>}
          {adminHotels.map(h=><option key={h.id} value={h.id}>{h.name} ({h.city})</option>)}
        </select>
        <select value={adminRoomId} onChange={e=>setAdminRoomId(e.target.value)} disabled={!adminRooms.length}>
          {adminRooms.length===0&&<option value="">No rooms</option>}
          {adminRooms.map(r=><option key={r.id} value={r.id}>{r.name} · {r.availableRooms}/{r.total_rooms} available · {baht(r.price)}/night</option>)}
        </select>
      </div>
      <div className="row">
        <label>Check-in<input type="date" min={today} value={checkIn} onChange={e=>onCheckIn(e.target.value)}/></label>
        <label>Check-out<input type="date" min={plusDays(checkIn,1)} value={checkOut} onChange={e=>setCheckOut(e.target.value)}/></label>
        <label className="check"><input type="checkbox" checked={confirmNow} onChange={e=>setConfirmNow(e.target.checked)}/>Confirm immediately (skip the 60s payment window)</label>
        <button onClick={raceBooking} disabled={!adminRoomId||!allCustomerCount}>Book this room with all {allCustomerCount} customers at once</button>
      </div>
      <small>Every customer (including simulation customers, if any) sends a booking for the same room and dates in the same instant. The atomic Redis script decides who gets a room; the others are rejected. Without "confirm immediately", the winners' rooms are released again after 60s unless that customer pays.</small>
      {race&&<div className="tablewrap"><table><thead><tr><th>Customer</th><th>Result</th><th>Rooms left after</th></tr></thead><tbody>
        {race.results.map((x:any)=><tr key={x.email} className={x.ok?'':'warn'}><td>{x.customer}<br/><small>{x.email}</small></td><td>{x.ok?<span className={`status ${x.status}`}>{STATUS_LABEL[x.status]}</span>:<span className="notice">✕ {x.error}</span>}</td><td>{x.ok?x.remaining:'—'}</td></tr>)}
      </tbody></table><small>{race.hotel} / {race.room} · {fmt(race.checkIn)} → {fmt(race.checkOut)} · availability {race.availabilityBefore} → {race.availabilityAfter} · {race.durationMs} ms</small></div>}
      <SimulationPanel onMessage={setMessage} hotels={adminHotels} rooms={adminRooms} hotelId={adminHotelId} roomId={adminRoomId} setHotelId={setAdminHotelId} setRoomId={setAdminRoomId}/>
    </section>}

    {user.role==='SELLER'&&view==='search'&&<p><small>As a seller, search only shows hotels you own.</small></p>}
    {view==='dashboard'&&user.role==='SELLER'&&<SellerDashboard days={dashDays} setDays={setDashDays} onMessage={setMessage}/>}
    {view==='myhotels'&&<section className="card">
      <h2>My hotels</h2>
      {myHotels.length===0?<p>You do not own any hotels yet.</p>:
      <div className="tablewrap"><table><thead><tr><th>Hotel</th><th>City</th><th>Address</th><th>Rooms</th><th>From / night</th><th>Upcoming bookings</th><th></th></tr></thead><tbody>
        {myHotels.map(h=><tr key={h.id}>
          <td><b>{h.name}</b></td><td>{h.city}</td><td>{h.address}</td><td>{h.roomCount}</td><td>{baht(h.startingPrice)}</td><td>{h.upcomingBookings}</td>
          <td className="actions"><button onClick={()=>openHotel(h.id)}>Availability</button><button onClick={()=>openHotelBookings(h)}>Bookings</button></td>
        </tr>)}
      </tbody></table></div>}
    </section>}

    {hotelBookings&&<div className="modal" onClick={()=>setHotelBookings(null)}><div className="card wide" onClick={e=>e.stopPropagation()}><button onClick={()=>setHotelBookings(null)}>Close</button>
      <h2>Bookings · {hotelBookings.hotel.name}</h2>
      {hotelBookings.rows.length===0?<p>No bookings for this hotel.</p>:
      <div className="tablewrap"><table><thead><tr><th>Customer</th><th>Room</th><th>Check-in</th><th>Check-out</th><th>Nights</th><th>Total</th><th>Status</th></tr></thead><tbody>
        {hotelBookings.rows.map((b:any)=><tr key={b.id}><td>{b.customerName}<br/><small>{b.customerEmail}</small></td><td>{b.roomName}</td><td>{fmt(b.checkIn)}</td><td>{fmt(b.checkOut)}</td><td>{b.nights}</td><td>{baht(b.price)}</td><td><span className={`status ${b.status}`}>{STATUS_LABEL[b.status]||b.status}</span></td></tr>)}
      </tbody></table></div>}
    </div></div>}

    {view==='search'&&<>
      <section className="search">
        <input value={city} onChange={e=>setCity(e.target.value)} placeholder="City, e.g. Bangkok"/>
        <label>Check-in<input type="date" min={today} value={checkIn} onChange={e=>onCheckIn(e.target.value)}/></label>
        <label>Check-out<input type="date" min={plusDays(checkIn,1)} value={checkOut} onChange={e=>setCheckOut(e.target.value)}/></label>
        <span className="nights">{rangeOk?`${nights} night${nights===1?'':'s'}`:'Invalid dates'}</span>
        <button onClick={search}>Search</button>
      </section>
      <section className="grid">{hotels.map(h=><article className="card" key={h.id} onClick={()=>openHotel(h.id)}><img loading="lazy" src={h.thumbnail}/><h3>{h.name}</h3><p>{h.city} · {h.address}</p><b>From {baht(h.startingPrice)} / night</b></article>)}</section>
    </>}

    {view==='bookings'&&<section className="card">
      <h2>My bookings</h2>
      {pending.length>0&&<p className="lockinfo">You have {pending.length} room{pending.length===1?'':'s'} locked. Pay before the countdown ends or the room is released to other customers.</p>}
      {bookings.length===0?<p>No bookings yet.</p>:
      <div className="tablewrap"><table className="bookings"><thead><tr><th>Hotel</th><th>City</th><th>Room</th><th>Check-in</th><th>Check-out</th><th>Nights</th><th>Total</th><th>Status</th><th>Note</th><th>Stay</th></tr></thead><tbody>
        {bookings.map(b=><tr key={b.id} className={b.status==='PENDING'?'pending':b.overlaps?.length?'warn':''}>
          <td><b>{b.hotelName}</b></td><td>{b.city}</td><td>{b.roomName}</td><td>{fmt(b.checkIn)}</td><td>{fmt(b.checkOut)}</td><td>{b.nights}</td><td>{baht(b.price)}</td>
          <td><span className={`status ${b.status}`}>{STATUS_LABEL[b.status]||b.status}</span>{b.status==='PENDING'&&<><br/><span className="countdown">{secondsLeft(b)}s left</span></>}</td>
          <td>{b.overlaps?.length>0&&<span className="notice" title={b.overlaps.map((o:any)=>`${o.hotelName} (${o.city}) ${fmt(o.checkIn)} → ${fmt(o.checkOut)}`).join('\n')}>⚠ Double booking: same dates as {b.overlaps.map((o:any)=>o.hotelName).join(', ')}</span>}</td>
          <td className="actions">
            {(()=>{const ph=stayPhase(b);return ph&&<span className={`phase ${ph.key}`} title={ph.hint}><b>{ph.label}</b><br/><small>{ph.hint}</small></span>})()}
            {b.status==='PENDING'&&<button className="pay" onClick={()=>pay(b.id)} disabled={secondsLeft(b)===0}>Pay {baht(b.price)}</button>}
            {(b.status==='CONFIRMED'||b.status==='PENDING')&&b.checkIn>today&&<button className="danger" onClick={()=>cancel(b.id)}>Cancel</button>}
          </td>
        </tr>)}
      </tbody></table></div>}
    </section>}

    {selected&&<div className="modal" onClick={()=>setSelected(null)}><div className="card" onClick={e=>e.stopPropagation()}><button onClick={()=>setSelected(null)}>Close</button><h2>{selected.name}</h2><p>{selected.city} · {selected.address}</p><p>{selected.description}</p>
      <h3>Rooms · {fmt(selected.checkIn)} → {fmt(selected.checkOut)} ({selected.nights} night{selected.nights===1?'':'s'})</h3>
      {selected.rooms.map((r:any)=><div className="room" key={r.id}><span><b>{r.name}</b><br/>{baht(r.price)} / night · {baht(r.totalPrice)} total · {r.availableRooms} available</span><button disabled={r.availableRooms<1||user.role!=='CUSTOMER'} onClick={()=>book(r.id)}>Book</button></div>)}
    </div></div>}
  </main>
}
createRoot(document.getElementById('root')!).render(<App/>);
