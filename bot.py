import os, time, asyncio, logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ATLAS AI V7.0 — S/R MAP ENGINE
# Research/paper-signal bot. No order execution and no profit guarantee.
# Public concepts implemented: higher-TF S/R zones, role reversal,
# rejection, failed breakout/sweep, breakout-retest, 20 EMA, MTF regime,
# volume confirmation, VWAP context, ATR-based stop/targets.

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("atlas")
TOKEN=os.getenv("BOT_TOKEN","").strip(); ALLOWED=os.getenv("ALLOWED_CHAT_ID","").strip(); SIGNAL_CHAT=os.getenv("SIGNAL_CHAT_ID","").strip()
INTERVAL=max(60,int(os.getenv("SCAN_INTERVAL","120"))); UNIVERSE_SIZE=max(20,min(100,int(os.getenv("UNIVERSE_SIZE","80"))))
THRESHOLD=max(60,min(100,int(os.getenv("RADAR_THRESHOLD","78")))); COOLDOWN=max(0,int(os.getenv("SIGNAL_COOLDOWN","3600"))); WORKERS=max(4,min(12,int(os.getenv("MAX_WORKERS","8"))))
BASE="https://api.binance.com"; S=requests.Session(); S.headers.update({"User-Agent":"AtlasAI-V7.0"})
state={"scans":0,"last":0.0,"universe":0,"scanned":0,"ready":0,"signals":0,"errors":0,"skipped":0,"last_error":"","top":[],"running":False}
sent=defaultdict(float); lock=asyncio.Lock(); scan_task=None

def ok(u): return not ALLOWED or (u.effective_chat and str(u.effective_chat.id)==ALLOWED)
async def guard(u):
    if ok(u): return True
    if u.effective_message: await u.effective_message.reply_text("⛔ Yetkiniz yok.")
    return False

def api(path,p=None):
    r=S.get(BASE+path,params=p,timeout=15); r.raise_for_status(); return r.json()
def f(v,d=None):
    try: x=float(v); return d if x!=x else x
    except (TypeError,ValueError): return d

def ema(v,n):
    if len(v)<n:return None
    x=sum(v[:n])/n; k=2/(n+1)
    for z in v[n:]: x=z*k+x*(1-k)
    return x

def rsi(v,n=14):
    if len(v)<=n:return 50.0
    g=l=0.0
    for i in range(len(v)-n,len(v)):
        d=v[i]-v[i-1]; g+=max(d,0); l+=max(-d,0)
    if l==0:return 100.0 if g else 50.0
    q=g/l; return 100-100/(1+q)

def atr(c,n=14):
    if len(c)<n+1:return 0.0
    tr=[]
    for i in range(1,len(c)):
        h,l,pc=c[i]["h"],c[i]["l"],c[i-1]["c"]; tr.append(max(h-l,abs(h-pc),abs(l-pc)))
    return sum(tr[-n:])/min(n,len(tr))

def candles(sym,tf,limit=160):
    raw=api("/api/v3/klines",{"symbol":sym,"interval":tf,"limit":limit}); out=[]
    for x in raw:
        try: out.append({"t":int(x[0]),"o":float(x[1]),"h":float(x[2]),"l":float(x[3]),"c":float(x[4]),"v":float(x[5])})
        except (ValueError,TypeError,IndexError): pass
    return out

def two_min(m):
    out=[]
    for i in range(0,len(m)-1,2):
        a,b=m[i],m[i+1]
        if b["t"]!=a["t"]+60000: continue
        out.append({"t":a["t"],"o":a["o"],"h":max(a["h"],b["h"]),"l":min(a["l"],b["l"]),"c":b["c"],"v":a["v"]+b["v"]})
    return out

def regime(c):
    if len(c)<60:return "NEUTRAL"
    v=[x["c"] for x in c]; e20,e50=ema(v,20),ema(v,50); r=rsi(v)
    if e20 is None or e50 is None:return "NEUTRAL"
    if v[-1]>e20>e50 and r>=52:return "BULLISH"
    if v[-1]<e20<e50 and r<=48:return "BEARISH"
    return "RANGE"

def pivots(c):
    s=[]; r=[]
    for i in range(2,len(c)-2):
        if c[i]["h"]>=max(c[i-2]["h"],c[i-1]["h"],c[i+1]["h"],c[i+2]["h"]):r.append((c[i]["h"],c[i]["t"]))
        if c[i]["l"]<=min(c[i-2]["l"],c[i-1]["l"],c[i+1]["l"],c[i+2]["l"]):s.append((c[i]["l"],c[i]["t"]))
    return s,r

def clusters(levels,tol):
    z=[]
    for p,t,w in sorted(levels):
        if z and abs(p-z[-1]["p"])<=tol:
            q=z[-1]; q["p"]=(q["p"]*q["n"]+p)/(q["n"]+1); q["n"]+=1; q["w"]+=w; q["t"]=max(q["t"],t)
        else:z.append({"p":p,"n":1,"w":w,"t":t})
    return z

def sr_map(frames):
    weights={"1d":4,"4h":3,"1h":2,"15m":1}; sup=[]; res=[]
    for tf,c in frames.items():
        if len(c)<40:continue
        ps,pr=pivots(c)
        for p,t in ps:sup.append((p,t,weights[tf]))
        for p,t in pr:res.append((p,t,weights[tf]))
    price=frames["15m"][-1]["c"]; tol=max(atr(frames["15m"])*.65,price*.002)
    S=clusters(sup,tol); R=clusters(res,tol)
    S=sorted([x for x in S if x["p"]<=price],key=lambda x:x["p"],reverse=True)[:5]
    R=sorted([x for x in R if x["p"]>=price],key=lambda x:x["p"])[:5]
    return S,R

def near(price,z,a): return bool(z and abs(price-z["p"])<=max(a*.7,price*.0025))
def reject(c,side):
    x=c[-1]; body=abs(x["c"]-x["o"]); rng=max(x["h"]-x["l"],1e-12); lo=min(x["o"],x["c"])-x["l"]; hi=x["h"]-max(x["o"],x["c"])
    return (lo>=body*1.2 and lo/rng>=.35) if side=="LONG" else (hi>=body*1.2 and hi/rng>=.35)
def sweep(c,side,level):
    if level is None:return False
    x=c[-1]; return (x["l"]<level and x["c"]>level) if side=="LONG" else (x["h"]>level and x["c"]<level)
def retest(c,side,level):
    if level is None or len(c)<4:return False
    a,x=c[-3],c[-1]
    return (a["c"]>level and x["l"]<=level*1.003 and x["c"]>level) if side=="LONG" else (a["c"]<level and x["h"]>=level*.997 and x["c"]<level)
def vol_ok(c):
    if len(c)<22:return False
    avg=sum(x["v"] for x in c[-21:-1])/20
    return bool(avg and c[-1]["v"]>=avg*1.15)
def vwap(c):
    q=c[-60:]; v=sum(x["v"] for x in q); return sum(x["c"]*x["v"] for x in q)/v if v else q[-1]["c"]

def analyze(sym):
    try:
        F={tf:candles(sym,tf) for tf in ("1d","4h","1h","15m")}
        if any(len(c)<60 for c in F.values()):return None,"mtf"
        m=two_min(candles(sym,"1m",120))
        if len(m)<40:return None,"2m"
        price=F["15m"][-1]["c"]; a=atr(F["15m"]); S,R=sr_map(F)
        s=S[0] if S else None; r=R[0] if R else None
        regs={tf:regime(c) for tf,c in F.items()}; bull=sum(x=="BULLISH" for x in regs.values()); bear=sum(x=="BEARISH" for x in regs.values())
        mv=[x["c"] for x in m]; e9,e20=ema(mv,9),ema(mv,20); rr=rsi(mv); vw=vwap(F["15m"]); volume=vol_ok(F["15m"])
        lp=sp=0; lr=[]; sr=[]
        if bull>=3:lp+=22;lr.append("MTF bullish")
        elif bull==2:lp+=10
        if bear>=3:sp+=22;sr.append("MTF bearish")
        elif bear==2:sp+=10
        ns,nr=near(price,s,a),near(price,r,a)
        if ns:lp+=24;lr.append("SUPPORT ZONE")
        if nr:sp+=24;sr.append("RESISTANCE ZONE")
        if ns and reject(m,"LONG"):lp+=15;lr.append("support rejection")
        if nr and reject(m,"SHORT"):sp+=15;sr.append("resistance rejection")
        if s and retest(F["15m"],"LONG",s["p"]):lp+=12;lr.append("S→S retest")
        if r and retest(F["15m"],"SHORT",r["p"]):sp+=12;sr.append("R→R retest")
        if s and sweep(F["15m"],"LONG",s["p"]):lp+=10;lr.append("sell-side sweep")
        if r and sweep(F["15m"],"SHORT",r["p"]):sp+=10;sr.append("buy-side sweep")
        if e9 and e20 and e9>e20 and rr>=52:lp+=8
        if e9 and e20 and e9<e20 and rr<=48:sp+=8
        if price>vw:lp+=5
        if price<vw:sp+=5
        if volume: (lr.append("volume") if lp>=sp else sr.append("volume"))
        if lp>=sp: side,score,reasons,level="LONG",lp,lr,s; at=ns or sweep(F["15m"],"LONG",s["p"] if s else None)
        else: side,score,reasons,level="SHORT",sp,sr,r; at=nr or sweep(F["15m"],"SHORT",r["p"] if r else None)
        score=min(100,int(score)); ready=score>=THRESHOLD and at
        if not a:return None,"atr"
        entry=price; z=level["p"] if level else entry
        if side=="LONG":
            stop=min(z-.35*a,entry-.9*a); risk=max(entry-stop,entry*.002); tp1=entry+1.5*risk; tp2=entry+2.5*risk
        else:
            stop=max(z+.35*a,entry+.9*a); risk=max(stop-entry,entry*.002); tp1=entry-1.5*risk; tp2=entry-2.5*risk
        return {"symbol":sym,"side":side,"score":score,"ready":ready,"entry":entry,"stop":stop,"tp1":tp1,"tp2":tp2,"support":s["p"] if s else None,"resistance":r["p"] if r else None,"regimes":regs,"reasons":reasons[:5],"volume":volume},None
    except Exception as e:return None,f"{type(e).__name__}: {e}"

def universe():
    info=api("/api/v3/exchangeInfo"); allowed={x["symbol"] for x in info.get("symbols",[]) if x.get("status")=="TRADING" and x.get("quoteAsset")=="USDT" and x.get("isSpotTradingAllowed",False)}
    banned=("UPUSDT","DOWNUSDT","BULLUSDT","BEARUSDT"); rows=[]
    for t in api("/api/v3/ticker/24hr"):
        s=t.get("symbol"); q=f(t.get("quoteVolume"))
        if s in allowed and q and q>0 and not any(s.endswith(x) for x in banned):rows.append((q,s))
    rows.sort(reverse=True); return [s for _,s in rows[:UNIVERSE_SIZE]]

def scan():
    U=universe(); out=[]; errors=0
    with ThreadPoolExecutor(max_workers=WORKERS) as p:
        fs={p.submit(analyze,s):s for s in U}
        for fu in as_completed(fs):
            d,e=fu.result()
            if d:out.append(d)
            else:errors+=1
    out.sort(key=lambda x:x["score"],reverse=True); return U,out,errors

def signal_text(x):
    return (f"🎯 ATLAS AI V7.0\n\n{x['symbol']} — {'🟢 LONG' if x['side']=='LONG' else '🔴 SHORT'} — {x['score']}/100\n"
            f"Entry: {x['entry']:.8g}\nStop: {x['stop']:.8g}\nTP1: {x['tp1']:.8g}\nTP2: {x['tp2']:.8g}\n\n"
            f"S: {x['support'] or '-'}\nR: {x['resistance'] or '-'}\n"
            f"MTF: 1D {x['regimes']['1d']} | 4H {x['regimes']['4h']} | 1H {x['regimes']['1h']} | 15M {x['regimes']['15m']}\n"
            f"Volume: {'YES' if x['volume'] else 'NO'}\nReasons: {', '.join(x['reasons']) or '-'}\n\n⚠️ Paper/research signal; no guaranteed profit.")

async def send_signal(app,x):
    chat=SIGNAL_CHAT or ALLOWED
    if not chat:return
    key=x['symbol']+':'+x['side']; now=time.time()
    if now-sent[key]<COOLDOWN:return
    await app.bot.send_message(chat_id=chat,text=signal_text(x)); sent[key]=now; state['signals']+=1

async def do_scan(app):
    if lock.locked():return
    async with lock:
        state['running']=True
        try:
            U,R,E=await asyncio.to_thread(scan); state.update(scans=state['scans']+1,last=time.time(),universe=len(U),scanned=len(R),ready=sum(x['ready'] for x in R),errors=E,skipped=len(U)-len(R),last_error='',top=R[:5])
            for x in R:
                if x['ready']:await send_signal(app,x)
        except Exception as e:state['errors']+=1;state['last_error']=f'{type(e).__name__}: {e}';log.exception('scan failed')
        finally:state['running']=False

async def scanner(app):
    await asyncio.sleep(3)
    while True:
        await do_scan(app); await asyncio.sleep(INTERVAL)

async def start(u,c):
    if await guard(u):await u.effective_message.reply_text(f"🚀 ATLAS AI V7.0\nS/R MAP + MTF + rejection + retest + sweep aktif.\nTarama: {INTERVAL}s | Universe: {UNIVERSE_SIZE}")
async def status(u,c):
    if not await guard(u):return
    last='yok' if not state['last'] else f"{int(time.time()-state['last'])}s önce"
    await u.effective_message.reply_text(f"🧭 ATLAS AI V7.0 STATUS\nTarama: {state['scans']}\nSon tarama: {last}\nUniverse: {state['universe']}\nTaranan: {state['scanned']}\nTRADE READY: {state['ready']}\nToplam sinyal: {state['signals']}\nHata: {state['errors']}\nAtlanan: {state['skipped']}\nSon hata: {state['last_error'] or 'Yok'}")
async def diagnostics(u,c):
    if not await guard(u):return
    last='yok' if not state['last'] else f"{int(time.time()-state['last'])}s önce"; lines=[f"🛠 ATLAS AI V7.0 DIAGNOSTICS\nTarama: {state['scans']}",f"Son tarama: {last}",f"Universe: {state['universe']}",f"Taranan: {state['scanned']}",f"TRADE READY: {state['ready']}",f"Toplam sinyal: {state['signals']}",f"Hata: {state['errors']}",f"Atlanan: {state['skipped']}",f"Durum: {'SCANNING' if state['running'] else 'IDLE'}","","🏆 TOP 5"]
    lines += [f"{i}. {x['symbol']} {x['side']} {x['score']}/100 {'READY' if x['ready'] else 'WATCH'}" for i,x in enumerate(state['top'],1)]
    await u.effective_message.reply_text('\n'.join(lines))
async def test(u,c):
    if not await guard(u):return
    try:
        d=candles('BTCUSDT','1m',3);await u.effective_message.reply_text(f"🧪 TEST OK\nBinance: {'✅' if len(d)==3 else '⚠️'}\nBTCUSDT 1m: {len(d)}")
    except Exception as e:await u.effective_message.reply_text(f"❌ TEST ERROR: {e}")
async def post_init(app):
    global scan_task;scan_task=asyncio.create_task(scanner(app))
async def post_shutdown(app):
    global scan_task
    if scan_task:scan_task.cancel()
def main():
    if not TOKEN:raise RuntimeError('BOT_TOKEN is required')
    app=Application.builder().token(TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()
    for name,fn in [('start',start),('status',status),('diagnostics',diagnostics),('test',test)]:app.add_handler(CommandHandler(name,fn))
    app.run_polling(drop_pending_updates=True)
if __name__=='__main__':main()
