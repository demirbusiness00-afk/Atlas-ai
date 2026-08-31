import os,time,logging,asyncio
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor,as_completed
import requests
from telegram import Update
from telegram.ext import Application,CommandHandler,ContextTypes

logging.basicConfig(level=logging.INFO,format="%(asctime)s | %(levelname)s | %(message)s")
log=logging.getLogger("atlas")
TOKEN=os.getenv("BOT_TOKEN","").strip()
SIGNAL_CHAT=os.getenv("SIGNAL_CHAT_ID","").strip()
ALLOWED=os.getenv("ALLOWED_CHAT_ID","").strip()
INTERVAL=int(os.getenv("SCAN_INTERVAL","120"))
UNIVERSE_SIZE=int(os.getenv("UNIVERSE_SIZE","80"))
THRESHOLD=int(os.getenv("RADAR_THRESHOLD","80"))
COOLDOWN=int(os.getenv("SIGNAL_COOLDOWN","3600"))
BASE="https://api.binance.com"; S=requests.Session()
S.headers["User-Agent"]="AtlasAI-V6/1.0"
state={"scans":0,"last":0,"universe":0,"scanned":0,"ready":0,"signals":0,"errors":0,"last_error":"","top":[]}
sent=defaultdict(float)

def ok(u): return not ALLOWED or str(u.effective_chat.id)==ALLOWED
def api(path,params=None):
    r=S.get(BASE+path,params=params,timeout=12); r.raise_for_status(); return r.json()
def ema(a,n):
    if len(a)<n:return None
    x=sum(a[:n])/n;k=2/(n+1)
    for v in a[n:]:x=v*k+x*(1-k)
    return x
def rsi(a,n=14):
    if len(a)<=n:return 50
    g=l=0
    for x,y in zip(a[-n-1:-1],a[-n:]):
        d=y-x;g+=max(d,0);l+=max(-d,0)
    return 100 if l==0 else 100-100/(1+g/l)
def atr(c,n=14):
    if len(c)<n+1:return 0
    tr=[]
    for i in range(1,len(c)):
        tr.append(max(c[i]["h"]-c[i]["l"],abs(c[i]["h"]-c[i-1]["c"]),abs(c[i]["l"]-c[i-1]["c"])))
    return sum(tr[-n:])/n
def candles(sym,iv,limit=120):
    d=api("/api/v3/klines",{"symbol":sym,"interval":iv,"limit":limit})
    return [{"t":x[0],"o":float(x[1]),"h":float(x[2]),"l":float(x[3]),"c":float(x[4]),"v":float(x[5])} for x in d]
def two_min(m):
    out=[]
    for i in range(0,len(m)-1,2):
        a,b=m[i],m[i+1]
        if b["t"]!=a["t"]+60000:continue
        out.append({"t":a["t"],"o":a["o"],"h":max(a["h"],b["h"]),"l":min(a["l"],b["l"]),"c":b["c"],"v":a["v"]+b["v"]})
    return out
def trend(c):
    a=[x["c"] for x in c]; p=a[-1];e20=ema(a,20);e50=ema(a,50);q=rsi(a);m=(p/a[-21]-1)*100
    z=(1 if p>e20 else -1)+(1 if e20>e50 else -1)+(.7 if q>=55 else -.7 if q<=45 else 0)+(.3 if m>0 else -.3 if m<0 else 0)
    return ("BULLISH" if z>=1 else "BEARISH" if z<=-1 else "NEUTRAL"),z
def sr(c):
    c=c[-80:];p=c[-1]["c"];sup=[];res=[]
    for i in range(2,len(c)-2):
        if c[i]["h"]>=max(c[i-2]["h"],c[i-1]["h"],c[i+1]["h"],c[i+2]["h"]):res.append(c[i]["h"])
        if c[i]["l"]<=min(c[i-2]["l"],c[i-1]["l"],c[i+1]["l"],c[i+2]["l"]):sup.append(c[i]["l"])
    s=max([x for x in sup if x<=p],default=min(x["l"] for x in c))
    r=min([x for x in res if x>=p],default=max(x["h"] for x in c))
    ap=atr(c)/p*100
    near=max(.2,min(1.5,ap*.8))
    return s,r,abs(p-s)/p*100<=near,abs(r-p)/p*100<=near
def analyze(sym):
    try:
        d4=[candles(sym,"1d"),candles(sym,"4h"),candles(sym,"1h"),candles(sym,"15m")]
        ts=[trend(x)[0] for x in d4]; m2=two_min(candles(sym,"1m"))
        t2=trend(m2)[0]; a=[x["c"] for x in m2];p=a[-1];e9=ema(a,9);e20=ema(a,20);q=rsi(a)
        support,resistance,ns,nr=sr(d4[3]); bull=ts.count("BULLISH");bear=ts.count("BEARISH")
        L=sum(w for t,w in zip(ts,(28,24,20,14)) if t=="BULLISH");H=sum(w for t,w in zip(ts,(28,24,20,14)) if t=="BEARISH")
        if t2=="BULLISH":L+=7
        if t2=="BEARISH":H+=7
        if bull>=3:L+=7
        if bear>=3:H+=7
        direction="LONG" if L>=H else "SHORT";score=min(100,max(L,H))
        trig=(t2=="BULLISH" and e9>e20 and q>=52 and (ns or nr)) if direction=="LONG" else (t2=="BEARISH" and e9<e20 and q<=48 and (ns or nr))
        ready=score>=THRESHOLD and trig and (bull>=3 or bear>=3)
        a15=atr(d4[3]); entry=p
        if direction=="LONG":
            stop=min(support,entry-1.2*a15);risk=max(entry-stop,entry*.002);tp=[entry+1.2*risk,entry+2*risk,entry+3*risk]
        else:
            stop=max(resistance,entry+1.2*a15);risk=max(stop-entry,entry*.002);tp=[entry-1.2*risk,entry-2*risk,entry-3*risk]
        return dict(symbol=sym,price=p,direction=direction,score=int(score),ready=ready,mtf=ts+[t2],rsi=q,support=support,resistance=resistance,entry=entry,stop=stop,tp=tp)
    except Exception as e:
        state["errors"]+=1;state["last_error"]=f"{sym}: {e}";return None
def universe():
    info=api("/api/v3/exchangeInfo");allowed={x["symbol"] for x in info["symbols"] if x["status"]=="TRADING" and x["quoteAsset"]=="USDT" and x.get("isSpotTradingAllowed",False)}
    t=api("/api/v3/ticker/24hr");a=[]
    for x in t:
        s=x["symbol"]
        if s in allowed and not any(s.endswith(v) for v in ("UPUSDT","DOWNUSDT","BULLUSDT","BEARUSDT")):
            try:a.append((float(x["quoteVolume"]),s))
            except:pass
    return [s for _,s in sorted(a,reverse=True)[:UNIVERSE_SIZE]]
def scan():
    state["scans"]+=1;state["last"]=time.time();u=universe();state["universe"]=len(u);r=[]
    with ThreadPoolExecutor(max_workers=8) as ex:
        for f in as_completed([ex.submit(analyze,s) for s in u]):
            x=f.result()
            if x:r.append(x)
    r.sort(key=lambda x:x["score"],reverse=True);state["scanned"]=len(r);state["ready"]=sum(x["ready"] for x in r);state["top"]=r[:10]
    out=[]
    for x in r:
        if x["ready"] and time.time()-sent[x["symbol"]]>=COOLDOWN:
            sent[x["symbol"]]=time.time();out.append(x)
    state["signals"]+=len(out);return out
def fp(x):
    return f"{x:,.2f}" if x>=1000 else f"{x:,.4f}" if x>=1 else f"{x:,.6f}" if x>=.01 else f"{x:.8f}"
def msg(x):
    return f"""🚨 <b>ATLAS AI V6 — {x["direction"]}</b>
#{x["symbol"]} | Score: <b>{x["score"]}/100</b>

💰 Giriş: <code>{fp(x["entry"])}</code>
🛑 Stop: <code>{fp(x["stop"])}</code>
🎯 TP1: <code>{fp(x["tp"][0])}</code>
🎯 TP2: <code>{fp(x["tp"][1])}</code>
🎯 TP3: <code>{fp(x["tp"][2])}</code>

🧭 1D {x["mtf"][0]} → 4H {x["mtf"][1]} → 1H {x["mtf"][2]} → 15M {x["mtf"][3]}
⚡ 2M {x["mtf"][4]} | RSI {x["rsi"]:.1f}
📍 S {fp(x["support"])} | R {fp(x["resistance"])}

⚠️ Otomatik teknik analiz uyarısıdır; garanti kâr/satış tavsiyesi değildir."""
async def start(u,c):
    if ok(u):await u.message.reply_text("🧭 <b>ATLAS AI V6</b>\n\n1D → 4H → 1H → 15M = yön\nS/R = teyit\n2M = giriş zamanlaması\n\n/status /diagnostics /top /test",parse_mode="HTML")
async def status(u,c):
    if ok(u):await u.message.reply_text(f"🧭 <b>ATLAS AI V6 STATUS</b>\n\nTelegram: ✅\nBinance REST: ✅\nMTF: 1D → 4H → 1H → 15M\nS/R: ACTIVE\n2M: giriş zamanlaması\nTarama: {INTERVAL}s\nUniverse: top {UNIVERSE_SIZE} USDT spot",parse_mode="HTML")
async def diagnostics(u,c):
    if ok(u):
        age="yok" if not state["last"] else f"{int(time.time()-state['last'])}s önce"
        await u.message.reply_text(f"🔧 <b>ATLAS AI V6 DIAGNOSTICS</b>\n\nTarama: {state['scans']}\nSon tarama: {age}\nUniverse: {state['universe']}\nTaranan: {state['scanned']}\nTRADE READY: {state['ready']}\nSinyal: {state['signals']}\nHata: {state['errors']}\nSon hata: {state['last_error'] or 'yok'}",parse_mode="HTML")
async def top(u,c):
    if ok(u):
        text="🏆 <b>TOP RADAR</b>\n\n"+"\n".join(f"{i}. {x['symbol']} — {x['direction']} {x['score']}/100 | 1D {x['mtf'][0]} 4H {x['mtf'][1]} 1H {x['mtf'][2]} 15M {x['mtf'][3]} 2M {x['mtf'][4]}" for i,x in enumerate(state["top"],1))
        await u.message.reply_text(text or "Henüz tarama yok.",parse_mode="HTML")
async def test(u,c):
    if ok(u):
        try:
            k=candles("BTCUSDT","1m",3);await u.message.reply_text(f"🧪 <b>TEST OK</b>\nBinance: ✅\nBTCUSDT 1m: {len(k)}\n2m aggregation: {'✅' if len(two_min(k)) else '⚠️'}",parse_mode="HTML")
        except Exception as e:await u.message.reply_text(f"❌ TEST ERROR\n{e}")
async def worker(app):
    while True:
        t=time.time()
        try:
            for x in scan():
                if SIGNAL_CHAT: await app.bot.send_message(SIGNAL_CHAT,msg(x),parse_mode="HTML")
        except Exception as e:
            state["errors"]+=1;state["last_error"]=str(e);log.exception("scan")
        await asyncio.sleep(max(5,INTERVAL-(time.time()-t)))
def main():
    if not TOKEN:raise RuntimeError("BOT_TOKEN is required")
    app=Application.builder().token(TOKEN).build()
    for name,fn in [("start",start),("status",status),("diagnostics",diagnostics),("top",top),("test",test)]:app.add_handler(CommandHandler(name,fn))
    async def init(a):a.create_task(worker(a))
    app.post_init=init;app.run_polling(drop_pending_updates=True)
if __name__=="__main__":main()
