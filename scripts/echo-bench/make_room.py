"""Commands over music (V) and alone (Q), through hassmic's gain without (off) and with (grd) its guard.
  make_room.py CLIPS WAKE_DB CASES    CASES: [(command dBFS, music dBFS), ...]; run in a directory holding the .pcm clips
  first, there: gcc -O2 -I$HASSMIC room_gain.c $HASSMIC/micgain.c -lm -o mg    (HASSMIC=echo-dot-assist/src/hassmic)"""
import numpy as np,subprocess,sys,os
R=16000; rng=np.random.default_rng(1)
def ld(n):
    a=np.fromfile(n+'.pcm','<i2').astype(float); t=np.arange(int(len(a)*2/3))*1.5; return np.interp(t,np.arange(len(a)),a)
def act(x):
    f=x[:len(x)//160*160].reshape(-1,160); p=(f**2).mean(1); p=p[p>p.max()*0.003]; return 10*np.log10(p.mean()/32768**2)
def to(x,db): return x*10**((db-act(x))/20)
N=16*R; t=np.arange(N)/R
m=np.zeros(N)
for bar,ch in enumerate([(220,261.6,329.6),(174.6,220,261.6),(196,246.9,293.7),(130.8,164.8,196)]*2):
    s=slice(bar*2*R,(bar+1)*2*R); tt=t[:2*R]
    for f in ch:
        for h in (1,2,3,4): m[s]+=np.sin(2*np.pi*f*h*tt)/h*np.exp(-tt*0.7)
for k in range(32):
    i=int(k*.5*R); L=int(.12*R); e=np.exp(-np.arange(L)/(.03*R))
    m[i:i+L]+=(4*np.sin(2*np.pi*60*np.arange(L)/R) if k%2==0 else rng.standard_normal(L)*2)*e
voc=np.concatenate([ld(x) for x in ('frag_getover','parallel','frag','followup','pi_disk')])
voc=np.interp(np.arange(0,len(voc)-1,1.1),np.arange(len(voc)),voc)
voc=np.resize(voc,N); music=m/m.std()+1.2*voc/voc.std()
def rms(x): return 20*np.log10(np.sqrt((x**2).mean())/32768)
floor=rng.standard_normal(N); floor*=10**((-67-rms(floor))/20)
cmds=sys.argv[1].split(',')

CASES=eval(sys.argv[3])
for kind,mu in (('V',music),('Q',None)):
 for lvl,mus in CASES:
  for c in cmds:
    x=floor+(mu*10**((mus-rms(mu))/20) if mu is not None else 0)
    s=to(ld(c),lvl); x[int(1.5*R):int(1.5*R)+len(s)]+=s
    raw=np.clip(x,-32768,32767).astype('<i2').tobytes()
    for g,name in ((0,'off'),(8,'grd')):
      p=subprocess.run(['./mg',sys.argv[2],str(g)],input=raw,capture_output=True)
      o=np.frombuffer(p.stdout,'<i2').astype(float)
      open(f'r_{kind}k{-int(sys.argv[2])}_{-lvl}_{-mus}_{name}n_{c}.pcm16','wb').write(o.astype('<i2').tobytes())
