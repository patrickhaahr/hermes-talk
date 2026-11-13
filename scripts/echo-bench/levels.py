import array, glob, math, sys
def stats(pcm):
    a = array.array("h"); a.frombytes(pcm[:len(pcm)//2*2])
    frames = [a[i:i+480] for i in range(0, len(a), 480)]
    loud = [f for f in frames if f and max(map(abs, f)) > 500]
    s = [x for f in loud for x in f]
    return math.sqrt(sum(x*x for x in s)/len(s)), max(map(abs, s))
for f in sys.argv[1:]:
    r, p = stats(open(f, "rb").read()); print(f"{f}: speech RMS {r:.0f}  peak {p}")
