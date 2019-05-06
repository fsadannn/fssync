import re
import os
import sys
import json
from guessit import guessit

if hasattr(sys, 'frozen'):
    MODULE = os.path.dirname(sys.executable)
else:
    try:
        MODULE = os.path.dirname(os.path.realpath(__file__))
    except:
        MODULE = ""

subs_formats = set([".srt",".idx",".sub",".ssa",".ass"])

def editDistance(a, b, lower=False):
        """Distancia de Leventein entre dos cadenas de texto.
            a,b son string
            devuelve un int
        """
        if lower:
            a = a.lower()
            b = b.lower()
        m = []
        m.append([i for i in range(len(a)+1)])
        for i in range(len(b)):
            m.append([i+1]+[0 for i in range(len(a))])
        for i in range(1, len(b)+1):
            for j in range(1, len(a)+1):
                if a[j-1] == b[i-1]:
                    m[i][j] = m[i-1][j-1]
                else:
                    m[i][j] = min(
                        m[i-1][j-1]+1, min(m[i][j-1]+1, m[i-1][j]+1))
        ret = m[len(b)][len(a)]
        return ret


def parse_serie_guessit(title, params=None):
    if not params:
        params = '--json --no-default-config -E -t episode -c \"'+os.path.join(MODULE,'options.json\"')
    a = guessit(title, params)
    return a

def temp_format(ss):
    return '[Temp '+str(ss)+']'

temp_gap = len(temp_format('10'))