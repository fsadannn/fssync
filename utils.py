import re
import os
import sys
import json
from parser_serie import rename_serie, transform

if hasattr(sys, 'frozen'):
    MODULE = os.path.dirname(sys.executable)
else:
    try:
        MODULE = os.path.dirname(os.path.realpath(__file__))
    except:
        MODULE = ""

def editDistance(a, b, transf=True):
        """Distancia de Leventein entre dos cadenas de texto.
            a,b son string
            devuelve un int
        """
        if transf:
            a = transform(a)
            b = transform(b)
        else:
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


eb = re.compile('\{.+\}|\(.+\)|\[.+\]')
epi = re.compile('[Ee]pisodio|[Cc]ap[ií]tulo')
split = re.compile('([0-9]+[xX]?[0-9]*) *-? *')
normsp = re.compile('  +')
endesp = re.compile(' +$')
begesp = re.compile('^ +')
formatt = re.compile(' *- *([0-9]+)')


def rename(name, isserie=True):
    err = False
    txt, ext = os.path.splitext(name)
    if isserie:
        try:
            t1, t2 = rename_serie(txt)
        except (ValueError, IndexError):
            return '', '', '', True
        return t1, t2, ext, err
    return transform(txt), '', ext, err
