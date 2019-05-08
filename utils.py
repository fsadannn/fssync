import re
import os
import sys
import json
from guessit import guessit
from parser_serie import rename_serie

if hasattr(sys, 'frozen'):
    MODULE = os.path.dirname(sys.executable)
else:
    try:
        MODULE = os.path.dirname(os.path.realpath(__file__))
    except:
        MODULE = ""


class CapData:
    def __init__(self, name, nameep, num, ext, informats, err):
        self._things = {'title': name, 'episode_title': nameep, 'episode': num,
                        'ext': ext, 'is_video': informats, 'error': err}
        self._map = {0: name, 1: num, 2: ext, 3: informats, 4: nameep, 5: err}

    def __getattr__(self, name):
        classname = self.__class__.__name__
        if name in self._things:
            return self._things[name]
        raise AttributeError("\'{classname}\' object has no attribute \'{name}\'".format(**locals()))

    def __getitem__(self, item):
        if item in self._things:
            return self._things[item]
        if item in self._map:
            return self._map[item]
        raise KeyError(str(self.__class__.__name__)+" don\'t have key "+str(item))

    def __str__(self):
        return str(self._things)

subs_formats = set([".srt",".idx",".sub",".ssa",".ass"])
video_formats = set([".3g2",
                ".3gp",
                ".3gp2",
                ".asf",
                ".avi",
                ".divx",
                ".flv",
                ".mk3d",
                ".m4v",
                ".mk2",
                ".mka",
                ".mkv",
                ".mov",
                ".mp4",
                ".mp4a",
                ".mpeg",
                ".mpg",
                ".ogg",
                ".ogm",
                ".ogv",
                ".ra",
                ".ram",
                ".rm",
                ".ts",
                ".wav",
                ".webm",
                ".wma",
                ".wmv",
                ".vob"])


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


def rename(name):
    err = False
    txt, ext = os.path.splitext(name)
    try:
        t1, t2, t3 = rename_serie(txt)
    except (ValueError, IndexError):
        try:
            rr = parse_serie_guessit(name)
            ep = ''
            if 'episode' in rr:
                ep = rr['episode']
            ept = ''
            if 'episode_title' in rr:
                ept = rr['episode_title']
            return CapData(rr['title'], ept, ep, ext, bool(ext in video_formats), err)
        except:
            return CapData(txt, '', '', ext, bool(ext in video_formats), True)
    data = {''}
    return CapData(t1, t3, t2, ext, bool(ext in video_formats), err)


def temp_format(ss):
    return '[Temp '+str(ss)+']'

temp_gap = len(temp_format('10'))