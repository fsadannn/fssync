import abc
import os
import sys
import json
import fs
from fs.base import FS
from fs.memoryfs import MemoryFS
from fs.wrap import read_only, cache_directory
from fs.path import join, splitext
from utils import parse_serie_guessit as parse
from utils import rename
from utils import temp_format, subs_formats, temp_gap
from utils import editDistance
from parser_serie import transform

MOVIE = 0
ANIME = 1
PSERIE = 2

class BadClassError(Exception):
    pass

class DSync(metaclass=abc.ABCMeta):
    def __init__(self, source, dest):
        if not issubclass(source.__class__, FS):
            raise BadClassError('source must be direct/indirect subclass of FS')
        if not issubclass(dest.__class__, FS):
            raise BadClassError('dest must be direct/indirect subclass of FS')
        self._source = cache_directory(read_only(source))
        self._dest = dest

    @abc.abstractmethod
    def sync(self):
        raise NotImplementedError

    @abc.abstractmethod
    def organize(self):
        raise NotImplementedError

class Movies(DSync):

    def __init__(self, source, dest):
        super(Movies, self).__init__(source, dest)

    def sync(self, keep_old = True):
        pass

    def organize(self):
        pass

class SeriesAnimes(DSync):

    def __init__(self, source, dest, rename = False):
        super(SeriesAnimes, self).__init__(source, dest)
        self._rename = rename

    def sync(self, keep_old = True):
        pass

    def organize(self):
        """Reorganize the folder, put each chapter of the same serie
        and season in the same folder, including subtitle"""
        # make virtual filesystem in ram with the final
        # organization of the filesystem
        ff = self._dest
        ram = MemoryFS()

        for path, dirs, files in ff.walk():
            posprocsub = []
            fils = set()
            for j in files:
                if splitext(j.name)[1] in subs_formats and path!="/":
                    posprocsub.append(j.name)
                    continue
                pp = rename(j.name)
                try:
                    if pp[3]:
                        fold = transform(pp[0])
                        pth = join('/',fold)
                        if not ram.exists(pth):
                            ram.makedir(fold)
                        fils.add(fold)
                        ram.writetext(join(pth,j.name),join(path,j.name))
                except KeyError:
                    continue

            for j in posprocsub:
                pp = rename(j)
                fold = transform(pp[0])
                pth = join('/',fold)
                if ram.exists(pth):
                    ram.writetext(join(pth,j),join(path,j))
                elif len(fils)==1:
                    pth = join('/',list(fils)[0])
                    ram.writetext(join(pth,j),join(path,j))
                elif len(fils)>1:
                    best = None
                    gap = 3
                    for i in fils:
                        n = editDistance(i,foldd)
                        if n < 3 and n < gap:
                            best=i
                            gap=n
                        elif n == 0:
                            best = i
                            break
                    if best:
                        pth = join('/',best)
                        ram.writetext(join(pth,j),join(path,j))
                    else:
                        if not(ram.exists('/subs')):
                            ram.makedir('/subs')
                        ram.writetext(join('/subs',j),join(path,j))
                else:
                    if not(ram.exists('/subs')):
                        ram.makedir('/subs')
                    ram.writetext(join('/subs',j),join(path,j))

        # execute ram.tree() for see the structure in pretty format
        # reorganize the filesystem from the structure in the
        # virtualfilesistem in ram
        data = set(ram.listdir('/'))
        for fold in data:
            path = join('/',fold)
            if not(ff.exists(path)):
                ff.makedir(path)
            for fil in ram.listdir(path):
                pp = rename(fil)
                if pp[1]:
                    fill = transform(pp[0])+' - '+str(pp[1])+pp[2]
                else:
                    fill = transform(pp[0])+pp[2]
                path2 = join(path, fill)
                opth = ram.readtext(join(path, fil))
                if path2 == opth:
                    continue
                ff.move(opth, path2)

        # clean the filesystem before the reorganization
        for i in ff.scandir('/'):
            if not(i.name in data):
                if i.is_file:
                    ff.remove(join('/',i.name))
                else:
                    ff.removetree(join('/',i.name))


class SeriesPerson(DSync):

    def __init__(self, source, dest):
        super(SeriesPerson, self).__init__(source, dest)

    def organize(self):
        """Reorganize the folder, put each chapter of the same serie
        and season in the same folder, including subtitle"""
        # make virtual filesystem in ram with the final
        # organization of the filesystem
        ff = self._dest
        ram = MemoryFS()

        for path, dirs, files in ff.walk():
            posprocsub = []
            posprocimg = []
            fils = set()
            for j in files:
                if splitext(j.name)[1] in subs_formats and path!="/":
                    posprocsub.append(j.name)
                    continue
                pp = parse(j.name)
                try:
                    if 'video' in pp['mimetype']:
                        fold = transform(pp['title'])
                        if 'season' in pp:
                            fold += ' - '+temp_format(pp['season'])
                            fils.add((fold, 1))
                        else:
                            fils.add((fold, 0))
                        pth = join('/',fold)
                        if not ram.exists(pth):
                            ram.makedir(fold)
                        ram.writetext(join(pth,j.name),join(path,j.name))
                    elif 'image' in pp['mimetype']:
                        posprocimg.append(j.name)
                except KeyError:
                    continue

            for j in posprocsub:
                pp = parse(j)
                foldd = transform(pp['title'])
                if 'season' in pp:
                    fold = foldd + ' - '+temp_format(pp['season'])
                else:
                    fold = foldd
                pth = join('/',fold)
                if ram.exists(pth):
                    ram.writetext(join(pth,j),join(path,j))
                elif len(fils)==1:
                    pth = join('/',list(fils)[0])
                    ram.writetext(join(pth,j),join(path,j))
                elif len(fils)>1:
                    best = None
                    gap = temp_gap+10
                    for i, v in fils:
                        n = editDistance(i,foldd)
                        if n < temp_gap*v + (1-v)*3 and n < gap:
                            best=i
                            gap=n
                        elif n == 0:
                            best = i
                            break
                    if best:
                        pth = join('/',best)
                        ram.writetext(join(pth,j),join(path,j))
                    else:
                        if not(ram.exists('/subs')):
                            ram.makedir('/subs')
                        ram.writetext(join('/subs',j),join(path,j))
                else:
                    if not(ram.exists('/subs')):
                        ram.makedir('/subs')
                    ram.writetext(join('/subs',j),join(path,j))

            for j in posprocimg:
                pp = parse(j)
                foldd = transform(pp['title'])
                if 'season' in pp:
                    fold = foldd + ' - '+temp_format(pp['season'])
                else:
                    fold = foldd
                pth = join('/',fold)
                if ram.exists(pth):
                    ram.writetext(join(pth,j),join(path,j))
                elif len(fils)==1:
                    pth = join('/',list(fils)[0])
                    ram.writetext(join(pth,j),join(path,j))
                elif len(fils)>1:
                    best = None
                    gap = temp_gap+10
                    for i, v in fils:
                        n = editDistance(i,foldd)
                        if n < temp_gap*v + (1-v)*2 and n<gap:
                            best=i
                            gap=n
                        elif n == 0:
                            best = i
                            break
                    if best:
                        pth = join('/',best)
                        ram.writetext(join(pth,j),join(path,j))

        # execute ram.tree() for see the structure in pretty format
        # reorganize the filesystem from the structure in the
        # virtualfilesistem in ram
        data = set(ram.listdir('/'))
        for fold in data:
            path = join('/',fold)
            if not(ff.exists(path)):
                ff.makedir(path)
            for fil in ram.listdir(path):
                path2 = join(path, fil)
                opth = ram.readtext(path2)
                if path2 == opth:
                    continue
                ff.move(opth, path2)

        # clean the filesystem before the reorganization
        for i in ff.scandir('/'):
            if not(i.name in data):
                if i.is_file:
                    ff.remove(join('/',i.name))
                else:
                    ff.removetree(join('/',i.name))

    def sync(self, keep_old = True):
        pass

def organize(path, typee = PSERIE):
    ff = fs.open_fs(path)
    if typee == PSERIE:
        tt = SeriesPerson(MemoryFS(),ff)
    elif typee = ANIME:
        tt = SeriesAnimes(MemoryFS(),ff)
    else:
        tt = Movies(MemoryFS(),ff)
    tt.organize()
