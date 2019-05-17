import abc
import os
import sys
import json
import hashlib
import fs
from fs.base import FS
from fs.memoryfs import MemoryFS
from fs.wrap import read_only, cache_directory
from fs.path import join, splitext
from fs._bulk import Copier
from fs.errors import BulkCopyFailed, DirectoryExpected
from fs.tools import is_thread_safe
from .utils import parse_serie_guessit as parse
from .utils import rename
from .utils import temp_format, subs_formats, temp_gap
from .utils import editDistance
from .parser_serie import transform

MOVIE = 0
ANIME = 1
PSERIE = 2

BLOCKSIZE = 65536

RENAME = 0
OVERWRITE = 1


def hash_file(fsi, filename, algorithm='sha1'):
    """
    Basic hash for a file
    :param filename: file path
    :param algorithm: see hashlib.algorithms_available
    :return: hex hash
    """
    hasher = getattr(hashlib, algorithm)()
    with fsi.openbin(filename, 'rb') as afile:
        buf = afile.read(BLOCKSIZE)
        while buf:
            hasher.update(buf)
            buf = afile.read(BLOCKSIZE)
        afile.close()
    return hasher.hexdigest()


def hash_files(fsi1, filename1, fsi2, filename2, algorithm='sha1'):
    """
    Basic hash for a file
    :param filename: file path
    :param algorithm: see hashlib.algorithms_available
    :return: hex hash
    """
    hasher1 = getattr(hashlib, algorithm)()
    hasher2 = getattr(hashlib, algorithm)()
    with fsi1.openbin(filename1, 'rb') as afile1:
        with fsi2.openbin(filename2, 'rb') as afile2:
            buf1 = afile1.read(BLOCKSIZE)
            buf2 = afile2.read(BLOCKSIZE)
            while buf1 and buf2:
                hasher1.update(buf1)
                hasher2.update(buf2)
                if hasher1.digest() != hasher2.digest():
                    return False
                buf1 = afile1.read(BLOCKSIZE)
                buf2 = afile2.read(BLOCKSIZE)
            afile1.close()
            afile2.close()
    return True


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
    def sync(self, workers, use_hash, collition):
        raise NotImplementedError

    @abc.abstractmethod
    def organize(self):
        raise NotImplementedError

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._source.close()
        self._dest.close()


class Movies(DSync):

    def __init__(self, source, dest):
        super(Movies, self).__init__(source, dest)

    def sync(self, workers=1, use_hash=True, collition=OVERWRITE):
        assert workers >= 0

    def organize(self):
        pass


class SeriesAnimes(DSync):

    def __init__(self, source, dest, rename=False):
        super(SeriesAnimes, self).__init__(source, dest)
        self._rename = rename

    def _make_temp_fs(self, ff):
        # make virtual filesystem in ram with the final
        # organization of the filesystem
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
                    if pp.is_video:
                        fold = transform(pp.title)
                        pth = join('/',fold)
                        if not ram.exists(pth):
                            ram.makedir(fold)
                        fils.add(fold)
                        ram.writetext(join(pth,j.name),join(path,j.name))
                except KeyError:
                    continue

            for j in posprocsub:
                pp = rename(j)
                fold = transform(pp.title)
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
        return ram

    def organize(self):
        """Reorganize the folder, put each chapter of the same serie
        and season in the same folder, including subtitle"""
        ff = self._dest
        ram = self._make_temp_fs(ff)

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
                if pp.episode:
                    fill = transform(pp.title)+' - '+str(pp.episode)
                else:
                    fill = transform(pp.title)
                if pp.episode_title:
                    fill = fill+' - '+str(pp.episode_title)
                fill += pp.ext
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

    def sync(self, workers=1, use_hash=True, collition=OVERWRITE):
        assert workers >= 0
        sc = self._source
        ram = self._make_temp_fs(sc)
        ff = self._dest
        # execute ram.tree() for see the structure in pretty format
        # reorganize the filesystem from the structure in the
        # virtualfilesistem in ram
        try:
            data = set(ram.listdir('/'))
            with sc.lock(), ff.lock():
                _thread_safe = is_thread_safe(sc, ff)
                with Copier(num_workers=workers if _thread_safe else 0) as copier:
                    # iterate over the virtual structure( only folders un the 1st level)
                    for fold in data:
                        path = join('/', fold)
                        if not(ff.exists(path)):
                            ff.makedir(path)
                        # iterate over files in each folder
                        try:
                            lsd = ram.listdir(path)
                        except DirectoryExpected:
                            copier.copy(sc, path, ff, path)
                        for fil in lsd:
                            pp = rename(fil)
                            if pp.episode:
                                fill = transform(pp.title)+' - '+str(pp.episode)
                                fillt = fill
                            else:
                                fill = transform(pp.title)
                                fillt = fill
                            if pp.episode_title:
                                fill = fill + ' - ' + str(pp.episode_title)
                            fill += pp.ext
                            path2 = join(path, fill)
                            opth = ram.readtext(join(path, fil))
                            # if exist the file with the exactly transform name
                            if ff.exists(path2):
                                i1 = sc.getinfo(opth, namespaces=['details'])
                                i2 = ff.getinfo(path2, namespaces=['details'])
                                if i1.size < i2.size:
                                    ## if the size of new is less than older du nothing
                                    continue
                                elif use_hash and i1.size == i2.size:
                                    ## if has the same size and hash is avaliable compare the hash
                                    # h1 = hash_file(sc, opth)
                                    # h2 = hash_file(ff, path2)
                                    if hash_files(sc, opth, ff, path2):
                                        ## if the has coincide are the same file 99.9%
                                        continue
                                    # if size are equal but hash are different we have a collition
                                    if collition == OVERWRITE:
                                        copier.copy(sc, opth, ff, path2)
                                    if collition == RENAME:
                                        nn, ext = splitext(path2)
                                        num = 2
                                        temppth = nn+'_rename_'+str(num)+ext
                                        while ff.exists(temppth):
                                            num += 1
                                            temppth = nn+'_rename_'+str(num)+ext
                                        ff.move(path2, temppth)
                                        copier.copy(sc, opth, ff, path2)
                                elif not use_hash and i1.size == i2.size:
                                    # if size are equal but don use hash we have a collition
                                    if collition == OVERWRITE:
                                        copier.copy(sc, opth, ff, path2)
                                    if collition == RENAME:
                                        nn, ext = splitext(path2)
                                        num = 2
                                        temppth = nn+'_rename_'+str(num)+ext
                                        while ff.exists(temppth):
                                            num += 1
                                            temppth = nn+'_rename_'+str(num)+ext
                                        ff.move(path2, temppth)
                                        copier.copy(sc, opth, ff, path2)
                                else:
                                    copier.copy(sc, opth, ff, path2)
                            elif pp.ext in subs_formats:
                                copier.copy(sc, opth, ff, path2)
                            # if we have the chapter number
                            elif pp.episode:
                                try:
                                    if 'x' in str(pp.episode):
                                        my = int(str(pp.episode).split('x')[1])
                                    elif 'X' in str(pp.episode):
                                        my = int(str(pp.episode).split('X')[1])
                                    else:
                                        my = int(str(pp.episode))
                                except:
                                    copier.copy(sc, opth, ff, path2)
                                    continue
                                name = ''
                                for filee in ff.listdir(path):
                                    pp2 = rename(filee)
                                    if editDistance(pp2.title, fillt) < 3:
                                        try:
                                            if 'x' in str(pp2.episode):
                                                my2 = int(str(pp2.episode).split('x')[1])
                                            elif 'X' in str(pp2.episode):
                                                my2 = int(str(pp2.episode).split('X')[1])
                                            else:
                                                my2 = int(str(pp2.episode))
                                        except:
                                            continue
                                        if my2 == my:
                                            name = filee
                                            break
                                # if we found a file with similar name and same chapter
                                if name:
                                    i1 = sc.getinfo(opth, namespaces=['details'])
                                    i2 = ff.getinfo(join(path, name), namespaces=['details'])
                                    if i1.size < i2.size:
                                        ## if the size of new is less than older du nothing
                                        continue
                                    elif use_hash and i1.size == i2.size:
                                        ## if has the same size and hash is avaliable compare the hash
                                        temppth = join(path, name)
                                        # h1 = hash_file(sc, opth)
                                        # h2 = hash_file(ff, temppth)
                                        if hash_files(sc, opth, ff, temppth):
                                            continue
                                        # if size are equal but hash are different we have a collition
                                        if collition == OVERWRITE:
                                            copier.copy(sc, opth, ff, temppth)
                                        if collition == RENAME:
                                            nn, ext = splitext(temppth)
                                            num = 2
                                            temppth2 = nn+'_rename_'+str(num)+ext
                                            while ff.exists(temppth2):
                                                num += 1
                                                temppth2 = nn+'_rename_'+str(num)+ext
                                            ff.move(temppth, temppth2)
                                            copier.copy(sc, opth, ff, temppth)
                                    elif not use_hash and i1.size == i2.size:
                                        # if size are equal but don use hash we have a collition
                                        temppth = join(path, name)
                                        if collition == OVERWRITE:
                                            copier.copy(sc, opth, ff, temppth)
                                        if collition == RENAME:
                                            nn, ext = splitext(temppth)
                                            num = 2
                                            temppth2 = nn+'_rename_'+str(num)+ext
                                            while ff.exists(temppth2):
                                                num += 1
                                                temppth2 = nn+'_rename_'+str(num)+ext
                                            ff.move(temppth, temppth2)
                                            copier.copy(sc, opth, ff, temppth)
                                    else:
                                        temppth = join(path, name)
                                        copier.copy(sc, opth, ff, temppth)
                                else:
                                    copier.copy(sc, opth, ff, path2)

        except BulkCopyFailed as e:
            raise BulkCopyFailed(e.errors) ## do somthing with error late, for now just raise again


class SeriesPerson(DSync):

    def __init__(self, source, dest):
        super(SeriesPerson, self).__init__(source, dest)

    def _make_temp_fs(self, ff):
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
                        if pp.season:
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
                if pp.season:
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
                if pp.season:
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
        return ram

    def organize(self):
        """Reorganize the folder, put each chapter of the same serie
        and season in the same folder, including subtitle"""
        # make virtual filesystem in ram with the final
        # organization of the filesystem
        ff = self._dest
        ram = self._make_temp_fs(ff)

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

    def sync(self, workers=1, use_hash=True, collition=OVERWRITE):
        assert workers >= 0
        sc = self._source
        ram = self._make_temp_fs(sc)
        ff = self._dest
        # execute ram.tree() for see the structure in pretty format
        # reorganize the filesystem from the structure in the
        # virtualfilesistem in ram
        try:
            data = set(ram.listdir('/'))
            with sc.lock(), ff.lock():
                _thread_safe = is_thread_safe(sc, ff)
                with Copier(num_workers=workers if _thread_safe else 0) as copier:
                    # iterate over the virtual structure( only folders un the 1st level)
                    for fold in data:
                        path = join('/', fold)
                        if not(ff.exists(path)):
                            ff.makedir(path)
                        # iterate over files in each folder
                        try:
                            lsd = ram.listdir(path)
                        except DirectoryExpected:
                            copier.copy(sc, path, ff, path)
                        for fil in lsd:
                            pp = parse(fil)
                            path2 = join(path, fil)
                            opth = ram.readtext(join(path, fil))
                            # if exist the file with the exactly transform name
                            if ff.exists(path2):
                                i1 = sc.getinfo(opth, namespaces=['details'])
                                i2 = ff.getinfo(path2, namespaces=['details'])
                                if i1.size < i2.size:
                                    ## if the size of new is less than older du nothing
                                    continue
                                elif use_hash and i1.size == i2.size:
                                    ## if has the same size and hash is avaliable compare the hash
                                    # h1 = hash_file(sc, opth)
                                    # h2 = hash_file(ff, path2)
                                    if hash_files(sc, opth, ff, path2):
                                        ## if the has coincide are the same file 99.9%
                                        continue
                                    # if size are equal but hash are different we have a collition
                                    if collition == OVERWRITE:
                                        copier.copy(sc, opth, ff, path2)
                                    if collition == RENAME:
                                        nn, ext = splitext(path2)
                                        num = 2
                                        temppth = nn+'_rename_'+str(num)+ext
                                        while ff.exists(temppth):
                                            num += 1
                                            temppth = nn+'_rename_'+str(num)+ext
                                        ff.move(path2, temppth)
                                        copier.copy(sc, opth, ff, path2)
                                elif not use_hash and i1.size == i2.size:
                                    # if size are equal but don use hash we have a collition
                                    if collition == OVERWRITE:
                                        copier.copy(sc, opth, ff, path2)
                                    if collition == RENAME:
                                        nn, ext = splitext(path2)
                                        num = 2
                                        temppth = nn+'_rename_'+str(num)+ext
                                        while ff.exists(temppth):
                                            num += 1
                                            temppth = nn+'_rename_'+str(num)+ext
                                        ff.move(path2, temppth)
                                        copier.copy(sc, opth, ff, path2)
                                else:
                                    copier.copy(sc, opth, ff, path2)
                            elif pp.ext in subs_formats:
                                copier.copy(sc, opth, ff, path2)
                            elif 'image' in pp['mimetype']:
                                copier.copy(sc, opth, ff, path2)
                            # if we have the chapter number
                            elif pp.episode:
                                try:
                                    my = int(str(pp.episode))
                                except:
                                    copier.copy(sc, opth, ff, path2)
                                    continue
                                name = ''
                                fillt = pp.title
                                for filee in ff.listdir(path):
                                    pp2 = parse(filee)
                                    if editDistance(pp2.title, fillt) < 3:
                                        try:
                                            my2 = int(str(pp2.episode))
                                        except:
                                            continue
                                        if my2 == my:
                                            name = filee
                                            break
                                # if we found a file with similar name and same chapter
                                if name:
                                    i1 = sc.getinfo(opth, namespaces=['details'])
                                    i2 = ff.getinfo(join(path, name), namespaces=['details'])
                                    if i1.size < i2.size:
                                        ## if the size of new is less than older du nothing
                                        continue
                                    elif use_hash and i1.size == i2.size:
                                        ## if has the same size and hash is avaliable compare the hash
                                        temppth = join(path, name)
                                        # h1 = hash_file(sc, opth)
                                        # h2 = hash_file(ff, temppth)
                                        if hash_files(sc, opth, ff, temppth):
                                            continue
                                        # if size are equal but hash are different we have a collition
                                        if collition == OVERWRITE:
                                            copier.copy(sc, opth, ff, temppth)
                                        if collition == RENAME:
                                            nn, ext = splitext(temppth)
                                            num = 2
                                            temppth2 = nn+'_rename_'+str(num)+ext
                                            while ff.exists(temppth2):
                                                num += 1
                                                temppth2 = nn+'_rename_'+str(num)+ext
                                            ff.move(temppth, temppth2)
                                            copier.copy(sc, opth, ff, temppth)
                                    elif not use_hash and i1.size == i2.size:
                                        # if size are equal but don use hash we have a collition
                                        temppth = join(path, name)
                                        if collition == OVERWRITE:
                                            copier.copy(sc, opth, ff, temppth)
                                        if collition == RENAME:
                                            nn, ext = splitext(temppth)
                                            num = 2
                                            temppth2 = nn+'_rename_'+str(num)+ext
                                            while ff.exists(temppth2):
                                                num += 1
                                                temppth2 = nn+'_rename_'+str(num)+ext
                                            ff.move(temppth, temppth2)
                                            copier.copy(sc, opth, ff, temppth)
                                    else:
                                        temppth = join(path, name)
                                        copier.copy(sc, opth, ff, temppth)
                                else:
                                    copier.copy(sc, opth, ff, path2)

        except BulkCopyFailed as e:
            raise BulkCopyFailed(e.errors) ## do somthing with error late, for now just raise again


def organize(path, typee = PSERIE):
    ff = fs.open_fs(path)
    if typee == PSERIE:
        with SeriesPerson(MemoryFS(), ff) as tt:
            tt.organize()
    elif typee == ANIME:
        with SeriesAnimes(MemoryFS(), ff) as tt:
            tt.organize()
    else:
        with Movies(MemoryFS(), ff) as tt:
            tt.organize()

def sync(sc_path, dest_path, typee = ANIME, workers=1, use_hash=False, collition=OVERWRITE):
    assert workers >= 0
    ff2 = fs.open_fs(sc_path)
    ff = fs.open_fs(dest_path)
    if typee == PSERIE:
        with SeriesPerson(ff2, ff) as tt:
            tt.sync(workers, use_hash, collition)
    elif typee == ANIME:
        with SeriesAnimes(ff2, ff) as tt:
            tt.sync(workers, use_hash, collition)
    else:
        with Movies(ff2, ff) as tt:
            tt.sync(workers, use_hash, collition)