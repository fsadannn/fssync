import re
import os
import sys
import json
from stopwords import stopwords
if hasattr(sys, 'frozen'):
    MODULE = os.path.dirname(sys.executable)
else:
    try:
        MODULE = os.path.dirname(os.path.realpath(__file__))
    except:
        MODULE = ""

keep = set(['kun', 'sama'])

tokens = re.compile('[a-zA-Z0-9!ñÑ\']+')
tv = re.compile('[^a-zA-Z0-9ñÑ][tT][vV][^a-zA-Z0-9ñÑ]')
normsp = re.compile('  +')
daysstr = ['lunes', 'martes', 'mi[eé]rcoles', 'jueves', 'viernes', 's[áa]bado', 'domingo',
           'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
days = re.compile('|'.join(daysstr), re.I)
dates = re.compile(
    '[0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4}|[0-9]{2,4}[/-][0-9]{1,2}[/-][0-9]{1,2}')
epi = re.compile('episodios?$|cap[ií]tulos?$|caps?$', re.I)
epin = re.compile('episodios?[0-9]+|cap[ií]tulos?[0-9]+|caps?[0-9]+', re.I)
garbage = re.compile('\{ *\}|\( *\)|\[ *\]')
groupsop = re.compile('|'.join(['\{', '\(', '\[']))
groupscl = re.compile('|'.join(['\}', '\)', '\]']))
clopgp = re.compile('|'.join(['\]\[', '\}\{', '\)\(']))
resolution = re.compile('1080p|720p|480p|1920 *[xX] *1080|1280 *[xX] *720|720 *[xX] *480')
captemp = re.compile('[0-9]{1,4}x?[0-9]{0,4}', re.I)
ordinal = re.compile('1st|2nd|3rd|[1-9][0-9?]th|1ro|2do|3ro|[4-6]to|7mo|8vo|9no')
upperm = re.compile('[A-Z].*[A-Z]')
letn = re.compile('[0-9][a-z]',re.I)


def transform(txt):
    res = []
    for n,i in enumerate(txt.split()):
        if i.lower() in stopwords and n!=0:
            res.append(i.lower())
        elif i == " ":
            continue
        elif len(i) < 3 and letn.search(i):
            res.append(i[0]+i[1].upper())
        elif len(i) < 3 or upperm.search(i):
            res.append(i)
        else:
            res.append(i[0].upper()+i[1:].lower())
    return ' '.join(res).strip()


def clean(txt):
    txt = txt.replace('?','\?')
    txt = txt.replace('+','\+')
    txt = txt.replace('.','\.')
    txt = tv.sub(' ',txt)
    txt = days.sub('', txt)
    txt = dates.sub('', txt)
    txt = resolution.sub('', txt)
    txt = garbage.sub('', txt)
    txt = normsp.sub(' ', txt)
    return txt.strip()


def parse(txt):
    toks = []
    for i in tokens.finditer(txt):
        toks.append(i.group())
    seps = []
    for i in tokens.split(txt):
       if len(i) > 1 and clopgp.match(i.strip()):
           i = i.strip()
           seps.append(i[0])
           toks.insert(len(seps)-1, '')
           seps.append(i[1])
       else:
           seps.append(i)
    if len(seps) == 0:
        return toks, ['']
    if seps[0] == '':
        seps = seps[1:]
    if seps[-1] == '' and len(seps) != len(toks):
        seps = seps[:-1]
    if groupsop.search(seps[0]):
        toks = ['']+toks
    while len(seps) != len(toks):
        if len(seps)<len(toks):
            seps = seps+['']
        else:
            toks = toks+['']
    return toks, seps


def process(toks, seps, data={}, deep=0, nep = True):
    # proc []{}()
    while len(seps) != len(toks):
        seps = seps+['']
    if len(toks) == 1 and deep == 0:
        ff = captemp.search(toks[0])
        if ff:
            data['cap'] = ff.group()
            data['name'] = captemp.sub('', toks[0], 1)
            data['nameep'] = ''
        else:
            data['cap'] = ''
            data['name'] = toks[0]
            data['nameep'] = ''
        return data
    ungrouptoks = []
    ungroupseps = []
    op = 0
    gp = False
    qq = []
    i = 0
    while i < len(seps):
        if not gp and groupsop.search(seps[i]):
            if i != 0 and toks[i] != '':
                ungrouptoks.append(toks[i])
            op = i
            gp = True
        elif gp and groupscl.search(seps[i]):
            jump = i-op
            data = process(toks[op+1:op+jump+1],
                           seps[op+1:op+jump], data, deep+1)
            qq.append((op, i))
            gp = False
        elif not gp:
            ungrouptoks.append(toks[i])
            ungroupseps.append(seps[i])
        i += 1
    while len(ungroupseps) != len(ungrouptoks):
        ungroupseps = ungroupseps+['']
    i = 0
    capflag = not('cap' in data)
    if not('capcandidate' in data):
        data['capcandidate'] = []
    # if deep == 0:
    name = {'toks': [], 'seps': []}
    #print(ungrouptoks, ungroupseps)
    while i < len(ungrouptoks):
        if capflag and epi.search(ungrouptoks[i]):
            if i+1 < len(ungrouptoks):
                ff = re.search('[0-9]{1,4}', ungrouptoks[i+1])
                if ff:
                    data['cap'] = int(ff.group())
                    capflag = False
                    i += 1
        elif capflag and epin.search(ungrouptoks[i]):
            ff = captemp.search(ungrouptoks[i])
            data['cap'] = int(ff.group())
            capflag = False
        elif capflag and captemp.search(ungrouptoks[i]) and not ordinal.search(ungrouptoks[i]):
            data['capcandidate'].append(
                (ungrouptoks[i], int(bool(re.search('[A-Za-z]+', ungrouptoks[i]))) -
                 int(bool(re.search('[0-9]{1,4}[xX][0-9]{1,4}', ungrouptoks[i]))) +
                 1-i/len(ungrouptoks), len(name['toks'])))
            if deep == 0:
                name['toks'].append(ungrouptoks[i])
                name['seps'].append(ungroupseps[i])
        elif deep == 0:
            name['toks'].append(ungrouptoks[i])
            name['seps'].append(ungroupseps[i])
        i += 1
    if deep == 0 and capflag:
        capcandidate = list(sorted(data['capcandidate'], key=lambda x: x[1]))
        if len(capcandidate) == 0:
            data['cap'] = ''
            data['pos'] = -1
        elif len(capcandidate) == 1:
            ff = captemp.search(capcandidate[0][0])
            data['cap'] = ff.group()
            data['pos'] = capcandidate[0][2]
        elif len(list(filter(lambda x: x[1] == 0, capcandidate))) == 1:
            ff = captemp.search(capcandidate[0][0])
            data['cap'] = ff.group()
            data['pos'] = capcandidate[0][2]
        else:
            ff = captemp.search(capcandidate[0][0])
            data['cap'] = ff.group()
            data['pos'] = capcandidate[0][2]
    if deep == 0:
        if not('pos' in data):
            data['pos']=len(name['toks'])+10
        namee = ''
        nameep = ''
        check = False
        for nn, (tok, sep) in enumerate(zip(name['toks'], name['seps'])):
            if nn == data['pos']:
                continue
            if nn < data['pos']:
                if check:
                    if tok in keep:
                        namee += '-'
                    else:
                        namee += ' '
                    check = False
                if sep == '-':
                    check = True
                    namee += tok
                else:
                    check = False
                    namee += tok
                    namee += ' '
            else:
                if check:
                    if tok in keep:
                        nameep += '-'
                    else:
                        nameep += ' '
                    check = False
                if sep == '-':
                    check = True
                    nameep += tok
                else:
                    check = False
                    nameep += tok
                    nameep += ' '
        namee = transform(namee)
        nameep = transform(nameep)
        data['name'] = namee
        data['nameep'] = nameep
    if deep == 0:
        data.pop('capcandidate')
        data.pop('pos')
    return data


def rename_serie(txt):
    cc = clean(txt)
    toks, seps = parse(cc)
    #print(toks,seps)
    res = process(toks, seps, {})
    return res['name'], res['cap'], res['nameep']