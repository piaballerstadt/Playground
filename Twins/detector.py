#!/usr/bin/env python
# -*- coding: utf-8 -*-# @formatter:off
#
#                                             ,,
#                                             db
#     \
#     _\,,          `7MM  `7MM  `7MMpMMMb.  `7MM  ,p6"bo   ,pW"Wq.`7Mb,od8 `7MMpMMMb.
#    "-=\~     _      MM    MM    MM    MM    MM 6M'  OO  6W'   `Wb MM' "'   MM    MM
#       \\~___( ~     MM    MM    MM    MM    MM 8M       8M     M8 MM       MM    MM
#      _|/---\\_      MM    MM    MM    MM    MM 8M       8M     M8 MM       MM    MM
#     \        \      MM    MM    MM    MM    MM YM.    , YA.   ,A9 MM       MM    MM
#                     `Mbod"YML..JMML  JMML..JMML.YMbmd'   `Ybmd9'.JMML.   .JMML  JMML.
#
#
#                           written with <3 by Pia Ballerstadt using PyCharm
#                           https://github.com/piaballerstadt
#
#                       Licensed under the Apache License, Version 2.0 (the "License");
#                       you may not use this file except in compliance with the License.
#                       You may obtain a copy of the License at
#
#                           http://www.apache.org/licenses/LICENSE-2.0
#
#                       Unless required by applicable law or agreed to in writing, software
#                       distributed under the License is distributed on an "AS IS" BASIS,
#                       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#                       See the License for the specific language governing permissions and
#                       limitations under the License.
#
#                                                      __
#                                             _ww   _a+"D
#                                      y#,  _r^ # _*^  y`
#                                     q0 0 a"   W*`    F   ____
#                                  ;  #^ Mw`  __`. .  4-~~^^`
#                                 _  _P   ` /'^           `www=.
#                               , $  +F    `                q
#                               K ]                         ^K`
#                             , §_                . ___ r    ],
#                             _*.^            '.__dP^^~#,  ,_ *,
#                             ^b    / _         ``     _F   ]  ]_
#                              '___  '               ~~^    ]   [
#                              :` ]b_    ~k_               ,`  yl
#                                §P        `*a__       __a~   z~`
#                                §L     _      ^------~^`   ,
#                                 ~-vww*"v_               _/`
#                                         ^"q_         _x"
#                                          __§my..___p/`mma____
#                                      _awP",`,^"-_"^`._ L L  #
#                                    _#0w_^_^,^r___...._ t [],"w
#                                   e^   ]b_x^_~^` __,  .]Wy7` x`
#                                    '=w__^9*§P-*MF`      ^[_.=
#                                        ^"y   qw/"^_____^~9 t
#                                          ]_l  ,'^_`..===  x'
#                                           ">.ak__awwwwWW
#                                             #§WWWWWWWWWWWWWW
#                                            _WWWWWWMM§WWWW_JP^"~-=w_
#                                  .____awwmp_wNw#[w/`     ^#,      ~b___.
#                                   ` ^^^~^"W___            ]Raaaamw~`^``^^~
#                                             ^~"~---~~~~~~`#
# @formatter:on


"""
A detector is a class that gets two images and returns wether they are the same or not.

Each detector has its own method to detect whether or not the images are the same. Some detectors are fast but not
precise, others are slow, but precisely.

Detectors are build to be used in a multiprocessing environment. They are not supposed to read from harddisk,
but from a image pool in RAM. This image pool is a simple list of files. The size of the image pool is determined
by the amount of RAM accessible.
"""


# todo: remove import of unicode_literals for Qt Userinterfaces!
from __future__ import unicode_literals, print_function, division
import sys
import os
import multiprocessing as mp
from PIL import Image
from psutil import virtual_memory

_ = unicode

def get_buffer_size(memory_usage=1., max_amount=1074464256):
    """ returns how much ram is accessible in total on this machine """
    memory = virtual_memory()
    digits = 0

    messages = {
        "total amount": _("Amount of memory:  {memory.total:15d} bytes ~ {total_humanized:3d} GiB"),
        "available amount": _("Available memory:  {memory.available:15d} bytes ~ {available_humanized:3d} GiB"),
        "used amount": _("Buffer size ({used_percent:3d}%):{used:15d} bytes ~ {used_humanized:3d} GiB\n"
                         "You may configure the amount of memory used for the buffer in the settings.") + "\n" + 80 * '-',
    }

    total_memory_humanized = round(memory.total / (1024 ** 3), digits)
    available_memory_humanized = round(memory.available / (1024 ** 3), digits)
    used_memory_humanized = round(min(max_amount, memory.available * memory_usage) / (1024 ** 3), digits)
    percentage_humanized = int(round(min(max_amount, memory.available * memory_usage) / memory.available * 100, 0))

    if not digits:
        total_memory_humanized = int(total_memory_humanized)
        available_memory_humanized = int(available_memory_humanized)
        used_memory_humanized = int(used_memory_humanized)

    print(_("Running on {} cores ...").format(mp.cpu_count()))
    print(messages["total amount"].format(memory=memory, total_humanized=total_memory_humanized))
    print(messages["available amount"].format(memory=memory, available_humanized=available_memory_humanized))
    print(messages["used amount"].format(
        used_percent=percentage_humanized,
        used=int(min(max_amount, memory.available * memory_usage)),
        used_humanized=used_memory_humanized,
    ))
    return int(min(max_amount, memory.available * memory_usage))

def fileinfo(filename, **kwargs):
    start = os.path.dirname(kwargs.get("start", os.path.abspath(filename)))
    if os.path.isfile(filename):
        return kwargs.get('indent', '') + os.path.basename(filename), os.stat(filename)
    elif os.path.isdir(filename):
        for file in os.listdir(os.path.abspath(filename)):
            return fileinfo(
                os.path.join(os.path.abspath(filename), file),
                start=start,
                indent=os.path.relpath(os.path.abspath(filename), start)+os.path.sep
            )

if __name__ == "__main__":
    directory = "E:\\Photos\\Unsortiert"
    directory = "E:\\Id"

    memory = virtual_memory()
    memory_usage = 0.5  # maximal amount of percent used for image buffer, default 0.5
    min_buffer_size = 0 # minimum buffer size should not deceed amount of memory needed to give work to every processor on this machine
    max_buffer_size = memory.available  # maximal buffer size should not exceed total amount of memory available to this process, default: virtual_memory().total

    buffer_size = 0  # size of current buffer
    max_buffer_size = get_buffer_size(memory_usage, max_amount=max_buffer_size)  # amount of memory on this machine

    buffer = dict()  # the actual buffer

    tmp_size = 0

    for file in os.listdir(directory):
        name, stat = fileinfo(os.path.join(directory, file))
        min_buffer_size = max(min_buffer_size, stat.st_size)
    min_buffer_size *= (mp.cpu_count()-1 if mp.cpu_count() > 1 else 1)
    print("max({},{}) * {} = {}".format(min_buffer_size,stat.st_size,(mp.cpu_count()-1 if mp.cpu_count() > 1 else 1), min_buffer_size*(mp.cpu_count()-1 if mp.cpu_count() > 1 else 1)))

    print('Minimum buffer size: {}\nMaximum buffer size: {}\n'.format(min_buffer_size, max_buffer_size))

    for file in os.listdir(directory):
        name, stat = fileinfo(os.path.join(directory, file))
        if stat.st_size >= max_buffer_size:
            print(_('WARNING: "{name}" ({size} bytes) exceeds buffer size and therefore will be ignored.').format(
                name=name, size=stat.st_size
            ))
            continue

        fullname = os.path.join(directory, name)
        # add files to buffer
        sys.stdout.write(_('Add "{name}" ... ').format(name=name))

        if not os.path.isfile(fullname):
            print(_('FAILED, "{}" not found!').format(name))
            continue
        try:
            if buffer_size + stat.st_size < max_buffer_size:
                buffer[name] = Image.open(fullname)
                buffer_size += stat.st_size
                print(" {1}, {0}".format(
                    _('OK'), _("{size} bytes added to buffer").format(size=stat.st_size, total=buffer_size)
                ))
            else:
                print(_("Buffer is full ({amount_of_files}, {amount_of_bytes} bytes)").format(buffer_size))
                break
        except IOError:
            print(" "+_("FAILED!"))

    print(_("{amount_of_files} images added to buffer, {buffer_size} bytes total.").format(
        amount_of_files=len(buffer.keys()), buffer_size=buffer_size
    ))

    for key in buffer:
        buffer[key].close()
    print(_("Freed buffer, memory is available again."))
