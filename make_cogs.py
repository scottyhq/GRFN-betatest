#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#Create Cloud-optimized geotiffs!
#@author: scott

import glob
import os
import sys

intdir = sys.argv[1]
outdir = sys.argv[2]
if not os.path.isdir(outdir):
	os.mkdir(outdir)
igs = glob.glob(intdir + 'S1*v1.2.1-standard')
total = len(igs)
print(f'\n\nMaking {total} Cloud-optimized geotiffs...\n\n')
for i,ig in enumerate(igs):
    print('\n',i,ig,'\n')
    t1, t2 = ig.split('_')[-2].split('-')
    pair = t1[:8] + '-' + t2[:8]
    src = os.path.join(ig,'merged','filt_topophase.unw.geo.vrt')
	tgt = os.path


    tgt = os.path.join(outdir, pair + '-unw.geo.tif')
    cmd = f'rio cogeo {src} {tgt} -b 2 -p deflate --nodata 0'
    print(cmd)
    os.system(cmd)

print('Output to: ', outdir)
