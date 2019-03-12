#!-*-coding:utf-8-*-
import os
import sys
import time
import numpy as np
import pandas as pd
import gdal
import netCDF4 as nc
import write_output as wo


# ARRAY DIMENSIONS
NX = 720
NY = 360

dtypes_list = [('ny', 'i4'),
               ('nx', 'i4'),
               ('lat', 'f4'),
               ('lon', 'f4'),
               ('forest', 'i2'),
               ('area_m2', 'f4'),
               ('gpp', 'f4'),
               ('ra', 'f4'),
               ('npp', 'f4'),
               ('rc', 'f4'),
               ('et', 'f4'),
               ('cue', 'f4'),
               ('cmass', 'f4'),
               ('cleaf', 'f4'),
               ('cfroot', 'f4'),
               ('cawood', 'f4')]

lat = np.arange(-89.75, 90., 0.5)
lon = np.arange(-179.75, 180., 0.5)

mask = np.load("amazon_mask.npy")
mask_forest = np.load("mask_forests.npy")


def read_as_array(nc_fname, var):
    """ only for multilayers files"""
    with nc.Dataset(nc_fname, mode='r') as fcon:
    #fcon = nc.Dataset(nc_fname)
        data_array = fcon.variables[var][:]
    return np.fliplr(data_array)


def make_table_aux(folder):
    
    root = os.getcwd()
    area_m2 = np.flipud(nc.Dataset("cell_area.nc").variables['cell_area'][:])

    print('\n\nRunning Make_table for folder', end ="-")
    print (folder)
    
    rname = folder.split('_')[0] # to be used in pls_attrs_save
    
    # Variable run stores the string ID of the current run(e.g. "r01")
    
    os.chdir(root + os.sep + folder )
    
    ## open files and read variables
    print("Read variables --- %s\n" % (time.ctime()))
    area_ocp = read_as_array("area.nc", "area") / 100.0
    cmass = read_as_array("cmass.nc", "cmass").sum(axis=0,)
    cleaf = read_as_array("cleaf.nc", "cleaf").sum(axis=0,)
    cfroot = read_as_array("cfroot.nc", "cfroot").sum(axis=0,)
    cawood = read_as_array("cawood.nc", "cawood").sum(axis=0,)
    
    npp =  read_as_array('npp.nc', 'annual_cycle_mean_of_npp').mean(axis=0,)
    photo =  read_as_array('photo.nc', 'annual_cycle_mean_of_ph').mean(axis=0,)
    aresp =  read_as_array('aresp.nc', 'annual_cycle_mean_of_ar').mean(axis=0,)
    cue =  read_as_array('cue.nc', 'annual_cycle_mean_of_cue').mean(axis=0,)
    # wue =  read_as_array('wue.nc', 'annual_cycle_mean_of_wue').mean(axis=0,)
    evapm =  read_as_array('evapm.nc', 'annual_cycle_mean_of_et').mean(axis=0,)
    rcm = read_as_array('rcm.nc', 'annual_cycle_mean_of_rcm').mean(axis=0,)
    print("Ended ncdf readings --- %s\n" % (time.ctime()))
        
    
    struct_array = []
    counter = 0
            
    for Y in range(NY):
        for X in range(NX):
            if not mask[Y, X]:
                NPP = npp[Y, X]
                area1 = area_ocp[:,Y,X]
                if NPP == 0 or NPP is None:
                    pass
                else:
                    counter += 1                             # types
                    line =(Y,                    # i2
                           X,                    # i2
                           lat[Y],               # f4
                           lon[X],               # f4
                           mask_forest[Y, X],    # f4
                           area_m2[Y, X],        # f4
                           '%.6f' %  photo[Y, X],          # daqui pra frente tudo f4
                           '%.6f' %  aresp[Y, X],
                           '%.6f' %  npp[Y, X], 
                           '%.6f' %  rcm[Y, X], 
                           '%.6f' %  evapm[Y, X],
                           '%.6f' %  cue[Y, X],     
                           '%.6f' %  cmass[Y, X],
                           '%.6f' %  cleaf[Y, X],
                           '%.6f' %  cfroot[Y, X],
                           '%.6f' %  cawood[Y, X])                    
                    struct_array.append(line)
    fname_csv = folder + ".csv"
    if os.path.exists(fname_csv):
        pd.DataFrame(np.array(struct_array, dtype=dtypes_list)).to_csv(fname_csv, header=False, index=False, mode='a')
    else:
        pd.DataFrame(np.array(struct_array, dtype=dtypes_list)).to_csv(fname_csv, header=True, index=False, mode='w')
        # clean_variables
    struct_array = None
    area_ocp = None
    cmass = None
    cleaf = None
    cfroot = None
    cawood = None
    os.chdir(root)
    return None


def make_folder_runs(fl):
    
    #with conc.ThreadPoolExecutor(max_workers=5) as executor:
    make_table_aux(fl)
    

def make_table():
    """ Constructs the final table of caete results"""
    index = 1
    # Create the list of lists of output directories
    flds = ["5PFTs_nclim", "5PFTs_application"]
    make_folder_runs(flds[index])
    return None

if __name__ == '__main__':

    # Faz a tabela com dados de cada celula de grid
    make_table()
