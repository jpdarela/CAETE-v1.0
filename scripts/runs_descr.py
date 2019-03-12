#!-*-coding:utf-8-*-
import os
import sys
import glob
import time
import numpy as np
import pandas as pd
import gdal
import netCDF4 as nc

import write_output as wo
from dtype_dict import dtypes_list

# Import filepaths from homedir
sys.path.insert(0, '../src/')
import homedir

# UNTAR output files
os.system("ipython3 untar.py")

# This variable must store the correct path to the results folder
data_dir = homedir.RESULTS_DIR

# This variable contains the correct path to save all pls_attrs.csv together and the final csv for analysis
attr_dir = homedir.SAVE_CSV_FILES
csv_dir = homedir.SAVE_CSV_FINAL

# Create outputs folder if it dont exists
if not os.path.exists(attr_dir): os.mkdir(attr_dir)
if not os.path.exists(csv_dir): os.mkdir(csv_dir)
# Some variables
root  = os.getcwd()

# ARRAY DIMENSIONS
NX = 720
NY = 360

lat = np.arange(-89.75, 90., 0.5)
lon = np.arange(-179.75, 180., 0.5)
dtypes = dict(dtypes_list)
# Defining some functions

def _cwm(area, traits):
    """area :: [np.array  1D] shape(npls,)
    traits [np.array 1D ] shape(npls):: g1 || vcmax || tleaf || twood || troot || aleaf || awood || aroot"""

    # assert area.shape == traits.shape, 'shape mismatch'
    return (area * traits).sum(axis=0,)

def _cwv(area, traits):
    """area :: [np.array  1D] shape(npls,)
    traits [np.array 1D ] shape(npls):: g1 || vcmax || tleaf || twood || troot || aleaf || awood || aroot"""
    
    # assert area.shape == traits.shape, 'shape mismatch'
    return pow((area * traits).std(),2)

def read_as_array(nc_fname, var):
    """ only for multilayers files"""
    with nc.Dataset(nc_fname, mode='r') as fcon:
    #fcon = nc.Dataset(nc_fname)
        data_array = fcon.variables[var][:]
    return np.fliplr(data_array)


def folder_list(dr = data_dir):

    """Constructs a list of lists containing the structure 
       of directories that contains CAETÊ outputs
       dr : path to outputs"""

    os.chdir(dr)
    # creating a list with all folders results
    folders = os.listdir(os.getcwd())
    #print folders

    # creating a set with all npls number ex.: {'out12', 'out15'}
    pls_set = set([n.split('_')[0] for n in folders])
    #print pls_set
    # creating a list of lists containing run folder names
    # ex.:[['out12_r1', 'out12_r2'], [out50_r1, out50_r2], [out100, r1, out100_r2]]
    folder_l= [glob.glob1(os.getcwd(), n + '_*') for n in pls_set]
    #print folder_l
    os.chdir(root)
    return folder_l

def make_table_aux(folder):

    traits = ['g1','vcmax','tleaf','twood','troot','aleaf','awood','aroot']

    mask_forest = np.load('mask_forests.npy')
    area_m2 = np.flipud(nc.Dataset("cell_area.nc").variables['cell_area'][:])

    print('\n\nRunning Make_table for folder', end ="-")
    print (folder)
    
    rname = folder.split('_')[0] # to be used in pls_attrs_save

    rname1 = os.sep.join([csv_dir, rname])
    
    # Variable run stores the string ID of the current run(e.g. "r01")
    run = int(folder.split("r")[-1])
    
    os.chdir(data_dir + os.sep + folder )
    
    ## open files and read variables
    print("Read variables %d --- %s\n" % (run,time.ctime()))
    area_ocp = read_as_array("area.nc", "area") / 100.0
    cmass = read_as_array("cmass.nc", "cmass").sum(axis=0,)
    cleaf = read_as_array("cleaf.nc", "cleaf").sum(axis=0,)
    cfroot = read_as_array("cfroot.nc", "cfroot").sum(axis=0,)
    cawood = read_as_array("cawood.nc", "cawood").sum(axis=0,)
    
    npp =  read_as_array('npp.nc', 'annual_cycle_mean_of_npp').mean(axis=0,)
    photo =  read_as_array('photo.nc', 'annual_cycle_mean_of_ph').mean(axis=0,)
    aresp =  read_as_array('aresp.nc', 'annual_cycle_mean_of_ar').mean(axis=0,)
    cue =  read_as_array('cue.nc', 'annual_cycle_mean_of_cue').mean(axis=0,)
    wue =  read_as_array('wue.nc', 'annual_cycle_mean_of_wue').mean(axis=0,)
    evapm =  read_as_array('evapm.nc', 'annual_cycle_mean_of_et').mean(axis=0,)
    rcm = read_as_array('rcm.nc', 'annual_cycle_mean_of_rcm').mean(axis=0,)
    print("Ended ncdf readings %d --- %s\n" % (run,time.ctime()))
    
    attr_table = pd.read_csv('pls_attrs.csv',dtype=np.float32)
    rpath =  attr_dir + os.sep 
    NPLS = rname[3:]
    attr_table.to_csv(rpath + 'pls_attrs' + '-' +str(run) + '-' + NPLS + '.csv', index=False)
    
    
    mask = cleaf.mask
    print("assembling data & printing to file %d -" \
          "with %s PLSs --- %s" % (run, NPLS, time.ctime()))
    
    # DEF MAKE LINE? make_line()
    # Structured Arrays
    
    struct_array = []
    counter = 0
            
    for Y in range(NY):
        for X in range(NX):
            if not mask[Y, X]:
                NPP = npp[Y, X]
                area1 = area_ocp[:,Y,X]
                if NPP <= 1e-5:
                    pass
                else:
                    counter += 1                             # types
                    line =(Y,                    # i2
                           X,                    # i2
                           lat[Y],               # f4
                           lon[X],               # f4
                           mask_forest[Y, X],    # f4
                           area_m2[Y, X],        # f4
                           run,                 # i2
                           '%.6f' %  photo[Y, X],          # daqui pra frente tudo f4
                           '%.6f' %  aresp[Y, X],
                           '%.6f' %  npp[Y, X], 
                           '%.6f' %  rcm[Y, X], 
                           '%.6f' %  evapm[Y, X],
                           '%.6f' %  wue[Y, X],
                           '%.6f' %  cue[Y, X],     
                           '%.6f' %  cmass[Y, X],
                           '%.6f' %  cleaf[Y, X],
                           '%.6f' %  cfroot[Y, X],
                           '%.6f' %  cawood[Y, X],
                        #    '%.6f' %  _cwm(area1, attr_table[traits[0]]),  # g1_cwm 
                        #    '%.6f' %  _cwm(area1, attr_table[traits[1]]),  # vcmax_cwm
                           '%.6f' %  _cwm(area1, attr_table[traits[2]]),  # tleaf_cwm
                           '%.6f' %  _cwm(area1, attr_table[traits[3]]),  # twood_cwm
                           '%.6f' %  _cwm(area1, attr_table[traits[4]]),  # troot_cwm
                           '%.6f' %  _cwm(area1, attr_table[traits[5]]),  # aleaf_cwm
                           '%.6f' %  _cwm(area1, attr_table[traits[6]]),  # awood_cwm
                           '%.6f' %  _cwm(area1, attr_table[traits[7]]))  # aroot_cwm
                        #    '%.6f' %  _cwv(area1, attr_table[traits[0]]),  # g1_cwv
                        #    '%.6f' %  _cwv(area1, attr_table[traits[1]]),  # vcmax_cwv
                        #    '%.6f' %  _cwv(area1, attr_table[traits[2]]),  # tleaf_cwv
                        #    '%.6f' %  _cwv(area1, attr_table[traits[3]]),  # twood_cwv
                        #    '%.6f' %  _cwv(area1, attr_table[traits[4]]),  # troot_cwv
                        #    '%.6f' %  _cwv(area1, attr_table[traits[5]]),  # aleaf_cwv
                        #    '%.6f' %  _cwv(area1, attr_table[traits[6]]),  # awood_cwv
                        #    '%.6f' %  _cwv(area1, attr_table[traits[7]]))  # aroot_cwv
                    
                    sys.stdout.write("\rLines completed: %d" % counter)
                    struct_array.append(line)
    sys.stdout.flush()
    fname_csv = rname1 + ".csv"
    if os.path.exists(fname_csv):
        pd.DataFrame(np.array(struct_array, dtype=dtypes_list)).to_csv(fname_csv, header=False, index=False, mode='a')
    else:
        pd.DataFrame(np.array(struct_array, dtype=dtypes_list)).to_csv(fname_csv, header=True, index=False, mode='w')
        # clean_variables
    struct_array = None
    area_ocp = None
    area_ocp0 = None
    cmass = None
    cleaf = None
    cfroot = None
    cawood = None
    attr_table = None
    os.chdir(root)
    return None


def make_folder_runs(fl):
    
    aux = fl.sort()
    #with conc.ThreadPoolExecutor(max_workers=5) as executor:
    for folder in fl:
        make_table_aux(folder)
    

def make_table():
    """ Constructs the final table of caete results"""
    root = os.getcwd()
    # Create the list of lists of output directories
    flds = folder_list()    
    for fl in flds:
        make_folder_runs(fl)
    return None


def calc_diversity(runs):
    """runs :: list of runs"""
    os.chdir(root)
    os.chdir(data_dir)
    res = os.getcwd()
    os.chdir(runs[0])
    sh = read_as_array("area.nc", "area").shape
    
    os.chdir(res)
    div = np.zeros(shape=sh,dtype=np.float32)
    #lst_div = []
    for run in runs:
        os.chdir(run)
        dt = read_as_array("area.nc", "area")
        div += np.array(dt > 0.0, np.int32)/np.float32(len(runs))
        #lst_div.append(div)
        dt = None
        os.chdir(res)
    #div_arr = np.array(lst_div)
    #del(lst_div)
    div_arr = div.mean(axis=0,)
    del(div)
    os.chdir(root)
    return div_arr


def calc_stats_vars(runs,var):
    """ runs :: list containing run folders
    var  :: string ->>> var name ex.: npp"""

    assert len(runs) > 0,'lista runs vazia'
    # creating a list of arrays containing variour arrays of var as elements
    list_of_arrays = []
    for folder in runs:
        #print os.listdir(folder)
        files = glob.glob1(folder, var + '.nc')
        files = [ folder +'/'+ f for f in files]
        #print files
        for f in files:
            dt = gdal.Open(f).ReadAsArray()
            list_of_arrays.append(dt.copy())
            dt = None

    # running statistics among arrays in list of arrays
    # out = mean + median + std
    #print len(list_of_arrays)

    input_array = np.array(list_of_arrays)
    del(list_of_arrays)
    avg = np.mean(input_array,axis=0,)
    #med = np.median(input_array,axis=0,)
    sig = np.std(input_array,axis=0,)

    os.chdir(root)
    return avg,sig



def calc_stats_pools(runs,var):
    """ runs :: list containing run folders
    var  :: string ->>> var name ex.: cmass"""

    assert len(runs) > 0,'empty -runs list'
    # creating a list of arrays containing variour arrays of var as elements
    list_of_arrays = []
    for folder in runs:
        #print os.listdir(folder)
        files = glob.glob1(folder, var + '.nc')
        files = [ folder +'/'+ f for f in files]
        #print files
        for f in files:
            dt = read_as_array(f, var)
            dt = dt.sum(axis=0,)
            list_of_arrays.append(dt.copy())
            dt = None

    # running statistics among arrays in list of arrays
    # out = mean + median + std
    #print len(list_of_arrays)

    input_array = np.array(list_of_arrays)
    del(list_of_arrays)
    avg = np.mean(input_array,axis=0,)
    #med = np.median(input_array,axis=0,)
    sig = np.std(input_array,axis=0,)

    os.chdir(root)
    return avg,sig


def make_stats(runs):
    vars_tuple = 'cleaf','cawood','cfroot','cmass', 'npp', 'photo', 'evapm', 'rcm', 'aresp', 'wue', 'cue'
    #     print os.getcwd()
    os.chdir(data_dir)
    res= os.getcwd()
    run = runs[0].split('_')[0] + 'PLS_assembled/'
    os.mkdir(run)
    for var in vars_tuple:
        print ("\n" +var) #, '...reading data'
        if var in wo.monthly_out:
            avg,sig = calc_stats_vars(runs,var)
        else:
            avg,sig = calc_stats_pools(runs,var)
        #print os.getcwd()
        os.chdir(data_dir + '/' + run)
        np.save(var + '_avg.npy', avg)
        #np.save(var + '_med.npy', med)
        np.save(var + '_sig.npy', sig)
        os.chdir(res)
    print('reading area --> richness')
    div = calc_diversity(runs)
    os.chdir(data_dir + '/' + run)
    np.save('richness' + '.npy', np.flipud(div))
    os.chdir(root)
    return run # returns folder name for pls results

if __name__ == '__main__':

    # Faz a tabela com dados de cada celula de grid
    make_table()

    # Salva os arquivos de medias de dp (netCDF files)
    folderl = folder_list()
    results = []
    for f in folderl:
       results.append(make_stats(f))

    for f in results:
        os.chdir(data_dir + '/' + f)
        f_to_save = glob.glob1(os.getcwd(), "*.npy")
        npls = f.split('P')[0].split('t')[-1]
        for npy in f_to_save:
            #print(npy)
            var = npy.split('.')[0] 
            arr = np.load(npy)
            ncfilename = var + npls + 'PLS' + '.nc'
            print("\n" + ncfilename)
            wo.write_CAETE_output(ncfilename,arr,var)
        os.chdir(root)
