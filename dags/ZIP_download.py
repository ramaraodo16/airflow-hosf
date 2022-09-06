import wget
import shutil

import datetime
import os
import zipfile
def nse_data_down_site():
    dt = datetime.datetime.now().date()
    
    dnum = dt.day
    mname = dt.strftime("%b").upper()
    mnum = dt.month
    year = dt.year
    ymdnum = year*10000+(mnum*100)+dnum
    ymnum= year*100+mnum
    stdnum = 20220825

    files = []
    #files1 = []
    for k in range(2020,2023):
        #print(k)
        
        #monts= ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
        #montnum = [1,2,3,4,5,6,7,8,9,10,11,12]
        monkey_it = {'JAN' : 1,'FEB': 2,'MAR': 3,'APR': 4,'MAY': 5,'JUN': 6,
                    'JUL': 7,'AUG': 8,'SEP': 9,'OCT': 10,'NOV': 11,'DEC' : 12}
        if (k <= year ):
                pass
        else:
                break
        for key,val in monkey_it.items():
            if (ymnum >= k*100+val):
                pass
            else:
                break
            for i in range(1,31):
                if ((ymdnum >= k*10000+val*100+i) & (stdnum <= k*10000+val*100+i)):
                    
            
                    filename= 'cm'+str(i).zfill(2)+key+str(k)+'bhav'
                    print('\n'+filename)
                    
                    url = 'https://www1.nseindia.com/content/historical/EQUITIES/'+str(k)+'/'+key+'/'+filename+'.csv.zip'
                    print(url)
                    try:
                        print("enter try in loop")
                        print(url)
                        wget.download(url)
                        print(url)
                        files.append(filename+'.csv.zip')
                        #files.append(filename+'.csv')
                    except:
                        print("issue occured")
                else:
                    if(ymdnum >= k*10000+val*100+i):
                        pass
                    else:
                        break
        monkey_it.clear

    print(files)
    
    import zipfile

    for fw in files: 
        print(fw)
        with zipfile.ZipFile(fw, 'r') as zip_ref: 
            zip_ref.extractall('/home/osboxes/Desktop/Files')
        shutil.move(fw,'/home/osboxes/Desktop/Files')



def files_move_to_archives():
    src = '/home/osboxes/Desktop/Files/'
    tgt = '/home/osboxes/Desktop/archives_files/'
    dt_name = datetime.datetime.now().date().strftime('%Y-%m-%d')
    print(dt_name)
    sub_fol = dt_name

    allfiels = os.listdir(src)
    try:
        ntgt = os.path.join(tgt,sub_fol)
        os.mkdir(ntgt)
    except:
        print("directory already available")
    print(ntgt,"target file location")  
    for f1 in allfiels:
        try:
            shutil.move(src+f1, ntgt)
        except Exception as error:
            pass
            print(src+f1,"\n error identified",error)
files_move_to_archives();
nse_data_down_site();