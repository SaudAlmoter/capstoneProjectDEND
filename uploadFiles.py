import os
import glob
import json

import boto3
from botocore.exceptions import ClientError

from time import time      
import datetime as dt

def local_get_all_files(folders):
    """
    Summary line. 
    Scans folder and prepares files list except folders starting with '.'
  
    Parameters: 
    arg1 (Folder names in array)
  
    Returns: 
    Return1 (Array of Selected files)
    Return2 (Array of Ignored files)
    """     
    
    selected_files, ignored_files = [], []    
    
    # 1. checking your current working directory
    print('Current Working Directory : ',os.getcwd())

    for folder in folders:
        # Get your current folder and subfolder event data
        filepath = os.getcwd() + '/' + folder
        print('Scanning Directory : ',filepath)

        # 2. Create a for loop to create a list of files and collect each filepath
        #    join the file path and roots with the subdirectories using glob
        #    get all files matching extension from directory

        for root, dirs, files in os.walk(filepath):
            files = glob.glob(os.path.join(root,'*.*'))
            #print('root = ',root)
            #print('dirs = ',dirs, ' : ',len(dirs))

            # Below condition is to ignore directories like ['.ipynb_checkpoints']
            dotdir = root.split('/')[-1]
            #print('dotdir = ',dotdir[0:1], 'length = ',len(dotdir))
            if( (dotdir[0:1]!='.' and len(dotdir) > 1) or (dotdir[0:1]=='.' and len(dotdir)==1) ):
                #print(files)
                for f in files :
                    selected_files.append(os.path.abspath(f))
            else:
                ignored_files.append(root)

    # 3. get total number of files found
    print('{} files found, {} files ignored'.format(len(selected_files), len(ignored_files) ))
    #print(all_files)
    return selected_files, ignored_files



def s3_upload_files(s3c, bucket_name, selected_files, rFindStr, rStr):
    """
    Summary line. 
    Upload only files to S3 will fail when a directory is encountered in the filepath
  
    Parameters: 
    arg1 (S3 Client)
    arg2 (Bucket name)
    arg3 (Selected files list)
    arg4 (Find string to replace)
    arg5 (String to be replaced with)
  
    Returns: None
    """     

    print('Uploading {} files to S3'.format(len(selected_files)))
    for f in selected_files:
        f = f.replace(rFindStr, rStr)
   
        s3c.upload_file(f, bucket_name, f)
        
def main():
    
    ps_start = time()
    print('{} : Starting S3 Upload process'.format(dt.datetime.now()))

    
    s3c = boto3.client('s3',
                           region_name="us-west-2",
                           aws_access_key_id= 'AKIAQJ4G36KCWBQQCJIV',
                           aws_secret_access_key= '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'
                         )
        
    location = {'LocationConstraint': 'us-east-1'}
    bucket_name = 'source-data2'  
    
    folders = ['../../data/18-83510-I94-Data-2016','inputs','../../data2']
    # calling function to get all file paths in the directory 
    selected_files, ignored_files = local_get_all_files(folders)    
    
    
    print('Selected files = ',len(selected_files))
    for num, fp in enumerate(selected_files, start=1):
        print('{}. {}'.format(num, fp))

    print('Ignored files = ',len(ignored_files))
    for num, fp in enumerate(ignored_files, start=1):
        print('{}. {}'.format(num,fp))
        
    # /home/workspace/
 
    print('{} : Uploading files to bucket {}'.format(dt.datetime.now(), bucket_name))
    rFindStr='/home/workspace/'
    rStr = ''
    #test_arr=['ddl.txt', 'inputs/airline-codes.csv']
    s3_upload_files(s3c, bucket_name, selected_files, rFindStr, rStr)


    
    ps_et = time() - ps_start
    print("=== {} Total Elapsed time is {} sec\n".format('S3 Upload process', round(ps_et,2) ))
    print('{} : Done!'.format(dt.datetime.now()))
    
    
    
if __name__ == "__main__":
    main()
