
import streamlit as st
from PIL import Image
from utils import *
import glob
import os

def setup():
    
    with open("README.md", 'r') as f:
        readme_line = f.readlines()
        readme_buffer = []
        resource_files = [os.path.basename(x) for x in glob.glob(f'src/streamlit/images/*')]
    # resource_files
    for line in readme_line:
        readme_buffer.append(line)
       

    st.markdown(''.join(readme_buffer))


        
    return