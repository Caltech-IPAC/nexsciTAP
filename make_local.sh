#!/bin/sh                                                                      
                                                                               
pip install Cython                                                             
pip install jinja2                                                             
                                                                               
rm -rf dist                                                                    
                                                                               
python setup.py build bdist_wheel
