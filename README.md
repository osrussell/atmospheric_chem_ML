# Atmospheric Chemistry ML Model Building Helper
## Authors: Olivia Russell, Sarah Covey, and Nic Tekieli 
This repository contains functions to help with data selection for training different ML Models

## Outline

<ul>
  <li>Defines a Python class to give insite about NaNs in a pandas dataframe for air pollutant data applications (```nan_checker.py```)</li>
  <li>Provides a notebook that demonstrates the different capabiltiies of the class (```nan_testing.ipynb```)</li>
</li>

## Setup 

A virtual environment was used to manage any modules added. To use this repo use the following commands:

```
$ python3 -m venv venv # Creates a virtual environment
$ source venv/bin/activate # Activates virtual environment
$ python3 -m pip install --upgrade pip
$ python3 -m pip install -r requirements.txt # Download requirements
```

Shoutout Thomas for the environment set-up !

Also, if you're running any included notebooks on your own machine, make sure to change the following code to the path on your own computer to properly use the submodule!
```
sys.path.append('/Users/olivia/main/research/atmospheric_chem_ML/chem150')
```
