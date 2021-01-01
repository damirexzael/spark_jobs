# Requisites
- Python >=3.6
- Java OpenJDK ==1.8.0

# Install
1. Create environ


    pip -m venv venv

2. Install requirements.txt


    pip install -r requirements.txt 

# Testing
1. Run pytest on tests folder.


    pytest tests

2. Run stop spark session script. 

    
    bash -x stop_spark_session.sh

# Additional commands

- Remove all packages from pip


     pip uninstall -y -r <(pip freeze)
