In order for the files to operate, the device needs to have AWS SDK and IoT device certifications.
The path to the certifications and keys need to be speciifed within the python files.
Light_sense.py handles data collection and needs to be supplied with a --start and --end parameter to determine what data entry to begin with and how long the program will run for. 
Light_control.py handles device response and does not need to be run during initial data collection.