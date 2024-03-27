Getting bad interpretor error when running a bash file

The error message indicates that there is a problem with the line endings in your shell script build.sh. The ^M characters suggest that the file contains Windows-style line endings (\r\n) instead of Unix-style line endings (\n).

Simply correct it by converting your script to unix style

bash```
sudo apt-get install dos2unix

dos2unix ./docker/spark/build.sh
```
