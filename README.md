# spark
A conglomerate of projects/applications I've written to practice my spark/hadoop/MapReduce skills.

## Getting Started
My environment for this project is Ubuntu 20.04 through WSL. The instructions below (for downloading and shell commands) assume such. If you're using a different distribution, or a different OS, please keep this in mind!

### Downloads

Firstly, I downloaded spark into my machine. I used [this guide](https://www.virtono.com/community/tutorial-how-to/how-to-install-apache-spark-on-ubuntu-22-04-and-centos/) to download spark.

Additionally, I've opted to use PySpark, and as such running `pip install pyspark` does the trick. You could also opt for Scala, which I happily used in cs451 at the University of Waterloo. But I wanted to test my Python abilities a little bit :P

### Running a program
You may, like me, be tempted to run `python3 PYTHON_FILE.py` to start your code. Tip: don't! Instead, opt for submitting a spark job with `spark-submit PYTHON_FILE.py`
